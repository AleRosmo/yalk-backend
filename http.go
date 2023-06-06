package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"
	"yalk/cattp"
	"yalk/chat"
	"yalk/chat/clients"
	"yalk/logger"

	"gorm.io/gorm"
	"nhooyr.io/websocket"
)

func startHttpServer(conf cattp.Config, chatServer *chat.Server) error {
	router := cattp.New(chatServer)

	router.HandleFunc("/ws", connectHandle)

	router.HandleFunc("/auth", validateHandle)
	router.HandleFunc("/auth/validate", validateHandle)
	router.HandleFunc("/auth/signin", signinHandle)
	router.HandleFunc("/auth/signout", signoutHandle)

	err := router.Listen(&conf)
	if err != nil {
		return err
	}

	log.Println("HTTP Server succesfully started") // TODO: Move back in main func
	return nil
}

// TODO: Enum log events and colors 'info' 'warning 'error'
var connectHandle = cattp.HandlerFunc[*chat.Server](func(w http.ResponseWriter, r *http.Request, server *chat.Server) {
	logger.Info("WEBSOCK", fmt.Sprintf("Requested WebSocket - %s", r.RemoteAddr))

	// TODO: Custom config for parameters on admin site

	session, err := server.SessionsManager.Validate(server.Db, r, "YLK") // TODO: Separate in other config
	if err != nil {
		logger.Warn("HTTP", "Can't validate session")
		http.Redirect(w, r, "/login", http.StatusSeeOther)
		return
	}

	session.SetClientCookie(w) // TODO: Reimplement for JWT and WebSession
	if err != nil {
		log.Println("Error marshaling JWT Token")
		return
	}

	conn, err := upgradeHttpRequest(w, r)
	if err != nil {
		logger.Err("WEBSOCK", fmt.Sprintf("Can't start accepting - %s", r.RemoteAddr))
		w.WriteHeader(http.StatusInternalServerError)
		r.Body.Close()
		return
	}

	defer conn.Close(websocket.StatusNormalClosure, "Client disconnected")
	account := &chat.Account{}
	account.ID = session.AccountID

	if err = account.GetInfo(server.Db); err != nil {
		logger.Err("WEBSOCK", fmt.Sprintf("Can't get account info - %s", r.RemoteAddr))
		w.WriteHeader(http.StatusInternalServerError)
		r.Body.Close()
		return
	}

	var user *chat.User
	tx := server.Db.Preload("Account").Preload("Chats").Preload("Chats.ChatType").Find(&user, "account_id =?", account.ID)
	if tx.Error != nil {
		logger.Err("WEBSOCK", fmt.Sprintf("Can't get user info - %s", r.RemoteAddr))
		w.WriteHeader(http.StatusInternalServerError)
		r.Body.Close()
		return
	}

	// // user := &chat.User{ID: account.ID}
	// if _, err := user.GetInfo(server.Db); err != nil {
	// 	logger.Err("WEBSOCK", fmt.Sprintf("Can't get user info - %s", r.RemoteAddr))
	// }

	// Todo: Use profile instead of User ID?
	client := server.RegisterClient(conn, user.ID)

	notify := make(chan bool)

	var wg sync.WaitGroup

	// Ping ticker which must be switched to the literally already fully made
	// methods on the connection.
	var ticker = time.NewTicker(time.Second * time.Duration(100000))

	channelsContext := &chat.EventContext{
		NotifyChannel: notify,
		PingTicket:    ticker,
		WaitGroup:     &wg,
		Request:       r,
		Connection:    conn,
	}

	wg.Add(1)
	go server.Receiver(client.ID, channelsContext)

	wg.Add(1)
	go server.Sender(client, channelsContext)

	initalPayload, err := makeInitialPayload(server.Db, user)
	if err != nil {
		logger.Err("CORE", "Error marshalling payload")
		return
	}

	if clients.ClientWriteWithTimeout(r.Context(), time.Second*5, conn, initalPayload); err != nil {
		logger.Info("CLIENT", "Timeout Initial Payload")
		return
	}

	logger.Info("CLIENT", fmt.Sprintf("Full data sent to ID: %v", "test"))

	wg.Wait()

	go func() {
		channelsContext.NotifyChannel <- true
		server.ClientsMu.Lock()
		delete(server.Clients, client.ID)
		server.ClientsMu.Unlock()
		// onlineTick := time.NewTicker(time.Second * 10)
		// <-onlineTick.C
	}()
	logger.Info("CORE", fmt.Sprintf("Closed client ID %s", "test"))
})

func makeInitialPayload(db *gorm.DB, user *chat.User) ([]byte, error) {

	var chats *[]chat.Chat
	tx := db.Joins("left join chat_users on chat_users.chat_id=chats.id").
		Where("chat_users.user_id = ?", user.ID).
		Preload("Messages", func(db *gorm.DB) *gorm.DB {
			return db.Order("messages.timestamp ASC")
		}).
		Preload("Messages.User").
		Preload("ChatType").
		Find(&chats)
	if tx.Error != nil {
		return nil, tx.Error
	}

	var serverAccounts *[]chat.Account
	if user.IsAdmin {
		tx = db.Find(&serverAccounts)
		if tx.Error != nil {
			return nil, tx.Error
		}
	}

	var users *[]chat.User
	tx = db.Find(&users)
	if tx.Error != nil {
		return nil, tx.Error
	}

	initialPayload := struct {
		User     *chat.User      `json:"user"`
		Chats    *[]chat.Chat    `json:"chats"`
		Accounts *[]chat.Account `json:"accounts,omitempty"`
		Users    *[]chat.User    `json:"users"`
	}{user, chats, serverAccounts, users}

	jsonPayload, err := json.Marshal(&initialPayload)
	if err != nil {
		return nil, err
	}

	newRawEvent := &chat.RawEvent{Type: "initial", Data: jsonPayload}

	jsonEvent, err := newRawEvent.Serialize()
	fmt.Println(string(jsonEvent))
	if err != nil {
		return nil, err
	}

	return jsonEvent, nil
}

var signupHandle = cattp.HandlerFunc[*chat.Server](func(w http.ResponseWriter, r *http.Request, context *chat.Server) {
	defer r.Body.Close()

	cookie, err := r.Cookie("YLK")
	if err != nil {
		return
	}

	http.SetCookie(w, &http.Cookie{
		Name:   "YLK",
		Value:  "",
		Path:   "/",
		MaxAge: -1,
	})

	err = context.SessionsManager.Delete(context.Db, cookie.Value) // TODO: Even just a property on the SessionManager is ok
	if err != nil {
		log.Println("Error deleting session", err)
	}
	log.Println("Signed out")
	w.WriteHeader(http.StatusOK)
})
