package handlers

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

	"nhooyr.io/websocket"
)

var ConnectHandle = cattp.HandlerFunc[*chat.Server](func(w http.ResponseWriter, r *http.Request, server *chat.Server) {
	logger.Info("WSCK", fmt.Sprintf("Requested WebSocket - %s", r.RemoteAddr))

	// TODO: Custom config for parameters on admin site

	session, err := server.SessionsManager.Validate(server.Db, r, "YLK") // TODO: Separate in other config
	if err != nil {
		// logger.Warn("HTTP", "Can't validate session")
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
		logger.Err("WSCK", fmt.Sprintf("Can't start accepting - %s", r.RemoteAddr))
		w.WriteHeader(http.StatusInternalServerError)
		r.Body.Close()
		return
	}

	defer conn.Close(websocket.StatusNormalClosure, "Client disconnected")
	account := &chat.Account{}
	account.ID = session.AccountID

	if err = account.GetInfo(server.Db); err != nil {
		logger.Err("WSCK", fmt.Sprintf("Can't get account info - %s", r.RemoteAddr))
		w.WriteHeader(http.StatusInternalServerError)
		r.Body.Close()
		return
	}

	var user *chat.User
	tx := server.Db.Preload("Account").Preload("Chats").Preload("Chats.ChatType").Find(&user, "account_id =?", account.ID)
	if tx.Error != nil {
		logger.Err("WSCK", fmt.Sprintf("Can't get user info - %s", r.RemoteAddr))
		w.WriteHeader(http.StatusInternalServerError)
		r.Body.Close()
		return
	}
	// Todo: Use profile instead of User ID?
	client := server.RegisterClient(conn, user.ID)

	logger.Info("CLNT", fmt.Sprintf("Full data sent to ID: %d", client.ID))

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

	var initialPayloadSent bool // Add a flag to tracak wheether the initial payload has been sent

	// Send initial payload to new client
	initalPayload, err := makeInitialPayload(server.Db, user)
	if err != nil {
		logger.Err("CORE", "Error marshalling payload")
		return
	}

	if clients.ClientWriteWithTimeout(client.ID, r.Context(), time.Second*5, conn, initalPayload); err != nil {
		logger.Info("CLNT", "Timeout Initial Payload")
		return
	}
	initialPayloadSent = true

	user.ChangeStatus(server.Db, "online")
	user.Status = &chat.Status{Name: "online"}

	// Broadcast user online event to all connected clients, but wait for initial payload to be sent first
	if initialPayloadSent {
		// <-notify
		// TODO: Same as below for offline, so need to be removed the repetition
		userOnlinePayload, err := user.Serialize()
		if err != nil {
			logger.Err("HTTP", fmt.Sprintf("Error serializing user online: %v", err))
		}
		var rawEvent = &chat.RawEvent{Type: "user", Action: "change_status", UserID: client.ID, Data: userOnlinePayload}
		jsonRawEvent, err := json.Marshal(rawEvent)
		if err != nil {
			logger.Err("HTTP", fmt.Sprintf("Error serializing raw event: %v", err))
		}
		server.SendAll(jsonRawEvent)
	}

	// if err := server.HandleIncomingEvent(client.ID, rawEvent); err != nil {
	// 	logger.Info("HTTP", "Error broadcasting disconnection event")
	// }

	wg.Wait()

	go func() {
		server.UnregisterClient(client)
		onlineTick := time.NewTicker(time.Second * 10)
		<-onlineTick.C
		if server.Clients[client.ID] == nil {
			if err := user.GetInfo(server.Db); err != nil {
				logger.Err("CLNT", "Error getting info upon closure")
			}

			if err := user.ChangeStatus(server.Db, "offline"); err != nil {
				logger.Err("CLNT", "Error changing status upon closure")
			}

			var userStatus = &chat.User{StatusName: "offline"}

			userStatusPayload, err := userStatus.Serialize()
			if err != nil {
				logger.Err("HTTP", fmt.Sprintf("Error serializing user status: %v", err))
			}

			var rawEvent = &chat.RawEvent{Type: "user", Action: "change_status", UserID: client.ID, Data: userStatusPayload}

			logger.Info("HTTP", fmt.Sprintf("%d disconnected after 10s", client.ID))
			if err := server.HandleIncomingEvent(client.ID, rawEvent); err != nil {
				logger.Info("HTTP", "Error broadcasting disconnection event")

			}

		}
	}()

	fmt.Println("Terminated WG")

	// }()
})
