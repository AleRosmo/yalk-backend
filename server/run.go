package server

import (
	"fmt"
	"sync"
	"time"
	"yalk/config"
	"yalk/database"
	newserver "yalk/newchat/server"
	"yalk/sessions"

	"gorm.io/gorm"
)

func RunServer(config *config.Config, conn *gorm.DB) {
	var wg sync.WaitGroup

	sessionDatabase := sessions.NewDatabase(conn)

	sessionLenght := time.Hour * 720
	sessionsManager := sessions.NewSessionManager(sessionDatabase, sessionLenght)

	// chatServer := server.NewServer(16, conn, sessionsManager)
	db := database.NewDatabase(conn)
	newChatServer := newserver.NewServer(db, sessionsManager)
	fmt.Print(newChatServer) // TODO: remove
	// TODO: enable again when modifying StartHttpServer()
	// wg.Add(1)
	// go func() {
	// 	defer wg.Done()
	// 	StartHttpServer(config, newChatServer)
	// }()

	fmt.Println("server started")
	wg.Wait()
}
