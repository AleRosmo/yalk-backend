package server

import (
	"log"
	"yalk/config"
	"yalk/handlers"
	"yalk/newchat/server"

	"github.com/AleRosmo/cattp"
)

func StartHttpServer(config *config.Config, chatServer server.Server) error {
	router := cattp.New(chatServer)

	router.HandleFunc("/ws", handlers.ConnectionHandler)

	// TODO: Temporarily disabled
	// router.HandleFunc("/auth", handlers.ValidateHandle)
	// router.HandleFunc("/auth/validate", handlers.ValidateHandle)
	// router.HandleFunc("/auth/signin", handlers.SigninHandle)
	// router.HandleFunc("/auth/signout", handlers.SignoutHandle)

	// router.HandleFunc("/auth/signup", signupHandle)

	netConf := cattp.Config{
		Host: config.HttpHost,
		Port: config.HttpPort,
		URL:  config.HttpUrl}

	err := router.Listen(&netConf)
	if err != nil {
		return err
	}

	log.Println("HTTP Server succesfully started") // TODO: Move back in main func
	return nil
}
