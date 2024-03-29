package server

import (
	"log"
	"yalk/app"
	"yalk/config"
	"yalk/handlers"

	"github.com/AleRosmo/cattp"
)

func StartHttpServer(config *config.Config, context app.HandlerContext) error {
	router := cattp.New(context)

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
