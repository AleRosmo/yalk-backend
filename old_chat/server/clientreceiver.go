package server

import (
	"io"
	"log"
	"yalk/chat/models"

	"nhooyr.io/websocket"
)

func (server *Server) Receiver(clientID uint, ctx *models.EventContext) {
	defer func() {
		ctx.WaitGroup.Done()
		ctx.NotifyChannel <- true
	}()

	// Run:
	for {
		messageType, jsonEventMessage, err := ctx.Connection.Read(ctx.Request.Context())

		log.Printf("Received payload lenght: %d", len(jsonEventMessage))

		if err != nil && err != io.EOF {

			statusCode := websocket.CloseStatus(err)

			if statusCode == websocket.StatusGoingAway {
				log.Println("Graceful sender shutdown")
				ctx.PingTicket.Stop()
				return
				// break Run

			} else {
				log.Println("Sender - Error in reading from websocket context, client closed? Check main.go")
				// break Run
				return
			}
		}

		if messageType.String() == "MessageText" && err == nil {
			log.Printf("Message received: %s", jsonEventMessage)

			rawEvent := &models.RawEvent{UserID: clientID}

			if err := rawEvent.Deserialize(jsonEventMessage); err != nil {
				log.Printf("Error unmarshaling RawEvent")
				return
			}
			if err := server.HandleIncomingEvent(clientID, rawEvent); err != nil {
				log.Printf("Sender - errors in broadcast: %v", err)
				return
			}
		}
	}
}
