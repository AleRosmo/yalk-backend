// TODO: Consider the methods to send messages and notificatins: How should they be and what is the default structure to keep?
// TODO: We can have a structure like this with specific types on args and these names (SendToChat, SendNotification)..
// ..or more generic like SendToClient, SendToMany, Broadcast, and so on or even just use SendClient accepting an array of clients along..
// ..with the others above.
// TODO: Maybe not exported methods (which I guess are helper functions?) might go in one single utils.go file?

package server

import (
	"fmt"
	"sync"
	"yalk/chat/client"
	"yalk/chat/database"
	"yalk/chat/event"
	"yalk/chat/event/handlers"
	"yalk/chat/models/events"
	"yalk/chat/serialization"
	"yalk/sessions"
)

type serverImpl struct {
	clients    map[uint]client.Client
	mu         sync.Mutex
	handlers   map[string]event.Handler
	db         database.DatabaseOperations
	sm         sessions.SessionManager
	serializer serialization.SerializationStrategy
}

func NewServer(db database.DatabaseOperations, sm sessions.SessionManager) Server {
	s := &serverImpl{
		clients:  make(map[uint]client.Client),
		handlers: make(map[string]event.Handler),
		db:       db,
		sm:       sm,
	}

	s.RegisterEventHandler("NewMessage", handlers.NewMessageHandler{})
	s.RegisterEventHandler("NewUser", handlers.NewUserHandler{})
	return s
}

func (s *serverImpl) GetClientByID(id uint) (client.Client, error) {
	client, ok := s.clients[id]
	if !ok {
		return nil, fmt.Errorf("client %d not registered", id)
	}
	return client, nil
}

// TODO: Revisit for specialized event handling, and make type with UserID metadata
// TODO: Maybe this could be an interface for all the base event types and have a HandleEvent method?
// ..Is it useful being only the initial payload? Even for the sake of decoupling, if it makes sense I'll doit.
func (s *serverImpl) HandleEvent(eventWithMetadata *events.BaseEventWithMetadata) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	baseEvent := eventWithMetadata.Event
	eventType := baseEvent.Type

	// Look up the event handler for the given event type
	handler, err := s.getHandler(eventType)
	if err != nil {
		return err
	}
	ctx := &event.HandlerContext{DB: s.db, SendToChat: s.SendChat}

	// Pass the event to the appropriate handler
	return handler.HandleEvent(ctx, baseEvent)
}

func (s *serverImpl) getHandler(eventType string) (event.Handler, error) {
	handler, exists := s.handlers[eventType]
	if !exists {
		return nil, fmt.Errorf("no handler registered for event type %s", eventType)
	}
	return handler, nil
}

// TODO: Implement error checking if args are empty
// TODO: This can become a util for handlers?
// func newBaseEvent(opcode string, data json.RawMessage, clientID uint, eventType string) (*events.BaseEvent, error) {
// 	baseEvent := &events.BaseEvent{
// 		Opcode:   opcode,
// 		Data:     data,
// 		ClientID: clientID,
// 		Type:     eventType,
// 	}
// 	// There must be a better way to do this error check
// 	if opcode == "" {
// 		return nil, fmt.Errorf("opcode empty")
// 	}
// 	return baseEvent, nil
// }
