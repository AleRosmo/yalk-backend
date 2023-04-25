package main

// * Handle incoming user payload and process it eventually forwarding in the correct routine channels for other users to receive.
// func (server *server) handlePayload(msg []byte, origin string) (err error) {
// 	var _req any
// 	var _payload map[string]any
// 	var data payload
// 	var event string

// 	err = json.Unmarshal(msg, &_req)
// 	if err != nil {
// 		log.Println("Listener - Error deserializing JSON")
// 		return err
// 	}

// 	// Asserting request type
// 	switch p := _req.(type) {
// 	case map[string]any:
// 		_payload = p
// 	default:
// 		log.Println("Listener - can't decode payload")
// 	}

// 	switch v := _payload["event"].(type) {
// 	case string:
// 		event = v
// 	default:
// 		log.Println("Listener - can't decode event")
// 	}

// 	data = payload{
// 		Success: true,
// 		Origin:  origin,
// 		Event:   event,
// 	}

// 	/*
// 	* Event 'data_image' contains a base64 encoded image
// 	* Max 2MB, Accepts: jpg, jpeg, png, gif
// 	* Picture is decoded and saved in users directory`
// 	 */

// 	// * Routing event to server
// 	switch event {
// 	case "pong":
// 		switch value := _payload["message"].(type) {
// 		case float64:
// 			_t1 := int64(value)
// 			t1 := time.UnixMilli(_t1)
// 			// t2 := time.Now().UnixMilli()
// 			// n, err := Atoi(value)
// 			if err != nil {
// 				log.Println("Ping - Err converting to int")
// 				return err
// 			}
// 			log.Printf("User %v ping %v\n", origin, time.Since(t1))

// 		default:
// 			log.Println("Pong - can't decode timestamp")
// 			return err
// 		}
// 	case "data_image":
// 		value, ok := _payload["data"].(string)
// 		if !ok {
// 			log.Println("Listener - invalid picture type")
// 			return err
// 		}
// 		stringEnc, err := base64.StdEncoding.DecodeString(value)
// 		if err != nil {
// 			log.Println("Listener - cannot take out mia nonna dal forno")
// 			return err
// 		}
// 		fmt.Printf("Avatar payload lenght: %v", len(stringEnc))
// 		f, err := os.OpenFile(fmt.Sprintf("static/data/user_avatars/%s/avatar.png", origin), os.O_WRONLY|os.O_CREATE, 0666)
// 		if err != nil {
// 			log.Println("Listener - can't open directory")
// 			return err
// 		}
// 		data.Event = "user_update"
// 		data.Data, err = userRead(server.dbconn, origin, false)
// 		if err != nil {
// 			log.Println("Listener - can't read userInfo")
// 			return err
// 		}
// 		defer f.Close()
// 		n, err := f.Write(stringEnc)
// 		if err != nil {
// 			log.Println("Listener - can't write file")
// 			return err
// 		}
// 		fmt.Printf("Succesfully wrote %v bytes", n)

// 		server.channels.Notify <- data
// 		return nil

// 	case "chat_message":
// 		var to, text string
// 		switch v := _payload["to"].(type) {
// 		case string:
// 			to = v
// 		default:
// 			log.Println("Listener - can't decode chat_id")
// 			return err
// 		}

// 		switch v := _payload["text"].(type) {
// 		case string:
// 			text = v
// 		default:
// 			log.Println("Listener - can't decode text")
// 			return err
// 		}

// 		if text == "" {
// 			return fmt.Errorf("empty")
// 		}

// 		data.Data = message{
// 			ID:   messageCreate(server.dbconn, time.Now().UTC(), text, origin, to),
// 			Time: time.Now().UTC(),
// 			From: origin,
// 			To:   to,
// 			Text: text,
// 			Type: "chat_message",
// 		}

// 		var info chat
// 		info, err = chatInfo(server.dbconn, to, true)
// 		if err != nil {
// 			log.Println("Listener - chat not found")
// 			return err
// 		}
// 		if info.Type == "channel_public" || info.Type == "channel_private" {
// 			server.channels.Msg <- data
// 			return nil
// 		}
// 		if info.Type == "dm" {
// 			data := map[string]any{"users": info.Users, "payload": data}
// 			server.channels.Dm <- data
// 			return nil
// 		}

// 	case "user_disconn":
// 		data.Data, err = userRead(server.dbconn, origin, true)
// 		if err != nil {
// 			log.Println("SendClient - can't read userInfo")
// 			return err
// 		}
// 		server.channels.Notify <- data

// 	case "user_update":
// 		var status string
// 		var fixed bool
// 		switch v := _payload["status"].(type) {
// 		case string:
// 			status = v
// 		default:
// 			log.Println("SendClient - can't decode status")
// 			return err
// 		}

// 		err = userStatusUpdate(server.dbconn, origin, status, fixed)
// 		if err != nil {
// 			log.Println("SendClient - can't decode statusFixed")
// 			return err
// 		}

// 		data.Data, err = userRead(server.dbconn, origin, true)
// 		if err != nil {
// 			log.Println("SendClient - can't read userInfo")
// 			return err
// 		}

// 		server.channels.Notify <- data
// 	case "chat_create":
// 		var chatType string
// 		var name string
// 		var users []string

// 		switch value := _payload["name"].(type) {
// 		case string:
// 			name = value
// 		default:
// 			name = ""
// 		}

// 		switch value := _payload["type"].(type) {
// 		case string:
// 			chatType = value
// 		default:
// 			if value == "channel_public" && name == "" {
// 				return err
// 			}
// 		}
// 		switch value := _payload["users"].(type) {
// 		case string:
// 			users = append(users, value)
// 		// case []interface{}:
// 		case []string:
// 			users = value
// 		}

// 		newID := chatCreate(server.dbconn, origin, chatType, name, users)

// 		users = append(users, origin)
// 		newChat := chat{
// 			ID:    newID,
// 			Type:  chatType,
// 			Name:  name,
// 			Users: users,
// 		}

// 		payload := dataPayload{
// 			Success: true,
// 			Origin:  origin,
// 			Event:   event,
// 			Data:    newChat,
// 		}
// 		server.channels.Notify <- payload

// 	case "chat_delete":
// 		var id string
// 		switch value := _payload["id"].(type) {
// 		case string:
// 			id = value
// 			// default:
// 			// 	chat_id = ""
// 		}
// 		err = chatDelete(server.dbconn, id)
// 		if err != nil {
// 			log.Println("Listener - Error deleting chat")
// 			return err
// 		}
// 		payload := dataPayload{
// 			Success: true,
// 			Origin:  origin,
// 			Event:   event,
// 			Data:    id,
// 		}
// 		server.channels.Notify <- payload
// 	case "chat_join":
// 		var id string
// 		switch value := _payload["id"].(type) {
// 		case string:
// 			id = value
// 		}
// 		chat, err := chatJoin(server.dbconn, origin, id)
// 		if err != nil {
// 			logger.LogColor("WEBSOCK", "Error joining chat")
// 			return err
// 		}
// 		payload := dataPayload{
// 			Success: true,
// 			Origin:  origin,
// 			Event:   event,
// 			Data:    chat,
// 		}
// 		server.channels.Notify <- payload
// 	case "user_conn":
// 		log.Printf("OK - Greetings received from ID %v - payload received: %v", origin, string(msg))
// 		server.channels.Notify <- data
// 	default:
// 		log.Println("SendClient - Invalid request")
// 		data.Success = false
// 		return fmt.Errorf("invalid_request")
// 	}
// 	return nil
// }
