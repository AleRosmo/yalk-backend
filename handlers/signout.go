package handlers

import (
	"log"
	"net/http"
	"yalk/chat/server"

	"github.com/AleRosmo/cattp"
)

var SignoutHandle = cattp.HandlerFunc[*server.Server](func(w http.ResponseWriter, r *http.Request, context *server.Server) {
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

	err = context.SessionsManager.Delete(cookie.Value) // TODO: Even just a property on the SessionManager is ok
	if err != nil {
		log.Println("Error deleting session", err)
	}
	log.Println("Signed out")
	w.WriteHeader(http.StatusOK)
})
