package worker

import (
	"log"
	"net/http"

	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

type Worker struct {
	id string 
}

func NewWorker(id string) *Worker {
	return &Worker{id: id}
}

func (w *Worker) HandleConnection(wr http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(wr, r, nil)
	if err != nil {
		log.Printf("[worker-%s] upgrade failed: %v", w.id, err)
		return
	}
	defer conn.Close()

	log.Printf("[worker-%s] new connection from gateway", w.id)

	for {
		messageType, message, err := conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err,
				websocket.CloseGoingAway,
				websocket.CloseNormalClosure,
			) {
				log.Printf("[worker-%s] unexpected error: %v", w.id, err)
			}
			log.Printf("[worker-%s] connection closed", w.id)
			return
		}

		log.Printf("[worker-%s] received: %s", w.id, string(message))

		// echo back
		err = conn.WriteMessage(messageType, message)
		if err != nil {
			log.Printf("[worker-%s] write error: %v", w.id, err)
			return
		}
	}
}

func (w *Worker) Start(addr string) error {
	mux := http.NewServeMux()
	mux.HandleFunc("/ws", w.HandleConnection)
	mux.HandleFunc("/health", func(wr http.ResponseWriter, r *http.Request) {
		wr.WriteHeader(http.StatusOK)
		wr.Write([]byte("ok"))
	})

	log.Printf("[worker-%s] starting on %s", w.id, addr)
	return http.ListenAndServe(addr, mux)
}