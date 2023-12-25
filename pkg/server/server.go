package server

import (
	"context"
	"fmt"
	"github.com/gorilla/mux"
	"github.com/ssfilatov/ts/pkg/storage"
	"log"
	"net/http"
	_ "net/http/pprof"
)

const defaultPort = "8279"

type Server struct {
	httpServer *http.Server
}

func NewServer(storage *storage.Storage) *Server{
	router := mux.NewRouter()
	router.PathPrefix("/debug/pprof/").Handler(http.DefaultServeMux)
	router.Handle("/", newHandler(storage))
	return &Server{
		httpServer: &http.Server{
			Addr: fmt.Sprintf(":%s", defaultPort),
			Handler: router,
		},
	}
}

type Handler interface {
	ServeHTTP(http.ResponseWriter, *http.Request) error
}

func (s *Server) Run() error {
	log.Printf("starting http server on addr %s", s.httpServer.Addr)
	return s.httpServer.ListenAndServe()
}

func (s *Server) Shutdown(ctx context.Context) error {
	return s.httpServer.Shutdown(ctx)
}
