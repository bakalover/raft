package srv

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"github.com/bakalover/raft/balancer/balancer"
)

type Server struct {
	b balancer.IBalancer
}

func (s *Server) config(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodPost {
		var config Config
		if err := json.NewDecoder(r.Body).Decode(&config); err != nil {
			log.Printf("Could not parse config: [%s]", err.Error())
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		s.b.SetStrategy(balancer.Strategy(config.Strategy))
		if config.Peers != nil {
			s.b.SetAddrs(config.Peers)
		}
	}
}

func (s *Server) apply(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodPut {

	}
}

func (s *Server) registerRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/config", s.config)
	mux.HandleFunc("/apply", s.apply)
}

func (s *Server) Start(addr string) error {
	mux := http.NewServeMux()
	s.registerRoutes(mux)
	fmt.Printf("Server is running on http://%s\n", addr)
	return http.ListenAndServe(addr, mux)
}

func NewServer() *Server {
	return &Server{b: balancer.NewBalancer()}
}
