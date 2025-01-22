package srv

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"github.com/bakalover/raft/balancer/balancer"
	"github.com/bakalover/raft/machine"
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
		var request machine.RSMcmd
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			log.Printf("Could not parse request: [%s]", err.Error())
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		err, leader := s.b.Balance(request)
		if err != nil {
			log.Printf("%s", err.Error())
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		s.b.SetLeader(leader)
	}
}

func (s *Server) run(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodPost {
		s.b.Run()
		w.WriteHeader(http.StatusOK)
	}
}

func (s *Server) destroy(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodPost {
		s.b.Destroy()
		w.WriteHeader(http.StatusOK)
	}
}

func (s *Server) registerRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/config", s.config)
	mux.HandleFunc("/apply", s.apply)
	mux.HandleFunc("/run", s.apply)
	mux.HandleFunc("/destroy", s.apply)
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
