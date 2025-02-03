package main

import (
	"context"
	"go-redis/client"
	"log"
	"log/slog"
	"net"
	"time"
)

const defaultListenAddr =":8976"

type Config struct{
	ListenAddress string
}
type Server struct{
	Config
	peers map[*Peer]bool
	ln net.Listener
	addPeerCh chan *Peer
	quitCh chan struct{}
	msgCh chan []byte
}


func NewServer(cfg Config) *Server{
	if len(cfg.ListenAddress) == 0{
		cfg.ListenAddress = defaultListenAddr
	}
	return &Server{
		Config: cfg,
		peers: make(map[*Peer]bool),
		addPeerCh: make(chan *Peer),
		quitCh: make(chan struct{}),
		msgCh: make(chan []byte),
	}
}

func (s *Server) Start() error{
	ln ,err := net.Listen("tcp", s.ListenAddress)
	if err != nil {
		return err
	}
	s.ln = ln
	go s.loop()

	slog.Info("server running", "listenAddr", s.ListenAddress)
	return s.acceptLoop() 
	
}

func (s *Server) handleRawMessage(rawMsg []byte) error{
	cmd, err := parseCommand(string(rawMsg))
	if err != nil{
		return err
	}
	switch v := cmd.(type){
	case SetCommand:
		slog.Info("somebody want to set a key in to the hash table", "key", v.key, "value", v.val)
	}
	return nil 
}

func (s *Server) loop() {
	for {
		select {
		case rawMsg:= <-s.msgCh:
			if err :=s.handleRawMessage(rawMsg); err != nil{
				slog.Error("Handle Raw Message Error", "err", err)
			}
		case <- s.quitCh:
			return
		case peer := <-s.addPeerCh:
			s.peers[peer] = true
	
		}
	}
}
func (s *Server) acceptLoop() error{
	for {
		conn, err := s.ln.Accept()
		if err!=nil{
			slog.Error("accept error", "error", err)
			continue
		}
		go s.handleConn(conn)
	}
}

func (s *Server) handleConn(conn net.Conn){
	peer := NewPeer(conn, s.msgCh)
	s.addPeerCh <- peer
	slog.Info("new peer connected", "remoteAddr", conn.RemoteAddr())
	if err:= peer.readLoop();err !=nil {
		slog.Error("peer read error", "err", err, "remoteAddr", conn.RemoteAddr())
	}
}

func main() {
	go func() {
		server := NewServer(Config{})
		log.Fatal(server.Start())
	}()

	time.Sleep(time.Second)
	client := client.New("localhost:8976")
	
	if err := client.Set(context.TODO(),"foo", "bar" ); err != nil{
		log.Fatal(err)
	}
}