package main

// server.go

import (
	"fmt"
	"google.golang.org/grpc"
	"net"

	"eventservice/db"
	sv "eventservice/example/serverproto"
	logFactory "eventservice/logFactory"
	sm "eventservice/service/module"
	"time"
)

// Main entrance
func main() {
	// init log
	go func() {
		logFactory.Init()
	}()
	time.Sleep(1 * time.Second)
	serv := &sm.Server{}
	//init  server
	serv.Init()
	serv.CreateTable()
	lis, err := net.Listen("tcp", db.Port)
	if err != nil {
		fmt.Println("failed to listen: %v", err)
	}
	// init connection
	s := grpc.NewServer()
	sv.RegisterGoEventServiceServer(s, serv)
	s.Serve(lis)
}
