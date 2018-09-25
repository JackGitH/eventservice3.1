package main

//client.go

import (
	"log"
	"os"

	"fmt"
	"golang.org/x/net/context"

	"google.golang.org/grpc"
	"io"
	pb "protocdemo/example/helloworld"
)

const (
	address     = "localhost:8851"
	defaultName = "world111"
)

func main() {
	TestChangeSayHello()
}
func TestSayHello() {
	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		log.Fatal("did not connect: %v", err)
	}
	defer conn.Close()
	c := pb.NewGreeterClient(conn)

	name := defaultName
	if len(os.Args) > 1 {
		name = os.Args[1]
	}
	r, err := c.SayHello(context.Background(), &pb.HelloRequest{Name: name})
	if err != nil {
		log.Fatal("could not greet: %v", err)
	}
	log.Printf("Greeting: %s", r.Message)
}
func TestChangeSayHello() {
	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		log.Fatal("did not connect: %v", err)
	}
	defer conn.Close()
	c := pb.NewGreeter2Client(conn)

	stream, err := c.ChangeSayHello(context.Background())
	for {

		testsendMsg := "aaaaaa"
		if err := stream.Send(&pb.HelloRequest{Name: testsendMsg}); err != nil {
			fmt.Println("Failed to send a note: %v", err)
		}

		in, err := stream.Recv()
		if err == io.EOF {
			// read done.
			fmt.Println("read done ")
			return
		}
		if err != nil {
			fmt.Println("Failed to receive a note : %v", err)
		}
		fmt.Println("Got message:", in.GetMessage())
	}

}
