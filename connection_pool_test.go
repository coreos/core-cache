package main

import (
	"fmt"
	"net"
	"os"
	"testing"
	"time"
)

const (
	RECV_BUF_LEN = 1024
)

var groupSize = 150

func TestPool(t *testing.T) {
	go server()
	time.Sleep(time.Second)
	cp, err := createConnectionPool("0.0.0.0:6666")

	if err != nil {
		panic(err)
	}

	c := make(chan bool, groupSize)

	for i := 0; i < groupSize; i++ {
		go client(cp, fmt.Sprintf("client %d", i), c)
	}

	for i := 0; i < groupSize; i++ {
		<-c
	}
}

func TestWithoutPool(t *testing.T) {
	go stupidServer()
	time.Sleep(time.Second)
	c := make(chan bool, groupSize)
	for i := 0; i < groupSize; i++ {
		go stupidClient(fmt.Sprintf("client %d", i), c)
	}

	for i := 0; i < groupSize; i++ {
		<-c
	}
}

func stupidClient(clientName string, c chan bool) {
	conn, err := net.Dial("tcp", "0.0.0.0:7777")

	if err != nil {
		panic(err)
	}
	for i := 0; i < 10; i++ {
		//fmt.Println(clientName, i)
		buf := make([]byte, RECV_BUF_LEN)
		sendSize, _ := conn.Write([]byte(fmt.Sprintln(clientName, " echo: ", i)))
		recvSize, _ := conn.Read(buf)
		if recvSize != sendSize {
			fmt.Println(recvSize, " ", sendSize)
			panic("size error!")
		}
	}
	conn.Close()
	c <- true
}

func client(cp *connectionPool, clientName string, c chan bool) {
	for i := 0; i < 10; i++ {
		//fmt.Println(clientName, i)
		var retry int
		conn, index := cp.getConn()
		buf := make([]byte, RECV_BUF_LEN)

		for retry := 0; retry < 3; retry++ {

			sendSize, err := conn.Write([]byte(fmt.Sprintln(clientName, " echo: ", i)))

			if err != nil {
				cp.renewConn(index)
				continue
			}

			recvSize, err := conn.Read(buf)
			if err != nil {
				cp.renewConn(index)
				continue
			}
			if recvSize != sendSize {
				fmt.Println(recvSize, " ", sendSize)
				panic("size error!")
			}
			break
		}
		cp.releaseConn(index)
		if retry == 3 {
			panic("retry too many times")
		}
	}
	c <- true
}

func server() {
	println("Starting the server")

	listener, err := net.Listen("tcp", "0.0.0.0:6666")
	if err != nil {
		fmt.Println("error listening:", err.Error())
		os.Exit(1)
	}

	for {
		conn, err := listener.Accept()
		if err != nil {
			println("Error accept:", err.Error())
			return
		}
		go EchoFunc(conn)
	}
}

func stupidServer() {
	println("Starting stupid the server")

	listener, err := net.Listen("tcp", "0.0.0.0:7777")
	if err != nil {
		fmt.Println("error listening:", err.Error())
		os.Exit(1)
	}

	for {
		conn, err := listener.Accept()
		if err != nil {
			println("Error accept:", err.Error())
			return
		}
		go EchoFunc(conn)
	}
}

func EchoFunc(conn net.Conn) {
	for {
		buf := make([]byte, RECV_BUF_LEN)
		n, err := conn.Read(buf)
		if err != nil {
			conn.Close()
			return
		}

		//send reply
		_, err = conn.Write(buf[0:n])
		if err != nil {
			fmt.Println("Error send reply:", err.Error())
		}
	}
}
