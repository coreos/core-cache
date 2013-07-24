package main

import (
	"bytes"
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"github.com/coreos/etcd/store"
	"github.com/coreos/go-etcd/etcd"
	"github.com/stathat/consistent"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
)

const (
	BufferLength     = 1024
	Magic            = 0x80
	BinaryHeaderSize = 24
)

var rwLock sync.RWMutex

var c *consistent.Consistent

var port int

func init() {
	flag.IntVar(&port, "p", 22122, "the port of the ds-memcached proxy")
}

func main() {

	flag.Parse()

	c = consistent.New()

	etcd.SyncCluster()

	presps, err := etcd.Get("/service/memcached")

	if err != nil {
		fmt.Println("Add at least one memcached instance under path")
		os.Exit(1)
	}

	for _, resp := range *(presps) {
		debugln("Add server ", resp.Value)
		c.Add(resp.Value)
	}

	go watch()

	startProxy()
}

func watch() {
	receiver := make(chan *store.Response, 10)
	stop := make(chan bool, 1)
	go update(receiver)
	etcd.Watch("/service/memcached", 0, receiver, &stop)
}

func update(receiver chan *store.Response) {
	for {
		resp := <-receiver
		debugln("Add server ", resp.Value)
		c.Add(resp.Value)
	}
}

func dispatch(key string) string {
	//do load balance
	server, _ := c.Get(key)

	debugln("map", key, "to server ", server)

	return server
}

func startProxy() {

	listener, err := net.Listen("tcp", "0.0.0.0:"+strconv.Itoa(port))

	if err != nil {
		fmt.Println("error listening:", err.Error())
		os.Exit(1)
	}

	debugln("Start listening on port", port)

	for {
		conn, err := listener.Accept()
		if err != nil {
			warnln("Error accept:", err.Error())
			return
		}
		go serve(conn)
	}

}

func serve(conn net.Conn) {
	clientAddr := conn.RemoteAddr().String()

	debugln("accept client ", clientAddr)
	for {
		buf := make([]byte, BufferLength)
		n, err := conn.Read(buf)

		if err != nil {
			return
		}

		debugln("received ", n, " bytes of data from [", clientAddr, "]")

		if buf[0] == Magic {
			err = serveBinary(conn, buf)
		} else {
			err = serveText(conn, buf)
		}

		if err != nil {
			conn.Write([]byte(err.Error()))
			conn.Close()
			return
		}

	}

}

func serveText(conn net.Conn, buf []byte) error {
	clientAddr := conn.RemoteAddr().String()

	debugln("received text protocol from ", clientAddr)
	content := string(buf)

	index := strings.Index(content, "\r\n")

	if index == -1 {
		debugln("[text protocol] bad request from ", clientAddr)
		return errors.New("ERROR\r\n")
	}

	header := content[0:index]

	debugln("[text protocol] header from ", clientAddr)

	fields := strings.Split(header, " ")

	op := fields[0]

	var err error

	if textCommands[op] {
		// all the ops in the command list should be like
		// [op] [key] ...
		// we only need to grab the key to do hashing
		// proxy does not care about other stuffs

		if len(fields) < 2 {
			// bad request if there is no key
			return errors.New("ERROR\r\n")
		}

		key := fields[1]

		err = serveCommands(conn, key, buf)

		return err

	} else if op == "stats" {

		serveStat(conn, buf)

	} else {

		return errors.New("Unimplemented\r\n")
	}

	return err
}

func serveBinary(conn net.Conn, buf []byte) error {
	clientAddr := conn.RemoteAddr().String()

	debugln("received binary protocol from ", clientAddr)

	bufLength := len(buf)

	if bufLength < BinaryHeaderSize {
		return errors.New("ERROR\r\n")
	}

	op := buf[1]

	var err error

	if binaryCommands[op] {
		var keyLength uint16

		keyLengthBuf := bytes.NewBuffer(buf[2:4])
		err := binary.Read(keyLengthBuf, binary.BigEndian, &keyLength)

		if err != nil {
			return errors.New("ERROR\r\n")
		}

		key := string(buf[BinaryHeaderSize : BinaryHeaderSize+keyLength])

		err = serveCommands(conn, key, buf)
	}
	return err
}

func serveCommands(conn net.Conn, key string, buf []byte) error {
	clientAddr := conn.RemoteAddr().String()

	recv_buf := make([]byte, BufferLength)

	server := dispatch(key)

	memcachedConn, err := net.Dial("tcp", server)

	if err != nil {
		errString := "SERVER_ERROR <" + err.Error() + ">\r\n"
		conn.Write([]byte(errString))
		return nil
	}

	// send package to actual memcached server
	_, err = memcachedConn.Write(buf)

	if err != nil {
		errString := "SERVER_ERROR <" + err.Error() + ">\r\n"
		conn.Write([]byte(errString))
		return nil
	}

	// read result
	num, err := memcachedConn.Read(recv_buf)

	if err != nil {
		errString := "SERVER_ERROR <" + err.Error() + ">\r\n"
		conn.Write([]byte(errString))
		return nil
	}

	_, err = conn.Write(recv_buf[:num])

	if err != nil {
		debugln("cannot send result back to client ", clientAddr)
		return err
	}
	return nil
}

func serveStat(conn net.Conn, buf []byte) {
	total_get_hits := 0
	total_get_misses := 0
	total_cmd_get := 0
	total_cmd_set := 0

	recv_buf := make([]byte, BufferLength)
	// collect info from each real memcached
	for _, server := range c.Members() {

		memcachedConn, err := net.Dial("tcp", server)
		if err != nil {
			fmt.Println("error connect to memcached server:", err.Error())
		}

		//fmt.Println("send to memcached: ", string(buf))
		// send package to actual memcached server
		_, err = memcachedConn.Write(buf)

		if err != nil {
			println("Error send reply:", err.Error())
		}

		// read result
		_, err = memcachedConn.Read(recv_buf)

		//fmt.Println("recv to memcached: ", string(buf))

		stats := string(recv_buf)

		get_hits, err := getStatValue(stats, "get_hits")
		fmt.Println("server", server, "get_hits: ", get_hits)
		num_get_hits, _ := strconv.Atoi(get_hits)
		total_get_hits += num_get_hits

		get_misses, err := getStatValue(stats, "get_misses")
		fmt.Println("server", server, "get_misses: ", get_misses)
		num_get_misses, _ := strconv.Atoi(get_misses)
		total_get_misses += num_get_misses

		cmd_get, err := getStatValue(stats, "cmd_get")
		fmt.Println("server", server, "cmd_get: ", cmd_get)
		num_cmd_get, _ := strconv.Atoi(cmd_get)
		total_cmd_get += num_cmd_get

		cmd_set, err := getStatValue(stats, "cmd_set")
		fmt.Println("server", server, "cmd_set: ", cmd_set)
		num_cmd_set, _ := strconv.Atoi(cmd_set)
		total_cmd_set += num_cmd_set

	}
	str := fmt.Sprintln("total get_hits ", total_get_hits)
	_, _ = conn.Write([]byte(str))
	str = fmt.Sprintln("total get_missess ", total_get_hits)
	_, _ = conn.Write([]byte(str))
	str = fmt.Sprintln("total cmd_set ", total_cmd_set)
	_, _ = conn.Write([]byte(str))
	str = fmt.Sprintln("total cmd_get ", total_cmd_get)
	_, _ = conn.Write([]byte(str))
}

func getStatValue(stats string, key string) (string, error) {
	index := strings.Index(stats, key)
	if index == -1 {
		return "", errors.New("Stats Key Not Found")
	}

	stats = stats[index+len(key)+1:]
	index = strings.Index(stats, "\r\n")
	value := stats[:index]

	return value, nil
}
