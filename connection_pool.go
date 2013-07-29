package main

import (
	"net"
	"sync"
)

const (
	MinSize = 10
	MaxSize = 20
)

type connectionPool struct {
	// server name is in the format of "IP:PORT",
	// such as "127.0.0.1:11211"
	serverName string
	connects   []net.Conn
	available  chan int
	currSize   int
	modifyLock sync.Mutex
}

func createConnectionPool(serverName string) (*connectionPool, error) {
	cp := new(connectionPool)
	cp.serverName = serverName
	cp.currSize = MinSize

	cp.connects = make([]net.Conn, MaxSize)
	cp.available = make(chan int, MaxSize)
	var err error
	for i := 0; i < MinSize; i++ {
		cp.connects[i], err = net.Dial("tcp", serverName)
		if err != nil {
			return nil, err
		}
		// fill available
		cp.available <- i
	}
	return cp, nil
}

func (cp *connectionPool) getConn() (net.Conn, int) {
	i := <-cp.available
	return cp.connects[i], i
}

func (cp *connectionPool) decrConnNum(n int) error {
	cp.modifyLock.Lock()
	defer cp.modifyLock.Unlock()

	for i := 0; i < n; i++ {
		k := <-cp.available
		cp.connects[k].Close()
		cp.connects[k] = nil
		cp.currSize--
	}
	return nil
}

func (cp *connectionPool) incrConnNum(n int) error {
	cp.modifyLock.Lock()
	defer cp.modifyLock.Unlock()

	var err error
	for i := 0; i < MaxSize; i++ {
		if cp.connects[i] == nil {
			cp.connects[i], err = net.Dial("tcp", cp.serverName)
			if err != nil {
				return err
			}
			cp.available <- i
			cp.currSize++
			n--
			if n == 0 {
				return nil
			}
		}
	}
	return nil
}

func (cp *connectionPool) renewConn(i int) error {
	cp.connects[i].Close()
	var err error
	cp.connects[i], err = net.Dial("tcp", cp.serverName)
	return err
}

func (cp *connectionPool) releaseConn(i int) {
	cp.available <- i
}

func (cp *connectionPool) destory() {
	cp.modifyLock.Lock()
	defer cp.modifyLock.Unlock()

	// close all the connections and set to nil
	for i := 0; i < MaxSize; i++ {
		if cp.connects[i] != nil {
			cp.connects[i].Close()
			cp.connects[i] = nil
			cp.currSize--
		}
	}

	// clear the chan cp.available
	for {
		select {
		case <-cp.available:
		default:
			break
		}
	}
}
