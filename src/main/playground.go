package main

import (
	"time"
)

func main() {
	ch := make(chan bool, 1)

	fun := func() {
		time.Sleep(2 * time.Second)
		ch <- true
	}

	go fun()
	select {
	case <-time.After(1 * time.Second):
		println("Timeout")
	case <-ch:
		println("Operation Success")
	}

	time.Sleep(2 * time.Second)
}
