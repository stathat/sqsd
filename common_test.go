package main

import "sync"

var setupOnce sync.Once

func Setup() {
	setupOnce.Do(setup)
	reset()
}

func setup() {
	*port = 19324
	go run("")
}
