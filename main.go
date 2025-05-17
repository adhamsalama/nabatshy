package main

import (
	"nabatshy/api"
	"nabatshy/collector"
)

func main() {
	go func() { api.Run() }()
	collector.Run()
}
