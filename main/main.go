package main

import (
	"fmt"
	"os"
	"os/signal"
	"time"

	"github.com/WIZARD-CXY/netAgent"
	"github.com/golang/glog"
)

const dataDir = "/tmp/test"

type netListener struct {
}

func (e netListener) NotifyNodeUpdate(Type netAgent.NotifyUpdateType, nodeName string) {
	fmt.Println("Node update", Type, nodeName)
}

func (e netListener) NotifyKeyUpdate(Type netAgent.NotifyUpdateType, key string, value []byte) {
	fmt.Println("Key update", Type, key, value)
}
func (e netListener) NotifyStoreUpdate(Type netAgent.NotifyUpdateType, store string, data map[string][]byte) {

}
func main() {
	err := netAgent.StartAgent(true, true, "eth1", dataDir)

	if err != nil {
		glog.Fatalf("start agent failed")

	}
	listener := netListener{}
	go netAgent.RegisterForNodeUpdates(listener)
	go netAgent.RegisterForKeyUpdates("haha", "test", listener)

	keyUpdates("test")

	netAgent.Delete("haha", "test")
	go netAgent.RegisterForKeyUpdates("haha", "test2", listener)
	keyUpdates("test2")
	keyUpdates("test")

	// SIGINT handling gracefully shut down

	netAgent.Leave()
	time.Sleep(time.Second * 10)
	netAgent.StartAgent(true, true, "eth1", dataDir)

	handler := make(chan os.Signal, 1)
	signal.Notify(handler, os.Interrupt)
	for sig := range handler {
		if sig == os.Interrupt {
			time.Sleep(1e9)
			break
		}
	}

}

//Random Key updates
func keyUpdates(key string) {
	currVal, _, _ := netAgent.Get("network", key)
	newVal := make([]byte, len(currVal))
	newVal = []byte("value1")
	netAgent.Put("haha", key, newVal, currVal)
	time.Sleep(time.Second * 2)
	updArray := []byte("value2")
	netAgent.Put("haha", key, updArray, newVal)
	time.Sleep(time.Second * 2)
}
