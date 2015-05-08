// Embedded net Agent

package netAgent

import (
	"bytes"
	b64 "encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/golang/glog"
	_ "github.com/hashicorp/consul/api"
	_ "github.com/hashicorp/consul/command"
	"github.com/hashicorp/consul/watch"
	"github.com/mitchellh/cli"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"time"
)

func StartAgent(serverMode bool, bootstrap bool, bindInterface string, dataDir string) error {
	bindAddr := ""

	if bindInterface != "" {
		netInterface, err := net.InterfaceByName(bindInterface)
		if err != nil {
			glog.Fatalf("Error : %v", err)
			return err
		}

		addrs, err := netInterface.Addrs()

		if err == nil {
			for _, addr := range addrs {
				ip, _, _ := net.ParseCIDR(addr.String())

				if ip != nil && ip.To4() != nil {
					bindAddr = ip.To4().String()
				}
			}
		}

	}

	errChan := make(chan int)

	watchForExistingRegisteredUpdates()

	//go RegisterForNodeUpdates()
	go startConsul(serverMode, bootstrap, bindAddr, dataDir, errChan)

	select {
	case <-errChan:
		return errors.New("Error start consul agent")
	case <-time.After(time.Second * 5):
	}
	return nil
}

func startConsul(serverMode bool, bootstrap bool, bindAddress string, dataDir string, eCh chan int) {
	args := []string{"agent", "-data-dir", dataDir}

	if serverMode {
		args = append(args, "-server")
	}

	if bootstrap {
		args = append(args, "-bootstrap-expect=1")
	}

	if bindAddress != "" {
		args = append(args, "-bind")
		args = append(args, bindAddress)
		args = append(args, "-advertise")
		args = append(args, bindAddress)
	}

	ret := Execute(args...)

	eCh <- ret
}

// Node operation related
func Join(addr string) error {
	ret := Execute("join", addr)

	if ret != 0 {
		glog.Errorf("Error (%d) joining %s with consul peers", ret, addr)
		return errors.New("Error joining the cluster")
	}

	return nil
}

func Leave() error {
	ret := Execute("leave")

	if ret != 0 {
		glog.Errorf("Error leaving consul cluster")
		return errors.New("Error leaving consul cluster")
	}
	return nil
}

const CONSUL_CATALOG_BASE_URL = "http://localhost:8500/v1/catalog/"

type Node struct {
	Name    string `json:"Name,omitempty"`
	Address string `json:"Addr,ommitempty"`
}

func getNodes() ([]Node, error) {
	url := CONSUL_CATALOG_BASE_URL + "nodes"

	resp, err := http.Get(url)
	if err != nil {
		glog.Errorf("Error (%v) get %s", err, url)
		return nil, errors.New("Get nodes failed")
	}

	defer resp.Body.Close()

	glog.Infof("Get %s for %s\n", resp.Status, url)

	var nodes []Node

	err = json.NewDecoder(resp.Body).Decode(nodes)

	if err != nil {
		glog.Errorf("getNodes failed error decoding")
		return nil, errors.New("Decode error in getNodes")
	}

	return nodes, nil

}

// Execute function is borrowed from Consul's main.go
func Execute(args ...string) int {

	for _, arg := range args {
		if arg == "-v" || arg == "--version" {
			newArgs := make([]string, len(args)+1)
			newArgs[0] = "version"
			copy(newArgs[1:], args)
			args = newArgs
			break
		}
	}

	cli := &cli.CLI{
		Args:     args,
		Commands: Commands,
		HelpFunc: cli.BasicHelpFunc("consul"),
	}

	exitCode, err := cli.Run()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error executing CLI: %s\n", err.Error())
		return 1
	}

	return exitCode
}

// K/V store

const CONSUL_KV_BASE_URL = "http://localhost:8500/v1/kv/"

type KVRespBody struct {
	CreateIndex int    `json:"CreateIndex,omitempty"`
	ModifyIndex int    `json:"ModifyIndex,omitempty"`
	Key         string `json:"Key,omitempty"`
	Flags       int    `json:Flags,omitempty"`
	Value       string `json:Value,omitempty"`
}

func Get(store string, key string) ([]byte, int, bool) {
	url := CONSUL_KV_BASE_URL + store + "/" + key

	resp, err := http.Get(url)

	if err != nil {
		glog.Errorf("Error (%v) in Get for %s\n", err, url)
		return nil, 0, false
	}

	defer resp.Body.Close()

	glog.Infof("Status of Get %s for %s", resp.Status, url)

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		var jsonBody []KVRespBody

		fmt.Printf("haha %+v %+v", jsonBody, resp.Body)

		body, err := ioutil.ReadAll(resp.Body)
		err = json.Unmarshal(body, &jsonBody)

		existingValue, err := b64.StdEncoding.DecodeString(jsonBody[0].Value)

		if err != nil {
			return nil, jsonBody[0].ModifyIndex, false
		}

		return existingValue, jsonBody[0].ModifyIndex, true

	} else {
		return nil, 0, false
	}
}

const (
	OK = iota
	OUTDATED
	ERROR
)

// return val indicate the error type
func Put(store string, key string, value []byte, oldVal []byte) int {

	existingVal, casIndex, ok := Get(store, key)

	if ok && !bytes.Equal(oldVal, existingVal) {
		return OUTDATED
	}

	url := fmt.Sprintf("%s%s/%s?cas=%d", CONSUL_KV_BASE_URL, store, key, casIndex)
	glog.Infof("Updating KV pair for %s %s %s %d", url, key, value, casIndex)

	req, err := http.NewRequest("PUT", url, bytes.NewBuffer(value))

	client := &http.Client{}
	resp, err := client.Do(req)

	if err != nil {
		glog.Errorf("Error creating KV pair for %s", key)
		return ERROR
	}

	defer resp.Body.Close()

	body, _ := ioutil.ReadAll(resp.Body)

	if string(body) == "false" {
		return ERROR
	}

	return OK

}

// return val indicate the error type
func Delete(store string, key string) int {
	url := fmt.Sprintf("%s%s/%s", CONSUL_KV_BASE_URL, store, key)

	glog.Infof("Deleting KV pair for %s", url)

	req, err := http.NewRequest("DELETE", url, nil)

	client := &http.Client{}
	resp, err := client.Do(req)

	if err != nil {
		glog.Errorf("Error deleting KV pair %d", key)
		return ERROR
	}

	defer resp.Body.Close()
	return OK
}

// Watch related

const (
	NOTIFY_UPDATE_ADD = iota
	NOTIFY_UPDATE_MODIFY
	NOTIFY_UPDATE_DELETE
)

type NotifyUpdateType int

const (
	WATCH_TYPE_NODE = iota
	WATCH_TYPE_KEY
	WATCH_TYPE_STORE
	WATCH_TYPE_EVENT
)

type WatchType int

type watchData struct {
	listeners  map[string][]Listener
	watchPlans []*watch.WatchPlan
}

var watches map[WatchType]watchData = make(map[WatchType]watchData)

type Listener interface {
	NotifyNodeUpdate(NotifyUpdateType, string)
	NotifyKeyUpdate(NotifyUpdateType, string, []byte)
	NotifyStoreUpdate(NotifyUpdateType, string, map[string][]byte)
}

// enaListener implement Listener Interface
type enaListener struct {
}

/*func (e enaListener) NotifyNodeUpdate(nType NotifyUpdateType, nodeAddress string) {
	if nType == NOTIFY_UPDATE_ADD && !started {
		populateKVStoreFromCache()
	}
}

func (e enaListener) NotifyKeyUpdate(nType NotifyUpdateType, key string, data []byte) {
}
func (e enaListener) NotifyStoreUpdate(nType NotifyUpdateType, store string, data map[string][]byte) {
}*/

func watchForExistingRegisteredUpdates() {
	for wType, ws := range watches {
		glog.Info("watchForExistingRegisteredUpdates : ", wType)
		for key, _ := range ws.listeners {
			glog.Info("key : ", key)
			switch wType {
			case WATCH_TYPE_NODE:
				//go registerForNodeUpdates()
			case WATCH_TYPE_KEY:
				//go registerForKeyUpdates(key)
			case WATCH_TYPE_STORE:
				//go registerForStoreUpdates(key)
			}
		}
	}
}
