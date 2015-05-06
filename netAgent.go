// Embedded net Agent

package netAgent

import (
	"errors"
	"fmt"
	"github.com/golang/glog"
	_ "github.com/hashicorp/consul/api"
	_ "github.com/hashicorp/consul/command"
	"github.com/hashicorp/consul/watch"
	"github.com/mitchellh/cli"
	"net"
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
		args = append(args, "-bootstrap-expect 1")
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
