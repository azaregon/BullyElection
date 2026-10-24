package main

import (
	"fmt"
	"time"
	"strings"
	"os"
	"strconv"

	"go.nanomsg.org/mangos/v3"
	"go.nanomsg.org/mangos/v3/protocol/rep" // Example: Reply protocol socket
	"go.nanomsg.org/mangos/v3/protocol/req"
	_ "go.nanomsg.org/mangos/v3/transport/tcp"
)

var NODEID int;
var CURRENTCOORDINATOR = 7;
var BASEPORT = 5000
var MAXNODEID = 7;


func nodePrintf(format string, args ...any) {
	fmt.Printf("[ node %d] "+format+"\n", append([]any{NODEID}, args...)...)
}


func sendToNode(targetIp string,targetPort string, nodeID int, message string) (string,error) {
	var sock mangos.Socket
	var err error

	if sock, err = req.NewSocket(); err != nil {
		return "",err
	}
	defer sock.Close()

	// Dial to the target node's address
	addr := "tcp://127.0.0.1:" + targetPort
	if err = sock.Dial(addr); err != nil {
		return "", err
	}

	// Set a small write/send timeout so it doesn't hang forever if the node is dead
	sock.SetOption(mangos.OptionSendDeadline, 1*time.Second)

	// Send message
	if err = sock.Send([]byte(buildMessage(nodeID, message))); err != nil {
		return "", err
	}

	// Wait for reply (like "OK")
	reply, err := sock.Recv()
	if err != nil {
		return "", err // If target node is dead (Node 7), this will trigger an error/timeout!
	}
	a, _ := strconv.Atoi(targetPort)

	nodePrintf("Got reply from %d: %s", a-BASEPORT, string(reply))
	return string(reply), nil
}


func buildMessage(node_id int, msg string) string {
    return fmt.Sprintf("%d:%s", node_id, msg)
}

func parseMessage(msg string) (string, []string) {
    parts := strings.Split(msg, ":")
    if len(parts) < 2 {
        return "", []string{}
    }
    nodeFromStr := parts[0]
    actualMsg := parts[1:] // Returns slice like ["ELECTION"] or ["COORDINATOR", "6"]
    return nodeFromStr, actualMsg
}

func startElection(nodeID int) {
	got_OK := false
	for i := nodeID + 1; i <= MAXNODEID; i++ {
		// EXPLANATION::: lets try to send to the higher nodes, is there any?
        portStr := fmt.Sprintf("%d", 5000+i)
		repl, err := sendToNode("127.0.0.1", portStr, NODEID, "ELECTION")
		if repl == "OK" && err == nil {
			got_OK = true;
		}	
    }

	if got_OK {
		// EXPLANATION::: got OK, so, there are/is higher node ID than me, ok then
		return
	} else {

		// if there is no another OK, then this node is the highest node
		for i := 0; i <= MAXNODEID; i++ {
			if i == NODEID {
				continue
			}
			repl, _ := sendToNode("127.0.0.1", fmt.Sprintf("%d", 5000+i), NODEID, fmt.Sprintf("COORDINATOR:%d", NODEID))
			if repl == "OK" {
				got_OK = true;
			}	
		}
	}
	
}


func main() {
	time.Sleep(2 * time.Second)
	if len(os.Args) < 3 {
		nodeID := "?"
		if len(os.Args) > 1 {
			nodeID = os.Args[1]
		}
		fmt.Printf("[ node %s] Usage: ./bully <node_id> <state>\n", nodeID)
		fmt.Printf("[ node %s] Example: ./bully 4 live_init\n", nodeID)
		os.Exit(1)
	}
nodeIDstr := os.Args[1]
    nodeIDchange, err := strconv.Atoi(nodeIDstr) // First time err is declared in main
    if err != nil {
        fmt.Printf("[ node %s] Invalid Node ID: must be an integer\n", nodeIDstr)
        os.Exit(1)
    }
    NODEID = nodeIDchange

    state := os.Args[2]
    nodePrintf("Starting up with state: %s", state)

    switch state {
    case "dead":
        os.Exit(1)
		return
    case "live_init":
		time.Sleep(3 * time.Second)
        startElection(NODEID)
    }

    ip := "127.0.0.1"
    port := fmt.Sprintf("%d", BASEPORT+NODEID)
    ip_port := ip + ":" + port

    var sock mangos.Socket
    
    // Use '=' instead of ':=' because err already exists in this function block
    sock, err = rep.NewSocket() 
    if err != nil {
        nodePrintf("Can't get new rep socket: %s", err)
        os.Exit(1)
    }

    // Use '=' for err here too
    err = sock.Listen("tcp://" + ip_port)
    if err != nil {
        nodePrintf("Can't listen on socket: %s", err)
        os.Exit(1)
    }

    nodePrintf("Node listening on %s...", ip_port)

	// 3. Background loop to continuously receive messages
	go func() {
		for {
			// Recv blocks until a message arrives from a peer
			msg, err := sock.Recv()
			if err != nil {
				nodePrintf("Error receiving: %s", err)
				break
			}
			
			nodePrintf("Received message: %s", string(msg))

			node_from_str, actual_msg := parseMessage(string(msg))
			node_from, err := strconv.Atoi(node_from_str)

			switch actual_msg[0] {
			case "ELECTION":
				if node_from < NODEID {
					// EXPLANATION::: the node id where the message from is lower than me
					sock.Send([]byte("OK"))
					
					// EXPLANATION::: so, send the ok, then let's see is there any higher ID node?
					go startElection(NODEID) // Kick off your election routine
				} else {
					// EXPLANATION::: the node id where the message from is higher than me
					// EXPLANATION::: just need to send the ok, because I will never be the COORDINATOR
					sock.Send([]byte("OK"))
				}

			case "COORDINATOR":
				// EXPLANATION::: the new coordinator has been elected, so, let's update the data.
				new_coordinator_int, _ := strconv.Atoi(actual_msg[1])
				CURRENTCOORDINATOR = new_coordinator_int
				nodePrintf("NEW COORDINATOR: %d", new_coordinator_int)
				sock.Send([]byte("OK"))
			}

		}
	}()

	// Keep main running
	select {}
}