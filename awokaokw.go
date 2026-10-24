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