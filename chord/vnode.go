package chord

import (
	"encoding/binary"
	"fmt"
	//"log"
	"time"
)

// Converts the ID to string
func (vn *Vnode) String() string {
	return fmt.Sprintf("%x", vn.Id)[:2]
}

// Initializes a local vnode
func (vn *LocalVnode) Init(idx int) {
	// Generate an ID
	vn.genId(uint16(idx))

	// Set our host
	vn.Host = vn.Ring.config.Hostname

	// Initialize all state
	vn.Successors = make([]*Vnode, vn.Ring.config.NumSuccessors)
	vn.Finger = make([]*Vnode, vn.Ring.config.HashBits)
	vn.Map = map[string]string{}

	// Register with the RPC mechanism
	vn.Ring.transport.Register(&vn.Vnode, vn)

	// Used to stablize network at regular intervels
	vn.schedule()
}

// Schedules the Vnode to do regular maintenence
func (vn *LocalVnode) schedule() {
	// Setup our stabilize timer
	vn.Timer = time.AfterFunc(randStabilize(vn.Ring.config), vn.stabilize)
}

func (vn *LocalVnode) PutKey(value int) error {
	vn.Test = value
	return nil
}

func (vn *LocalVnode) GetKey() (int, error) {
	return vn.Test, nil
}
/*
Notify(target, self *Vnode) ([]*Vnode, error)
Notify(*Vnode) ([]*Vnode, error)
func (vn *LocalVnode) Notify(maybe_pred *Vnode) ([]*Vnode, error)

GetPredecessor(*Vnode) (*Vnode, error)
GetPredecessor() (*Vnode, error)
func (vn *LocalVnode) GetPredecessor() (*Vnode, error)
*/

// Generates an ID for the node
func (vn *LocalVnode) genId(idx uint16) {
	// Use the hash funciton
	conf := vn.Ring.config
	hash := conf.HashFunc()
	hash.Write([]byte(conf.Hostname))
	binary.Write(hash, binary.BigEndian, idx)

	// Use the hash as the ID
	vn.Id = hash.Sum(nil)
}

// Called to periodically stabilize the vnode
func (vn *LocalVnode) stabilize() {
	// Clear the timer
	vn.Timer = nil

	// Check for shutdown
	if vn.Ring.shutdown != nil {
		vn.Ring.shutdown <- true
		return
	}

	// Setup the next stabilize timer
	defer vn.schedule()

	// Check for new successor
	if err := vn.CheckNewSuccessor(); err != nil {
		//log.Printf("[ERR] Error checking for new successor: %s", err)
	}

	// Notify the successor
	if err := vn.notifySuccessor(); err != nil {
		//log.Printf("[ERR] Error notifying successor: %s", err)
	}

	// Finger table fix up
	if err := vn.FixFingerTable(); err != nil {
		//log.Printf("[ERR] Error fixing finger table: %s", err)
	}

	// Check the predecessor
	if err := vn.checkPredecessor(); err != nil {
		//log.Printf("[ERR] Error checking predecessor: %s", err)
	}

	// Set the last stabilized time
	vn.Stabilized = time.Now()
}

// Checks for a new successor
func (vn *LocalVnode) CheckNewSuccessor() error {
	// Ask our successor for it's predecessor
	trans := vn.Ring.transport

CHECK_NEW_SUC:
	succ := vn.Successors[0]
	if succ == nil {
		panic("Node has no successor!")
	}
	maybe_suc, err := trans.GetPredecessor(succ)
	if err != nil {
		// Check if we have succ list, try to contact next live succ
		known := vn.knownSuccessors()
		if known > 1 {
			for i := 0; i < known; i++ {
				if alive, _ := trans.Ping(vn.Successors[0]); !alive {
					// Don't eliminate the last successor we know of
					if i+1 == known {
						return fmt.Errorf("All known successors dead!")
					}

					// Advance the successors list past the dead one
					copy(vn.Successors[0:], vn.Successors[1:])
					vn.Successors[known-1-i] = nil
				} else {
					// Found live successor, check for new one
					goto CHECK_NEW_SUC
				}
			}
		}
		return err
	}

	// Check if we should replace our successor
	if maybe_suc != nil && between(vn.Id, succ.Id, maybe_suc.Id) {
		// Check if new successor is alive before switching
		alive, err := trans.Ping(maybe_suc)
		if alive && err == nil {
			copy(vn.Successors[1:], vn.Successors[0:len(vn.Successors)-1])
			vn.Successors[0] = maybe_suc
		} else {
			return err
		}
	}
	return nil
}

// RPC: Invoked to return out predecessor
func (vn *LocalVnode) GetPredecessor() (*Vnode, error) {
	return vn.Predecessor, nil
}

// Notifies our successor of us, updates successor list
func (vn *LocalVnode) notifySuccessor() error {
	// Notify successor
	succ := vn.Successors[0]
	succ_list, err := vn.Ring.transport.Notify(succ, &vn.Vnode)
	if err != nil {
		return err
	}

	// Trim the successors list if too long
	max_succ := vn.Ring.config.NumSuccessors
	if len(succ_list) > max_succ-1 {
		succ_list = succ_list[:max_succ-1]
	}

	// Update local successors list
	for idx, s := range succ_list {
		if s == nil {
			break
		}
		// Ensure we don't set ourselves as a successor!
		if s == nil || s.String() == vn.String() {
			break
		}
		vn.Successors[idx+1] = s
	}
	return nil
}

// RPC: Notify is invoked when a Vnode gets notified
func (vn *LocalVnode) Notify(maybe_pred *Vnode) ([]*Vnode, error) {
	// Check if we should update our predecessor
	if vn.Predecessor == nil || between(vn.Predecessor.Id, vn.Id, maybe_pred.Id) {
		// Inform the delegate
		conf := vn.Ring.config
		old := vn.Predecessor
		vn.Ring.invokeDelegate(func() {
			conf.Delegate.NewPredecessor(&vn.Vnode, maybe_pred, old)
		})

		vn.Predecessor = maybe_pred
	}

	// Return our successors list
	return vn.Successors, nil
}

// Fixes up the finger table
func (vn *LocalVnode) FixFingerTable() error {

	// Determine the offset
	hb := vn.Ring.config.HashBits
	offset := powerOffset(vn.Id, vn.Last_finger, hb)

	//fmt.Println(offset)

	// Find the successor
	nodes, err := vn.FindSuccessors(1, offset)
	if nodes == nil || len(nodes) == 0 || err != nil {
		return err
	}
	node := nodes[0]

	// Update the finger table
	vn.Finger[vn.Last_finger] = node

	// Try to skip as many finger entries as possible
	for {

		//fmt.Println(vn.Last_finger)

		next := vn.Last_finger + 1
		if next >= hb {
			break
		}
		offset := powerOffset(vn.Id, next, hb)

		// While the node is the successor, update the finger entries
		if betweenRightIncl(vn.Id, node.Id, offset) {
			vn.Finger[next] = node
			vn.Last_finger = next
		} else {
			break
		}
	}

	// Increment to the index to repair
	if vn.Last_finger+1 == hb {
		vn.Last_finger = 0
	} else {
		vn.Last_finger++
	}

	return nil
}

// Checks the health of our predecessor
func (vn *LocalVnode) checkPredecessor() error {
	// Check predecessor
	if vn.Predecessor != nil {
		res, err := vn.Ring.transport.Ping(vn.Predecessor)
		if err != nil {
			return err
		}

		// Predecessor is dead
		if !res {
			vn.Predecessor = nil
		}
	}
	return nil
}

// Finds next N successors. N must be <= NumSuccessors
func (vn *LocalVnode) FindSuccessors(n int, key []byte) ([]*Vnode, error) {
	// Check if we are the immediate predecessor
	if betweenRightIncl(vn.Id, vn.Successors[0].Id, key) {
		return vn.Successors[:n], nil
	}

	// Try the closest preceeding nodes
	cp := closestPreceedingVnodeIterator{}
	cp.init(vn, key)
	for {
		// Get the next closest node
		closest := cp.Next()
		if closest == nil {
			break
		}

		// Try that node, break on success
		res, err := vn.Ring.transport.FindSuccessors(closest, n, key)
		if err == nil {
			return res, nil
		} else {
			//log.Printf("[ERR] Failed to contact %s. Got %s", closest.String(), err)
		}
	}

	// Determine how many successors we know of
	successors := vn.knownSuccessors()

	// Check if the ID is between us and any non-immediate successors
	for i := 1; i <= successors-n; i++ {
		if betweenRightIncl(vn.Id, vn.Successors[i].Id, key) {
			remain := vn.Successors[i:]
			if len(remain) > n {
				remain = remain[:n]
			}
			return remain, nil
		}
	}

	// Checked all closer nodes and our successors!
	return nil, fmt.Errorf("Exhausted all preceeding nodes!")
}

// Instructs the vnode to leave
func (vn *LocalVnode) Leave() error {
	// Inform the delegate we are leaving
	conf := vn.Ring.config
	pred := vn.Predecessor
	succ := vn.Successors[0]
	vn.Ring.invokeDelegate(func() {
		conf.Delegate.Leaving(&vn.Vnode, pred, succ)
	})

	// Notify predecessor to advance to their next successor
	var err error
	trans := vn.Ring.transport
	if vn.Predecessor != nil {
		err = trans.SkipSuccessor(vn.Predecessor, &vn.Vnode)
	}

	// Notify successor to clear old predecessor
	err = mergeErrors(err, trans.ClearPredecessor(vn.Successors[0], &vn.Vnode))
	return err
}

// Used to clear our predecessor when a node is leaving
func (vn *LocalVnode) ClearPredecessor(p *Vnode) error {
	if vn.Predecessor != nil && vn.Predecessor.String() == p.String() {
		// Inform the delegate
		conf := vn.Ring.config
		old := vn.Predecessor
		vn.Ring.invokeDelegate(func() {
			conf.Delegate.PredecessorLeaving(&vn.Vnode, old)
		})
		vn.Predecessor = nil
	}
	return nil
}

// Used to skip a successor when a node is leaving
func (vn *LocalVnode) SkipSuccessor(s *Vnode) error {
	// Skip if we have a match
	if vn.Successors[0].String() == s.String() {
		// Inform the delegate
		conf := vn.Ring.config
		old := vn.Successors[0]
		vn.Ring.invokeDelegate(func() {
			conf.Delegate.SuccessorLeaving(&vn.Vnode, old)
		})

		known := vn.knownSuccessors()
		copy(vn.Successors[0:], vn.Successors[1:])
		vn.Successors[known-1] = nil
	}
	return nil
}

// Determine how many successors we know of
func (vn *LocalVnode) knownSuccessors() (successors int) {
	for i := 0; i < len(vn.Successors); i++ {
		if vn.Successors[i] != nil {
			successors = i + 1
		}
	}
	return
}
