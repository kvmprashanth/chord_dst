package chord

import (
	"math/big"
)

type closestPreceedingVnodeIterator struct {
	key           []byte
	vn            *LocalVnode
	finger_idx    int
	successor_idx int
	yielded       map[string]struct{}
}

func (cp *closestPreceedingVnodeIterator) init(vn *LocalVnode, key []byte) {
	cp.key = key
	cp.vn = vn
	cp.successor_idx = len(vn.Successors) - 1
	cp.finger_idx = len(vn.Finger) - 1
	cp.yielded = make(map[string]struct{})
}

func (cp *closestPreceedingVnodeIterator) Next() *Vnode {
	// Try to find each node
	var successor_node *Vnode
	var finger_node *Vnode

	// Scan to find the next successor
	vn := cp.vn
	var i int
	for i = cp.successor_idx; i >= 0; i-- {
		if vn.Successors[i] == nil {
			continue
		}
		if _, ok := cp.yielded[vn.Successors[i].String()]; ok {
			continue
		}
		if between(vn.Id, cp.key, vn.Successors[i].Id) {
			successor_node = vn.Successors[i]
			break
		}
	}
	cp.successor_idx = i

	// Scan to find the next finger
	for i = cp.finger_idx; i >= 0; i-- {
		if vn.Finger[i] == nil {
			continue
		}
		if _, ok := cp.yielded[vn.Finger[i].String()]; ok {
			continue
		}
		if between(vn.Id, cp.key, vn.Finger[i].Id) {
			finger_node = vn.Finger[i]
			break
		}
	}
	cp.finger_idx = i

	// Determine which node is better
	if successor_node != nil && finger_node != nil {
		// Determine the closer node
		hb := cp.vn.Ring.config.HashBits
		closest := closest_preceeding_vnode(successor_node,
			finger_node, cp.key, hb)
		if closest == successor_node {
			cp.successor_idx--
		} else {
			cp.finger_idx--
		}
		cp.yielded[closest.String()] = struct{}{}
		return closest

	} else if successor_node != nil {
		cp.successor_idx--
		cp.yielded[successor_node.String()] = struct{}{}
		return successor_node

	} else if finger_node != nil {
		cp.finger_idx--
		cp.yielded[finger_node.String()] = struct{}{}
		return finger_node
	}

	return nil
}

// Returns the closest preceeding Vnode to the key
func closest_preceeding_vnode(a, b *Vnode, key []byte, bits int) *Vnode {
	a_dist := distance(a.Id, key, bits)
	b_dist := distance(b.Id, key, bits)
	if a_dist.Cmp(b_dist) <= 0 {
		return a
	} else {
		return b
	}
}

// Computes the forward distance from a to b modulus a ring size
func distance(a, b []byte, bits int) *big.Int {
	// Get the ring size
	var ring big.Int
	ring.Exp(big.NewInt(2), big.NewInt(int64(bits)), nil)

	// Convert to int
	var a_int, b_int big.Int
	(&a_int).SetBytes(a)
	(&b_int).SetBytes(b)

	// Compute the distances
	var dist big.Int
	(&dist).Sub(&b_int, &a_int)

	// Distance modulus ring size
	(&dist).Mod(&dist, &ring)
	return &dist
}
