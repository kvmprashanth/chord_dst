package main

import(
	"fmt"
	"chord"
	"time"
	"sort"
	"crypto/sha1"
)

func prepRing(port int) (*chord.Config, *chord.TCPTransport, error) {
	listen := fmt.Sprintf("127.0.0.1:%d", port)
	conf := &chord.Config{
		NumVnodes:     8,
		NumSuccessors: 8,
		HashFunc:      sha1.New,
		HashBits:      160,
		StabilizeMin:  time.Second,
		StabilizeMax:  5 * time.Second,
	}
	timeout := time.Duration(200 * time.Millisecond)
	trans, err := chord.InitTCPTransport(listen, timeout)
	if err != nil {
		return nil, nil, err
	}
	return conf, trans, nil
}

func main(){
	
	fmt.Println("Chord DST \n\n")
	
	// Prepare to create 2 nodes
	conf, trans, err := prepRing(10001)
	if err != nil {
		fmt.Println("unexpected err. %s", err)
	}

	// Create initial ring
	r, err := chord.Create(conf, trans)
	sort.Sort(r)

	if err != nil {
		fmt.Println("unexpected err. %s", err)
	}
	
	fmt.Println(r.Vnodes)	
	
	num := r.Len()
	for i := 0; i < num; i++ {
		r.Vnodes[i].Init(i)
		r.Vnodes[i].Successors[0] = &r.Vnodes[(i+1)%num].Vnode
	}

	// Fix finger should not error
	vn := r.Vnodes[0]
	if err := vn.FixFingerTable(); err != nil {
		fmt.Println("unexpected err. %s", err)
	}

	// Check we've progressed
	if vn.Last_finger != 158 {
		fmt.Println("unexpected last finger. %s", err)
	}

	// Ensure that we've setup our successor as the initial entries
	for i := 0; i < vn.Last_finger; i++ {
		if vn.Finger[i] != vn.Successors[0] {
			fmt.Println("unexpected finger entry!")
		}
	}

	// Fix next index
	if err := vn.FixFingerTable(); err != nil {
		fmt.Println("unexpected err, %s", err)
	}
	if vn.Last_finger != 0 {
		fmt.Println("unexpected last finger! %d", vn.Last_finger)
	}

	for i := 0; i < num; i++ {
		r.Vnodes[i].Leave()
	}
	
	r.Shutdown()
	trans.Shutdown()

}
