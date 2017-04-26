package main

import(
	"fmt"
	"chord"
	"time"
	"sort"
	"crypto/sha1"
)

func prepRing(listen string) (*chord.Config, *chord.TCPTransport, error) {
	conf := &chord.Config{
		Hostname:	   listen,
		NumVnodes:     2,
		NumSuccessors: 1,
		HashFunc:      sha1.New,
		HashBits:      160,
		StabilizeMin:  15 * time.Millisecond,
		StabilizeMax:  60 * time.Millisecond,
	}
	timeout := time.Duration(20 * time.Millisecond)
	trans, err := chord.InitTCPTransport(listen, timeout)
	if err != nil {
		return nil, nil, err
	}
	return conf, trans, nil
}

func main(){
	
	fmt.Println("Chord DST \n\n")
	
	// Prepare to create 2 nodes
	c1, t1, err := prepRing("localhost:10001")
	if err != nil {
		fmt.Println("unexpected err. %s", err)
	}

	// Create initial ring
	r1, err := chord.Create(c1, t1)
	if err != nil {
		fmt.Println("unexpected err. %s", err)
	}

	// Create a second ring
	c2, t2, err := prepRing("192.168.0.102:10002")
	r2, err := chord.Join(c2, t2, c1.Hostname)
	if err != nil {
		fmt.Println("failed to join remote node! Got %s", err)
	}

	c3, t3, err := prepRing("192.168.0.102:10003")
	r3, err := chord.Join(c3, t3, c1.Hostname)
	if err != nil {
		fmt.Println("failed to join remote node! Got %s", err)
	}

	time.Sleep(10 * time.Second)
	sort.Sort(r3)
	
	
	for i := 0; i < r1.Len(); i++ {
		fmt.Println(r1.Vnodes[i], r1.Vnodes[i].Successors[0])
	}

	for i := 0; i < r2.Len(); i++ {
		fmt.Println(r2.Vnodes[i], r2.Vnodes[i].Successors[0])
	}

	for i := 0; i < r3.Len(); i++ {
		fmt.Println(r3.Vnodes[i], r3.Vnodes[i].Successors[0])
	}

	//fmt.Println(r2.Vnodes)
	
	r1.Shutdown()
	r2.Shutdown()
	r3.Shutdown()

	t1.Shutdown()
	t2.Shutdown()
	t3.Shutdown()
}


/*

	// Fix finger should not error
	vn := r.Vnodes[0]
	if err := vn.FixFingerTable(); err != nil {
		fmt.Println("unexpected err. %s", err)
	}

	//fmt.Println(vn.Finger)

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
	/*if vn.Last_finger != 0 {
		fmt.Println("unexpected last finger! %d", vn.Last_finger)
	}

	*/