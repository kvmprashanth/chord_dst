package main

import(
	"fmt"
	"chord"
	"time"
	//"sort"
	"crypto/sha1"
)

func prepRing(port int) (*chord.Config, *chord.TCPTransport, error) {
	listen := fmt.Sprintf("localhost:%d", port)
	conf := &chord.Config{
		Hostname:	   listen,
		NumVnodes:     2,
		NumSuccessors: 1,
		HashFunc:      sha1.New,
		HashBits:      160,
		StabilizeMin:  time.Second,
		StabilizeMax:  5 * time.Second,
	}
	conf.StabilizeMin = time.Duration(15 * time.Millisecond)
	conf.StabilizeMax = time.Duration(45 * time.Millisecond)
	timeout := time.Duration(20 * time.Millisecond)
	trans, err := chord.InitTCPTransport(listen, timeout)
	if err != nil {
		return nil, nil, err
	}
	return conf, trans, nil
}

func main() {
	// Prepare to create 2 nodes
	c1, t1, err := prepRing(10025)
	if err != nil {
		fmt.Println("unexpected err. %s", err)
	}
	c2, t2, err := prepRing(10026)
	if err != nil {
		fmt.Println("unexpected err. %s", err)
	}

	// Create initial ring
	r1, err := chord.Create(c1, t1)
	if err != nil {
		fmt.Println("unexpected err. %s", err)
	}

	// Join ring
	r2, err := chord.Join(c2, t2, c1.Hostname)
	if err != nil {
		fmt.Println("failed to join local node! Got %s", err)
	}

	fmt.Println(r1.Vnodes)	
	fmt.Println(r2.Vnodes)	

	for i := 0; i < r1.Len(); i++ {
		//r1.Vnodes[i].Leave()
	}

	// Shutdown
	r1.Shutdown()
	r2.Shutdown()
	t1.Shutdown()
	t2.Shutdown()
}
