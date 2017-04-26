package main

import(
	"os"
	"fmt"
	"bytes"
	"chord"
	"time"
	"sort"
	"crypto/sha1"
)

func prepRing(listen string) (*chord.Config, *chord.TCPTransport, error) {
	conf := &chord.Config{
		Hostname:	   listen,
		NumVnodes:     2,
		NumSuccessors: 2,
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
	ip_address := os.Args[1]
	port_address := os.Args[2]
	join_req := 0

	var buffer2 bytes.Buffer

	if len(os.Args) == 5 {
		host_ip := os.Args[3]
		host_port := os. Args[4]
		buffer2.WriteString(host_ip)
		buffer2.WriteString(":")
		buffer2.WriteString(host_port)
		join_req = 1
	}

	var buffer bytes.Buffer
	buffer.WriteString(ip_address)
	buffer.WriteString(":")
	buffer.WriteString(port_address)

	c1, t1, err := prepRing(buffer.String())
	if err != nil {
		fmt.Println("unexpected err. %s", err)
	}

	// Create or join rings based on the arguments

	r1 := &chord.Ring{}

	if join_req == 0 {
		r1, err = chord.Create(c1, t1)
		if err != nil {
			fmt.Println("unexpected err. %s", err)
		}
	} else {
		r1, err = chord.Join(c1, t1, buffer2.String())
		if err != nil {
			fmt.Println("failed to join remote node! Got %s", err)
		}	
	}
	fmt.Println(r1.Vnodes)	
/*
	// Create a second ring
	c2, t2, err := prepRing("192.168.0.8:10002")
	

	c3, t3, err := prepRing("192.168.0.8:10003")
	r3, err := chord.Join(c3, t3, c1.Hostname)
	if err != nil {
		fmt.Println("failed to join remote node! Got %s", err)
	}
*/
	//time.Sleep(10 * time.Second)
	sort.Sort(r1)
	
	i := 0
	for {
		fmt.Println("Enter Your Choices")
		fmt.Println("1: List Successors")
		fmt.Println("2: List Predecessors")
		fmt.Println("3: Exit")
		
		fmt.Scanf("%d", &i)
		if i ==1 {
			for i := 0; i < r1.Len(); i++ {
				fmt.Println(r1.Vnodes[i], r1.Vnodes[i].Successors[0])
			}
		}
		if i == 2 {
			for i := 0; i < r1.Len(); i++ {
				fmt.Println(r1.Vnodes[i], r1.Vnodes[i].Predecessor)
			}	
		}
		if i == 3 {
			break
		}
	}

	for i := 0; i < r1.Len(); i++ {
		r1.Vnodes[i].Leave()
	}

	r1.Shutdown()
/*	r2.Shutdown()
	r3.Shutdown()
*/
	t1.Shutdown()
/*	t2.Shutdown()
	t3.Shutdown()
*/
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