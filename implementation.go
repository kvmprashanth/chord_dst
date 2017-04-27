package main

import(
	"os"
	"fmt"
	"bytes"
	"go-chord/chord"
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
		StabilizeMax:  100 * time.Millisecond,
	}
	timeout := time.Duration(200 * time.Millisecond)
	trans, err := chord.InitTCPTransport(listen, timeout)
	if err != nil {
		return nil, nil, err
	}
	return conf, trans, nil
}

func main(){
	
	fmt.Println("Chord DST")
	
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
	fmt.Println("Nodes created with ids (starting with):", r1.Vnodes)	

	sort.Sort(r1)

	//r1.Vnodes[0].Map["test"] = "testing"
	
	for {
		i := 0

		fmt.Println("\n\nMenu")
		fmt.Println("\t1: List Successors")
		fmt.Println("\t2: List Predecessors")
		fmt.Println("\t3: PUT key-value into store")
		fmt.Println("\t4: GET key-value from store")
		fmt.Println("\t5: Display key-value pairs in current node")
		fmt.Println("\t0: Exit")

		fmt.Println("Enter your choice")
		fmt.Scanf("%d", &i)

		if i ==1 {
			for i := 0; i < r1.Len(); i++ {
				fmt.Printf("\n%s\t%s @%s", r1.Vnodes[i], r1.Vnodes[i].Successors[0], r1.Vnodes[i].Successors[0].Host)
			}
		} else if i == 2 {
			for i := 0; i < r1.Len(); i++ {
				fmt.Printf("\n%s\t%s @%s", r1.Vnodes[i], r1.Vnodes[i].Predecessor, r1.Vnodes[i].Predecessor.Host)
			}	
		} else if i == 3 {
			
			key := ""
			value := ""

			fmt.Println("Enter Key: ")
			fmt.Scanf("%s", &key)
			fmt.Println("Enter Value: ")
			fmt.Scanf("%s", &value)

			r1.Vnodes[1].Successors[0].Map[key] = value

		} else if i == 5 {
			
			//vn := r1.Vnodes[0].Successors[0]
			//fmt.Println(vn.Last_finger)
			
			fmt.Println("Key-Values at VNode-1")
			//fmt.Println(r1.Vnodes[0].GetKey())
			for key, value := range r1.Vnodes[0].Map {
 	  			fmt.Println("\t", key, "-", value)
 	  		}

 	  		fmt.Println("Key-Values at VNode-2")
 	  		//fmt.Println(r1.Vnodes[1].GetKey())
			for key, value := range r1.Vnodes[1].Map {
 	  			fmt.Println("\t", key, "-", value)
 	  		}
 	  			
		} else if i == 0 {
			
			break
		}
	}

	for i := 0; i < r1.Len(); i++ {
		r1.Vnodes[i].Leave()
	}

	r1.Shutdown()
	t1.Shutdown()

}

