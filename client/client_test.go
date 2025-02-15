package client

import (
	"context"
	"fmt"
	"log"
	"sync"
	"testing"
)

func TestServerWithMultiClients(t *testing.T) {
	
	nClients := 10
	wg:= sync.WaitGroup{}
	wg.Add(nClients)
	for i := 0; i < nClients; i++ {
		go func(it int){

			c, err := New("localhost:8976")
			if err != nil {
				log.Fatal(err)
			}
			defer c.Close()
			key := fmt.Sprintf("client_foo_%d", i)
			value := fmt.Sprintf("client_bar_%d", i)

			if err := c.Set(context.TODO(), key, value); err != nil {
				log.Fatal(err)
			}
			val, err := c.Get(context.TODO(), key)
			if err != nil {
				log.Fatal(err)
			}
			fmt.Printf("client %d got this val back =>%s\n",it,val)
			wg.Done()
		}(i)
	}
	wg.Wait()
}

// func TestNewClient(t *testing.T) {
// 	c, err := New("localhost:8976")
// 	if err != nil {
// 		log.Fatal(err)
// 	}
// 	time.Sleep(time.Second)
// 	for i := 0; i < 10; i++ {
// 		fmt.Println("SET =>", fmt.Sprintf("bar_%d", i))
// 		if err := c.Set(context.TODO(), fmt.Sprintf("foo_%d", i), fmt.Sprintf("bar_%d", i)); err != nil {
// 			log.Fatal(err)
// 		}
// 		val, err := c.Get(context.TODO(), fmt.Sprintf("foo_%d", i))
// 		if err != nil {
// 			log.Fatal(err)
// 		}
// 		fmt.Println("GET =>", val)
// 	}
// }
