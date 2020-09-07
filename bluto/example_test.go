package bluto_test

import (
	"fmt"
	"log"

	"github.com/alibaba-go/bluto/bluto"
)

func ExampleNew() {
	bluto, _ := bluto.New(bluto.Config{
		Address:               "localhost:6379",
		ConnectTimeoutSeconds: 10,
		ReadTimeoutSeconds:    10,
	})
	defer bluto.ClosePool()
}

func ExampleNew_advancedConfig() {
	bluto, err := bluto.New(bluto.Config{
		// ---------------------------------------- dial options
		Network:               "tcp",
		Address:               "localhost:6379",
		ConnectTimeoutSeconds: 5,
		ReadTimeoutSeconds:    5,
		WriteTimeoutSeconds:   5,
		KeepAliveSeconds:      300,
		// ---------------------------------------- pool options
		MaxIdle:                10,
		MaxActive:              10,
		IdleTimeoutSeconds:     60,
		MaxConnLifetimeSeconds: 120,
	})
	defer bluto.ClosePool()
	if err != nil {
		log.Fatal(err)
	}
}

func ExampleBluto_Borrow() {
	bluto, err := bluto.New(bluto.Config{
		Address:               "localhost:6379",
		ConnectTimeoutSeconds: 10,
		ReadTimeoutSeconds:    10,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer bluto.ClosePool()
	var pingResult string
	err = bluto.Borrow().Ping(&pingResult).Commit()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(pingResult)

	// Output: PONG
}
