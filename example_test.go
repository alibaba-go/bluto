package bluto_test

import (
	"fmt"
	"log"

	"github.com/alibaba-go/bluto/bluto"
	"github.com/alibaba-go/bluto/commander"
	"github.com/gomodule/redigo/redis"
)

func Example_commandChain() {
	bluto, _ := bluto.New(bluto.Config{
		Address:               "localhost:6379",
		ConnectTimeoutSeconds: 10,
		ReadTimeoutSeconds:    10,
	})
	defer bluto.ClosePool()

	key1 := "SomeKey"
	key2 := "Other"
	var selectResult string
	var setResult1 string
	var setResult2 string
	var keysResult []string
	var delResult int
	var getResult1 int
	var getResult2 int
	var flushResult string

	errCmd := bluto.Borrow().
		Select(&selectResult, 0).
		Set(&setResult1, key1, 9, commander.SetOptionNX{}, commander.SetOptionEX{EX: 2}).
		Set(&setResult2, key2, 9).
		Keys(&keysResult, "*Key*").
		Del(&delResult, key1, "NotExistKey").
		Get(&getResult1, key1).
		FlushAll(&flushResult, commander.FlushAllOptionAsync{}).
		Get(&getResult2, key2).
		Commit()
	if errCmd != nil {
		log.Fatal(errCmd)
	}

	fmt.Println(keysResult)

	// Output: [SomeKey]
}

// Fields are the properties of each consumed message
type Fields struct {
	Key string `redis:"Key"`
}

// Message represents each consumed message
type Message struct {
	ID     string
	Fields *Fields
}

// RedisScan is the redis.Scanner interface implementation
func (m *Message) RedisScan(src interface{}) error {
	// each message has two parts: 1-id, 2-fields
	message, err := redis.Values(src, nil)
	if err != nil {
		return err
	}
	messageID, err := redis.String(message[0], nil)
	if err != nil {
		return err
	}
	m.ID = messageID
	msgFieldDetails, err := redis.Values(message[1], nil)
	if err != nil {
		return err
	}
	var msgField Fields
	err = redis.ScanStruct(msgFieldDetails, &msgField)
	if err != nil {
		return err
	}

	m.Fields = &msgField
	return nil
}

// Stream represents each stream
type Stream struct {
	Name     string
	Messages []*Message
}

// RedisScan is the redis.Scanner interface implementation
func (s *Stream) RedisScan(src interface{}) error {
	// each stream has two parts: 1-name, 2-messages
	stream, err := redis.Values(src, nil)
	if err != nil {
		return err
	}

	// set stream name
	name, err := redis.String(stream[0], nil)
	if err != nil {
		return err
	}
	s.Name = name

	// set stream messages
	messages, err := redis.Values(stream[1], nil)
	if err != nil {
		return err
	}
	// each message has two parts: 1-id, 2-fields
	for i := range messages {
		message := messages[i]
		msgDetails, err := redis.Values(message, nil)
		if err != nil {
			return err
		}

		// set message id
		msgID, err := redis.String(msgDetails[0], nil)
		if err != nil {
			return err
		}

		// set message field
		msgFieldDetails, err := redis.Values(msgDetails[1], nil)
		if err != nil {
			return err
		}
		var msgField Fields
		err = redis.ScanStruct(msgFieldDetails, &msgField)
		if err != nil {
			return err
		}

		// set messages
		s.Messages = append(s.Messages, &Message{
			ID:     msgID,
			Fields: &msgField,
		})
	}
	return nil
}

func Example_scanner() {
	bluto, _ := bluto.New(bluto.Config{
		Address:               "localhost:6379",
		ConnectTimeoutSeconds: 10,
		ReadTimeoutSeconds:    10,
	})
	defer bluto.ClosePool()

	groupName := "testGroup"
	consumerName := "testConsumer"
	key := "SomeKey"
	var flushResult string
	var xgroupCreateResult string
	var xaddResult string
	var xreadgroupResult []Stream

	err := bluto.Borrow().FlushAll(&flushResult).Commit()
	if err != nil {
		log.Panic(err)
	}
	err = bluto.Borrow().XGroupCreate(&xgroupCreateResult, "testStream", groupName, "0-0", commander.XGroupCreateOptionMKStream{}).Commit()
	if err != nil {
		log.Panic(err)
	}
	err = bluto.Borrow().XAdd(&xaddResult, "testStream", "*", &Fields{Key: key}).Commit()
	if err != nil {
		log.Panic(err)
	}
	err = bluto.Borrow().XReadGroup(&xreadgroupResult, groupName, consumerName, []string{"testStream"}, []string{">"}).Commit()
	if err != nil {
		log.Panic(err)
	}

	fmt.Println(xreadgroupResult[0].Messages[0].Fields.Key)

	// Output: SomeKey
}
