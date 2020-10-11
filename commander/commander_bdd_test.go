package commander_test

import (
	"os"
	"time"

	"github.com/alibaba-go/bluto/bluto"
	"github.com/bxcodec/faker/v3"
	"github.com/gomodule/redigo/redis"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	. "github.com/alibaba-go/bluto/commander"
)

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

// Consumer represents each consumer
type Consumer struct {
	ConsumerName string
	CountPending string
}

// MessageWithDetail represents each message with detail
type MessageWithDetail struct {
	MessageID       string
	ConsumerName    string
	IdleTimeMiliSec int
	DeliverdCount   int
}

// RedisScan is the redis.Scanner interface implementation
func (m *MessageWithDetail) RedisScan(src interface{}) error {
	// each pendigResult has four parts: 1-ConsumerName, 2-CountPending 3-IdleTimeMiliSec 4-DeliverdCount
	message, err := redis.Values(src, nil)
	if err != nil {
		return err
	}
	m.MessageID, err = redis.String(message[0], nil)
	if err != nil {
		return err
	}
	m.ConsumerName, err = redis.String(message[1], nil)
	if err != nil {
		return err
	}
	m.IdleTimeMiliSec, err = redis.Int(message[2], nil)
	if err != nil {
		return err
	}
	m.DeliverdCount, err = redis.Int(message[3], nil)
	if err != nil {
		return err
	}
	return nil
}

// PendingWithoutCount represent pending consumer's messages
type PendingWithoutCount struct {
	Count        int
	StartID      string
	EndID        string
	ConsumerList []Consumer
}

// RedisScan is the redis.Scanner interface implementation
func (p *PendingWithoutCount) RedisScan(src interface{}) error {
	// each pendigResult has four parts: 1-count, 2-start 3-end 4-consumerList
	pendingResult, err := redis.Values(src, nil)
	if err != nil {
		return err
	}
	p.Count, err = redis.Int(pendingResult[0], nil)
	if err != nil {
		return err
	}
	p.StartID, err = redis.String(pendingResult[1], nil)
	if err != nil {
		return err
	}
	p.EndID, err = redis.String(pendingResult[2], nil)
	if err != nil {
		return err
	}
	consumers, err := redis.Values(pendingResult[3], nil)
	if err != nil {
		return err
	}
	for _, consumer := range consumers {
		consumerInfo, err := redis.Values(consumer, nil)
		if err != nil {
			return err
		}
		consumerName, err := redis.String(consumerInfo[0], nil)
		if err != nil {
			return err
		}
		countPending, err := redis.String(consumerInfo[1], nil)
		if err != nil {
			return err
		}
		p.ConsumerList = append(p.ConsumerList,
			Consumer{
				ConsumerName: consumerName,
				CountPending: countPending,
			})
	}
	return nil
}

var _ = Describe("Commander", func() {

	// --------------------------------- global vars

	var pool *redis.Pool
	var getConn func() redis.Conn
	var getWrongConn func() redis.Conn

	// --------------------------------- global functions

	var getCorrectConfig = func() bluto.Config {
		address := os.Getenv("REDIS_ADDRESS")
		return bluto.Config{
			Address:               address,
			ConnectTimeoutSeconds: 10,
			ReadTimeoutSeconds:    10,
		}
	}

	var getWrongConfig = func() bluto.Config {
		return bluto.Config{
			Address:               "invalidAddress:1234",
			ConnectTimeoutSeconds: 10,
			ReadTimeoutSeconds:    10,
		}
	}

	// --------------------------------- before and after hooks

	BeforeSuite(func() {
		newPool, err := bluto.GetPool(getCorrectConfig())
		if err != nil {
			panic(err)
		}
		pool = newPool
		getConn = func() redis.Conn {
			return pool.Get()
		}
		wrongPool, _ := bluto.GetPool(getWrongConfig())
		getWrongConn = func() redis.Conn {
			return wrongPool.Get()
		}
	})

	AfterSuite(func() {
		err := pool.Close()
		if err != nil {
			panic(err)
		}
	})

	BeforeEach(func() {
		conn := getConn()
		commander := New(conn)
		var flushResult string
		var selectReslt string
		err := commander.
			Select(&selectReslt, 0).
			FlushAll(&flushResult).
			Commit()
		if err != nil {
			panic(err)
		}
		if selectReslt != "OK" || flushResult != "OK" {
			panic("BeforeEach failed")
		}
	})

	// --------------------------------- tests

	Describe("New method", func() {
		It("should return a new commander", func() {
			pool, err := bluto.GetPool(getCorrectConfig())
			defer func() {
				err := pool.Close()
				if err != nil {
					panic(err)
				}
			}()
			if err != nil {
				panic(err)
			}
			conn := pool.Get()
			defer func() {
				err := conn.Close()
				if err != nil {
					panic(err)
				}
			}()
			commander := New(conn)
			Expect(commander).To(Not(BeNil()))
			Expect(commander).To(BeAssignableToTypeOf(&Commander{}))
		})
	})

	Describe("Set", func() {
		It("should return the results of a valid SET", func() {
			key := "SomeKey"
			value := faker.Word()
			conn := getConn()
			commander := New(conn)
			var setResult string
			errCmd := commander.
				Set(&setResult, key, value).
				Commit()
			conn = getConn()
			var getResult string
			errSend := conn.Send("GET", key)
			results, errRsult := redis.Values(conn.Do(""))
			_, errScan := redis.Scan(results, &getResult)
			conn.Close()

			Expect(errSend).To(BeNil())
			Expect(errRsult).To(BeNil())
			Expect(errCmd).To(BeNil())
			Expect(errScan).To(BeNil())
			Expect(setResult).To(Equal("OK"))
			Expect(getResult).To(Equal(value))
		})

		It("should return the results of a expired SET with EX option", func() {
			key := "SomeKey"
			value := faker.Word()
			conn := getConn()
			commander := New(conn)
			var setResult string
			errCmd := commander.
				Set(&setResult, key, value, SetOptionEX{1}).
				Commit()
			time.Sleep(1100 * time.Millisecond)
			conn = getConn()
			var getResult string
			errSend := conn.Send("GET", key)
			results, errRsult := redis.Values(conn.Do(""))
			_, errScan := redis.Scan(results, &getResult)
			conn.Close()

			Expect(errSend).To(BeNil())
			Expect(errRsult).To(BeNil())
			Expect(errCmd).To(BeNil())
			Expect(errScan).To(BeNil())
			Expect(setResult).To(Equal("OK"))
			Expect(getResult).To(Equal(""))
		})

		It("should return the results of a expired SET with PX option", func() {
			key := "SomeKey"
			value := faker.Word()
			conn := getConn()
			commander := New(conn)
			var setResult string
			errCmd := commander.
				Set(&setResult, key, value, SetOptionPX{1000}).
				Commit()
			time.Sleep(1100 * time.Millisecond)
			conn = getConn()
			var getResult string
			errSend := conn.Send("GET", key)
			results, errRsult := redis.Values(conn.Do(""))
			_, errScan := redis.Scan(results, &getResult)
			conn.Close()

			Expect(errSend).To(BeNil())
			Expect(errRsult).To(BeNil())
			Expect(errCmd).To(BeNil())
			Expect(errScan).To(BeNil())
			Expect(setResult).To(Equal("OK"))
			Expect(getResult).To(Equal(""))
		})

		It("should return the results of a expired SET with NX option", func() {
			key := "SomeKey"
			value := faker.Word()
			newValue := 10
			conn := getConn()
			var setResult string
			errSend := conn.Send("SET", key, value)
			results, errResult := redis.Values(conn.Do(""))
			_, errScan := redis.Scan(results, &setResult)
			conn.Close()
			conn = getConn()
			commander := New(conn)
			var newSetResult string
			errCmd := commander.
				Set(&setResult, key, newValue, SetOptionNX{}).
				Commit()
			conn = getConn()
			var getResult string
			newErrSend := conn.Send("GET", key)
			results, newErrResult := redis.Values(conn.Do(""))
			_, newErrScan := redis.Scan(results, &getResult)
			conn.Close()

			Expect(errSend).To(BeNil())
			Expect(newErrSend).To(BeNil())
			Expect(errResult).To(BeNil())
			Expect(newErrResult).To(BeNil())
			Expect(errCmd).To(BeNil())
			Expect(errScan).To(BeNil())
			Expect(newErrScan).To(BeNil())
			Expect(setResult).To(Equal("OK"))
			Expect(newSetResult).To(Equal(""))
			Expect(getResult).To(Equal(value))
		})

		It("should return the results of a expired SET with XX option", func() {
			key := "SomeKey"
			value := faker.Word()
			conn := getConn()
			commander := New(conn)
			var setResult string
			errCmd := commander.
				Set(&setResult, key, value, SetOptionXX{}).
				Commit()

			conn = getConn()
			var getResult string
			errSend := conn.Send("GET", key)
			results, errResult := redis.Values(conn.Do(""))
			_, errScan := redis.Scan(results, &getResult)
			conn.Close()

			Expect(errSend).To(BeNil())
			Expect(errResult).To(BeNil())
			Expect(errCmd).To(BeNil())
			Expect(errScan).To(BeNil())
			Expect(setResult).To(Equal(""))
			Expect(getResult).To(Equal(""))
		})

	})

	Describe("Get", func() {
		It("should return the results of a valid GET", func() {
			key := "SomeKey"
			value := faker.Word()
			conn := getConn()
			var setResult string
			errSend := conn.Send("SET", key, value)
			results, errResult := redis.Values(conn.Do(""))
			_, errScan := redis.Scan(results, &setResult)
			conn.Close()
			conn = getConn()
			commander := New(conn)
			var getResult string
			errCmd := commander.
				Get(&getResult, key).
				Commit()
			Expect(errSend).To(BeNil())
			Expect(errScan).To(BeNil())
			Expect(errResult).To(BeNil())
			Expect(errCmd).To(BeNil())
			Expect(setResult).To(Equal("OK"))
			Expect(getResult).To(Equal(value))
		})
	})

	Describe("Select", func() {
		It("should return the results of a valid SELECT", func() {
			conn := getConn()
			commander := New(conn)
			var selectResult string
			errCmd := commander.
				Select(&selectResult, 3).
				Commit()
			Expect(errCmd).To(BeNil())
			Expect(selectResult).To(Equal("OK"))
		})
	})

	Describe("Expire", func() {
		It("should return the results of a valid EXPIRE", func() {
			key := "SomeKey"
			value := faker.Word()
			conn := getConn()
			var setResult string
			errSend := conn.Send("SET", key, value)
			results, errResult := redis.Values(conn.Do(""))
			_, errScan := redis.Scan(results, &setResult)
			conn.Close()
			conn = getConn()
			commander := New(conn)
			var expireResult bool
			errCmd := commander.Expire(&expireResult, key, 1).Commit()
			time.Sleep(1100 * time.Millisecond)
			conn = getConn()
			var getResult string
			errSendGet := conn.Send("GET", key)
			results, errResultGet := redis.Values(conn.Do(""))
			_, errScanGet := redis.Scan(results, &getResult)
			conn.Close()

			Expect(errSend).To(BeNil())
			Expect(errScan).To(BeNil())
			Expect(errResult).To(BeNil())
			Expect(setResult).To(Equal("OK"))
			Expect(errCmd).To(BeNil())
			Expect(expireResult).To(Equal(true))
			Expect(errSend).To(BeNil())
			Expect(errSendGet).To(BeNil())
			Expect(errResultGet).To(BeNil())
			Expect(errScanGet).To(BeNil())
			Expect(getResult).To(Equal(""))
		})
	})

	Describe("Del", func() {
		It("should return the results of a valid DEL", func() {
			key := "SomeKey"
			value := faker.Word()
			conn := getConn()
			var setResult string
			errSend := conn.Send("SET", key, value)
			results, errResult := redis.Values(conn.Do(""))
			_, errScan := redis.Scan(results, &setResult)
			conn.Close()
			conn = getConn()
			commander := New(conn)
			var delResult int
			errCmd := commander.Del(&delResult, key).Commit()
			Expect(errCmd).To(BeNil())
			Expect(delResult).To(Equal(1))
			conn = getConn()
			var getResult string
			errSendGet := conn.Send("GET", key)
			results, errResultGet := redis.Values(conn.Do(""))
			_, errScanGet := redis.Scan(results, &getResult)
			conn.Close()

			Expect(errSend).To(BeNil())
			Expect(errResult).To(BeNil())
			Expect(errScan).To(BeNil())
			Expect(errSendGet).To(BeNil())
			Expect(errResultGet).To(BeNil())
			Expect(errScanGet).To(BeNil())
			Expect(errCmd).To(BeNil())
			Expect(setResult).To(Equal("OK"))
			Expect(getResult).To(Equal(""))
			Expect(delResult).To(Equal(1))
		})
	})

	Describe("Incr", func() {
		It("should return the real results of a valid INCR", func() {
			key := "SomeKey"
			value := 9
			conn := getConn()
			var setResult string
			errSend := conn.Send("SET", key, value)
			results, errResult := redis.Values(conn.Do(""))
			_, errScan := redis.Scan(results, &setResult)
			conn.Close()
			conn = getConn()
			commander := New(conn)
			var incrResult int64
			errCmd := commander.Incr(&incrResult, key).Commit()
			conn = getConn()
			var getResult int
			errSendGet := conn.Send("GET", key)
			results, errResultGet := redis.Values(conn.Do(""))
			_, errScanGet := redis.Scan(results, &getResult)
			conn.Close()

			Expect(errSend).To(BeNil())
			Expect(errResult).To(BeNil())
			Expect(errScan).To(BeNil())
			Expect(errSendGet).To(BeNil())
			Expect(errResultGet).To(BeNil())
			Expect(errScanGet).To(BeNil())
			Expect(errCmd).To(BeNil())
			Expect(setResult).To(Equal("OK"))
			Expect(getResult).To(Equal(value + 1))
			Expect(incrResult).To(Equal(int64(value + 1)))
		})
	})

	Describe("Decr", func() {
		It("should return the real results of a valid DECR", func() {
			key := "SomeKey"
			value := 9
			conn := getConn()
			var setResult string
			errSend := conn.Send("SET", key, value)
			results, errResult := redis.Values(conn.Do(""))
			_, errScan := redis.Scan(results, &setResult)
			conn.Close()
			conn = getConn()
			commander := New(conn)
			var decrResult int64
			errCmd := commander.Decr(&decrResult, key).Commit()
			conn = getConn()
			var getResult int
			errSendGet := conn.Send("GET", key)
			results, errResultGet := redis.Values(conn.Do(""))
			_, errScanGet := redis.Scan(results, &getResult)
			conn.Close()

			Expect(errSend).To(BeNil())
			Expect(errResult).To(BeNil())
			Expect(errScan).To(BeNil())
			Expect(errSendGet).To(BeNil())
			Expect(errResultGet).To(BeNil())
			Expect(errScanGet).To(BeNil())
			Expect(errCmd).To(BeNil())
			Expect(setResult).To(Equal("OK"))
			Expect(getResult).To(Equal(value - 1))
			Expect(decrResult).To(Equal(int64(value - 1)))
		})
	})

	Describe("FlushAll", func() {
		It("should return the real results of a valid FLUSHALL", func() {
			key := "SomeKey"
			value := faker.Word()
			conn := getConn()
			var setResult string
			errSend := conn.Send("SET", key, value)
			results, errResult := redis.Values(conn.Do(""))
			_, errScan := redis.Scan(results, &setResult)
			conn.Close()
			conn = getConn()
			cmd := New(conn)
			var flushResult string
			errCmd := cmd.FlushAll(&flushResult, FlushAllOptionAsync{}).Commit()
			conn = getConn()
			var getResult string
			errSendGet := conn.Send("GET", key)
			results, errResultGet := redis.Values(conn.Do(""))
			_, errScanGet := redis.Scan(results, &getResult)
			conn.Close()

			Expect(errSend).To(BeNil())
			Expect(errResult).To(BeNil())
			Expect(errScan).To(BeNil())
			Expect(errSendGet).To(BeNil())
			Expect(errResultGet).To(BeNil())
			Expect(errScanGet).To(BeNil())
			Expect(errCmd).To(BeNil())
			Expect(setResult).To(Equal("OK"))
			Expect(getResult).To(Equal(""))
			Expect(flushResult).To(Equal("OK"))
		})
	})

	Describe("Keys", func() {
		It("should return the real results of a valid KEYS", func() {
			key1 := "SomeKey1"
			value1 := 9
			key2 := "SomeKey2"
			value2 := "SomeValue"
			conn := getConn()
			var setResult string
			errSend1 := conn.Send("SET", key1, value1)
			errSend2 := conn.Send("SET", key2, value2)
			results, errResult := redis.Values(conn.Do(""))
			_, errScan := redis.Scan(results, &setResult)
			conn.Close()
			conn = getConn()
			commander := New(conn)
			var keysResult []string
			errCmd := commander.Keys(&keysResult, "*Key*").Commit()

			Expect(errSend1).To(BeNil())
			Expect(errSend2).To(BeNil())
			Expect(errScan).To(BeNil())
			Expect(errResult).To(BeNil())
			Expect(errCmd).To(BeNil())
			Expect(setResult).To(Equal("OK"))
			Expect(keysResult).To(ContainElements("SomeKey1", "SomeKey2"))
		})
	})

	Describe("Ping", func() {
		It("should return the real results of a valid PING", func() {
			conn := getConn()
			commander := New(conn)
			var pingResult string
			errCmd := commander.Ping(&pingResult, PingOptionMessage{"PingMsg"}).Commit()
			Expect(errCmd).To(BeNil())
			Expect(pingResult).To(Equal("PingMsg"))
		})

	})

	Describe("XAdd", func() {
		It("should return the real results of a valid XAdd with MaxLen Aprroximate", func() {
			key := faker.Word()
			var xaddResult string
			var xreadResult []Stream
			conn := getConn()
			commander := New(conn)
			errCmd1 := commander.XAdd(&xaddResult, "testStream", "*", &Fields{Key: key}, XAddOptionMaxLen{MaxLen: 2, Approximate: true}).Commit()
			conn = getConn()
			commander = New(conn)
			errCmd2 := commander.XAdd(&xaddResult, "testStream", "*", &Fields{Key: key}, XAddOptionMaxLen{MaxLen: 2, Approximate: true}).Commit()
			conn = getConn()
			errSend := conn.Send("XREAD", "STREAMS", "testStream", "0-0")
			resutl, errResult := redis.Values(conn.Do(""))
			_, errScan := redis.Scan(resutl, &xreadResult)
			conn.Close()

			Expect(errCmd1).To(BeNil())
			Expect(errCmd2).To(BeNil())
			Expect(errSend).To(BeNil())
			Expect(errScan).To(BeNil())
			Expect(errResult).To(BeNil())
			Expect(xaddResult).To(Not(Equal("")))
			Expect(len(xreadResult[0].Messages)).To(Equal(2))
			Expect(xreadResult[0].Messages[0].Fields.Key).To(Equal(key))
			Expect(xreadResult[0].Messages[1].Fields.Key).To(Equal(key))
		})

		It("should return the real results of a valid XAdd with MaxLen", func() {
			key := faker.Word()
			var xaddResult string
			var xreadResult []Stream
			conn := getConn()
			commander := New(conn)
			errCmd1 := commander.XAdd(&xaddResult, "testStream", "*", &Fields{Key: key}, XAddOptionMaxLen{MaxLen: 1, Approximate: false}).Commit()
			conn = getConn()
			commander = New(conn)
			errCmd2 := commander.XAdd(&xaddResult, "testStream", "*", &Fields{Key: key}, XAddOptionMaxLen{MaxLen: 1, Approximate: false}).Commit()
			conn = getConn()
			errSend := conn.Send("XREAD", "STREAMS", "testStream", "0-0")
			resutl, errResult := redis.Values(conn.Do(""))
			_, errScan := redis.Scan(resutl, &xreadResult)
			conn.Close()

			Expect(errCmd1).To(BeNil())
			Expect(errCmd2).To(BeNil())
			Expect(errSend).To(BeNil())
			Expect(errScan).To(BeNil())
			Expect(errResult).To(BeNil())
			Expect(xaddResult).To(Not(Equal("")))
			Expect(len(xreadResult[0].Messages)).To(Equal(1))
			Expect(xreadResult[0].Messages[0].Fields.Key).To(Equal(key))
		})
	})

	Describe("XRead", func() {
		It("should return the real results of a valid XRead with COUNT", func() {
			key1 := faker.Word()
			key2 := faker.Word()
			var xaddResult1 string
			var xaddResult2 string
			var xreadResult []Stream
			conn := getConn()
			errSend1 := conn.Send("XADD", "testStream", "*", "Key", key1)
			result, errResult1 := redis.Values(conn.Do(""))
			_, errScan1 := redis.Scan(result, &xaddResult1)
			conn.Close()
			conn = getConn()
			errSend2 := conn.Send("XADD", "testStream", "*", "Key", key2)
			result, errResult2 := redis.Values(conn.Do(""))
			_, errScan2 := redis.Scan(result, &xaddResult2)
			conn.Close()
			conn = getConn()
			commander := New(conn)
			errCmd := commander.XRead(&xreadResult, []string{"testStream"}, []string{"0-0"}, XReadOptionCount{Count: 1}).Commit()

			Expect(errSend1).To(BeNil())
			Expect(errScan1).To(BeNil())
			Expect(errSend2).To(BeNil())
			Expect(errScan2).To(BeNil())
			Expect(errCmd).To(BeNil())
			Expect(errResult1).To(BeNil())
			Expect(errResult2).To(BeNil())
			Expect(xaddResult1).To(Not(Equal("")))
			Expect(xaddResult2).To(Not(Equal("")))
			Expect(len(xreadResult[0].Messages)).To(Equal(1))
			Expect(xreadResult[0].Messages[0].Fields.Key).To(Equal(key1))
		})

		It("should return the real results of a valid XRead with BLOCK", func() {
			key := faker.Word()
			var xaddResult string
			var xreadResult []Stream
			var errCmd error
			waitChan := make(chan bool)
			go func() {
				conn := getConn()
				commander := New(conn)
				errCmd = commander.XRead(&xreadResult, []string{"testStream"}, []string{"0-0"}, XReadOptionBlock{Block: 4000}).Commit()
				waitChan <- true
			}()
			conn := getConn()
			errSend := conn.Send("XADD", "testStream", "*", "Key", key)
			result, errResult := redis.Values(conn.Do(""))
			_, errScan := redis.Scan(result, &xaddResult)
			conn.Close()
			//wait for Xread command to end
			<-waitChan

			Expect(errSend).To(BeNil())
			Expect(errScan).To(BeNil())
			Expect(errCmd).To(BeNil())
			Expect(errResult).To(BeNil())
			Expect(len(xreadResult[0].Messages)).To(Equal(1))
			Expect(xreadResult[0].Messages[0].Fields.Key).To(Equal(key))
		})
	})

	Describe("XReadGroup", func() {
		It("should return the real results of a valid XReadGroup with COUNT", func() {
			groupName := "testGroup"
			consumerName := "testConsumer"
			key1 := faker.Word()
			key2 := faker.Word()
			var xaddResult1 string
			var xaddResult2 string
			var xgroupResult string
			var xreadGroupResult []Stream
			conn := getConn()
			errSendXAdd1 := conn.Send("XADD", "testStream", "*", "Key", key1)
			result, errResultXAdd1 := redis.Values(conn.Do(""))
			_, errScanXAdd1 := redis.Scan(result, &xaddResult1)
			conn.Close()
			conn = getConn()
			errSendXAdd2 := conn.Send("XADD", "testStream", "*", "Key", key2)
			result, errResultXAdd2 := redis.Values(conn.Do(""))
			_, errScanXAdd2 := redis.Scan(result, &xaddResult2)
			conn.Close()
			conn = getConn()
			errSend := conn.Send("XGROUP", "CREATE", "testStream", groupName, "0-0")
			result, errResult := redis.Values(conn.Do(""))
			_, errScan := redis.Scan(result, &xgroupResult)
			conn.Close()
			conn = getConn()
			commander := New(conn)
			errCmd := commander.XReadGroup(&xreadGroupResult, groupName, consumerName, []string{"testStream"}, []string{">"}, XReadGroupOptionCount{Count: 1}).Commit()

			Expect(errSend).To(BeNil())
			Expect(errScan).To(BeNil())
			Expect(errCmd).To(BeNil())
			Expect(errResult).To(BeNil())
			Expect(errSendXAdd1).To(BeNil())
			Expect(errScanXAdd1).To(BeNil())
			Expect(errResultXAdd1).To(BeNil())
			Expect(errSendXAdd2).To(BeNil())
			Expect(errScanXAdd2).To(BeNil())
			Expect(errResultXAdd2).To(BeNil())
			Expect(xaddResult1).To(Not(Equal("")))
			Expect(xaddResult2).To(Not(Equal("")))
			Expect(xgroupResult).To(Equal("OK"))
			Expect(len(xreadGroupResult[0].Messages)).To(Equal(1))
			Expect(xreadGroupResult[0].Messages[0].Fields.Key).To(Equal(key1))
		})

		It("should return the real results of a valid XReadGroup with NOACK", func() {
			groupName := "testGroup"
			consumerName := "testConsumer"
			key1 := faker.Word()
			var xaddResult1 string
			var xgroupResult string
			var xreadGroupResult []Stream
			var xreadGroupResult2 []Stream
			conn := getConn()
			errSendXAdd1 := conn.Send("XADD", "testStream", "*", "Key", key1)
			result, errResultXAdd1 := redis.Values(conn.Do(""))
			_, errScanXAdd1 := redis.Scan(result, &xaddResult1)
			conn.Close()
			conn = getConn()
			errSend := conn.Send("XGROUP", "CREATE", "testStream", groupName, "0-0")
			result, errResult := redis.Values(conn.Do(""))
			_, errScan := redis.Scan(result, &xgroupResult)
			conn.Close()
			conn = getConn()
			commander := New(conn)
			errCmd := commander.XReadGroup(&xreadGroupResult, groupName, consumerName, []string{"testStream"}, []string{">"}, XReadGroupOptionNoAck{}).Commit()
			conn = getConn()
			commander = New(conn)
			errCmd2 := commander.XReadGroup(&xreadGroupResult2, groupName, consumerName, []string{"testStream"}, []string{"0-0"}).Commit()

			Expect(errSend).To(BeNil())
			Expect(errScan).To(BeNil())
			Expect(errCmd).To(BeNil())
			Expect(errCmd2).To(BeNil())
			Expect(errResult).To(BeNil())
			Expect(errSendXAdd1).To(BeNil())
			Expect(errScanXAdd1).To(BeNil())
			Expect(errResultXAdd1).To(BeNil())
			Expect(xaddResult1).To(Not(Equal("")))
			Expect(xgroupResult).To(Equal("OK"))
			Expect(len(xreadGroupResult[0].Messages)).To(Equal(1))
			Expect(xreadGroupResult[0].Messages[0].Fields.Key).To(Equal(key1))
			Expect(xreadGroupResult2[0].Messages).To(BeNil())
		})

		It("should return the real results of a valid XREADGRROUP with BLOCK", func() {
			groupName := "testGroup"
			consumerName := "testConsumer"
			key := faker.Word()
			var xaddResult string
			var xreadGroupResult []Stream
			var xgroupResult string
			var errCmd error
			waitChan := make(chan bool)
			conn := getConn()
			errSend := conn.Send("XGROUP", "CREATE", "testStream", groupName, "0-0", "MKSTREAM")
			result, errResult := redis.Values(conn.Do(""))
			_, errScan := redis.Scan(result, &xgroupResult)
			conn.Close()
			go func() {
				conn := getConn()
				commander := New(conn)
				errCmd = commander.XReadGroup(&xreadGroupResult, groupName, consumerName, []string{"testStream"}, []string{">"}, XReadGroupOptionBlock{Block: 4000}).Commit()
				waitChan <- true
			}()
			conn = getConn()
			errSendXAdd := conn.Send("XADD", "testStream", "*", "Key", key)
			result, errResultXAdd := redis.Values(conn.Do(""))
			_, errScanXAdd := redis.Scan(result, &xaddResult)
			conn.Close()
			//wait for XREADGRROUP command to end
			<-waitChan

			Expect(errCmd).To(BeNil())
			Expect(errSend).To(BeNil())
			Expect(errScan).To(BeNil())
			Expect(errSendXAdd).To(BeNil())
			Expect(errScanXAdd).To(BeNil())
			Expect(errResultXAdd).To(BeNil())
			Expect(errResult).To(BeNil())
			Expect(len(xreadGroupResult[0].Messages)).To(Equal(1))
			Expect(xreadGroupResult[0].Messages[0].Fields.Key).To(Equal(key))
		})
	})

	Describe("XGROUP", func() {
		It("should return the real results of a valid XGROUP CREATE", func() {
			groupName := "testGroup"
			conn := getConn()
			commander := New(conn)
			var xgroupResult string
			errCmd := commander.XGroupCreate(&xgroupResult, "testStream", groupName, "0-0", XGroupCreateOptionMKStream{}).Commit()

			Expect(errCmd).To(BeNil())
			Expect(xgroupResult).To(Equal("OK"))
		})

		It("should return the real results of a valid XGROUP DESTROY", func() {
			groupName := "testGroup"
			var xgroupCreateResult string
			var xgroupDestroyResult int
			conn := getConn()
			errSend := conn.Send("XGROUP", "CREATE", "testStream", groupName, "0-0", "MKSTREAM")
			result, errResult := redis.Values(conn.Do(""))
			_, errScan := redis.Scan(result, &xgroupCreateResult)
			conn.Close()
			conn = getConn()
			commander := New(conn)
			errCmd := commander.XGroupDestroy(&xgroupDestroyResult, "testStream", groupName).Commit()

			Expect(errSend).To(BeNil())
			Expect(errScan).To(BeNil())
			Expect(errCmd).To(BeNil())
			Expect(errResult).To(BeNil())
			Expect(xgroupCreateResult).To(Equal("OK"))
			Expect(xgroupDestroyResult).To(Equal(1))
		})

		It("should return the real results of a valid XGROUP DELCONSUMER", func() {
			groupName := "testGroup"
			consumerName := "testConsumer"
			key := faker.Word()
			var xgroupCreateResult string
			var xaddResult string
			var xreadgroupResult []Stream
			var xgroupDelConsumerResult int
			conn := getConn()
			errSend := conn.Send("XGROUP", "CREATE", "testStream", groupName, "0-0", "MKSTREAM")
			result, errResult := redis.Values(conn.Do(""))
			_, errScan := redis.Scan(result, &xgroupCreateResult)
			conn.Close()
			conn = getConn()
			errSendXAdd := conn.Send("XADD", "testStream", "*", "Key", key)
			result, errResultXAdd := redis.Values(conn.Do(""))
			_, errScanXAdd := redis.Scan(result, &xaddResult)
			conn.Close()
			conn = getConn()
			errSendXReadGroup := conn.Send("XREADGROUP", "GROUP", groupName, consumerName, "STREAMS", "testStream", ">")
			result, errResultXReadGroup := redis.Values(conn.Do(""))
			_, errScanXReadGroup := redis.Scan(result, &xreadgroupResult)
			conn.Close()
			conn = getConn()
			commander := New(conn)
			errCmd := commander.XGroupDelConsumer(&xgroupDelConsumerResult, "testStream", groupName, consumerName).Commit()

			Expect(errSend).To(BeNil())
			Expect(errScan).To(BeNil())
			Expect(errResult).To(BeNil())
			Expect(errResultXReadGroup).To(BeNil())
			Expect(errSendXReadGroup).To(BeNil())
			Expect(errScanXReadGroup).To(BeNil())
			Expect(errResultXAdd).To(BeNil())
			Expect(errSendXAdd).To(BeNil())
			Expect(errScanXAdd).To(BeNil())
			Expect(errCmd).To(BeNil())
			Expect(xgroupCreateResult).To(Equal("OK"))
			Expect(xreadgroupResult[0].Messages[0].Fields.Key).To(Equal(key))
			Expect(xgroupDelConsumerResult).To(Equal(1))
		})
	})

	Describe("XACK", func() {
		It("should return the real results of a valid XACK", func() {
			groupName := "testGroup"
			consumerName := "testConsumer"
			key := faker.Word()
			var xgroupCreateResult string
			var xaddResult string
			var xreadgroupResult []Stream
			var xreadgroupResult2 []Stream
			var xackResult int
			conn := getConn()
			errSend := conn.Send("XGROUP", "CREATE", "testStream", groupName, "0-0", "MKSTREAM")
			result, errResult := redis.Values(conn.Do(""))
			_, errScan := redis.Scan(result, &xgroupCreateResult)
			conn.Close()
			conn = getConn()
			errSendXAdd := conn.Send("XADD", "testStream", "*", "Key", key)
			result, errResultXAdd := redis.Values(conn.Do(""))
			_, errScanXAdd := redis.Scan(result, &xaddResult)
			conn.Close()
			conn = getConn()
			errSendXReadGroup := conn.Send("XREADGROUP", "GROUP", groupName, consumerName, "STREAMS", "testStream", ">")
			result, errResultXReadGroup := redis.Values(conn.Do(""))
			_, errScanXReadGroup := redis.Scan(result, &xreadgroupResult)
			conn.Close()
			conn = getConn()
			commander := New(conn)
			errCmd := commander.XAck(&xackResult, "testStream", groupName, []string{xaddResult}).Commit()
			conn = getConn()
			errSendXReadGroup2 := conn.Send("XREADGROUP", "GROUP", groupName, consumerName, "STREAMS", "testStream", "0-0")
			result, errResultXReadGroup2 := redis.Values(conn.Do(""))
			_, errScanXReadGroup2 := redis.Scan(result, &xreadgroupResult2)
			conn.Close()

			Expect(errSend).To(BeNil())
			Expect(errScan).To(BeNil())
			Expect(errResult).To(BeNil())
			Expect(errResultXReadGroup).To(BeNil())
			Expect(errSendXReadGroup).To(BeNil())
			Expect(errScanXReadGroup).To(BeNil())
			Expect(errResultXReadGroup2).To(BeNil())
			Expect(errSendXReadGroup2).To(BeNil())
			Expect(errScanXReadGroup2).To(BeNil())
			Expect(errResultXAdd).To(BeNil())
			Expect(errSendXAdd).To(BeNil())
			Expect(errScanXAdd).To(BeNil())
			Expect(errCmd).To(BeNil())
			Expect(xgroupCreateResult).To(Equal("OK"))
			Expect(xreadgroupResult[0].Messages[0].Fields.Key).To(Equal(key))
			Expect(xreadgroupResult2[0].Messages).To(BeNil())
			Expect(xackResult).To(Equal(1))
		})
	})

	Describe("XPENDING", func() {
		It("should return the real results of a valid XPENDING", func() {
			groupName := "testGroup"
			consumerName := "testConsumer"
			key := faker.Word()
			var xgroupCreateResult string
			var xaddResult string
			var xreadgroupResult []Stream
			var xpendingResult PendingWithoutCount
			conn := getConn()
			errSend := conn.Send("XGROUP", "CREATE", "testStream", groupName, "0-0", "MKSTREAM")
			result, errResult := redis.Values(conn.Do(""))
			_, errScan := redis.Scan(result, &xgroupCreateResult)
			conn.Close()
			conn = getConn()
			errSendXAdd := conn.Send("XADD", "testStream", "*", "Key", key)
			result, errResultXAdd := redis.Values(conn.Do(""))
			_, errScanXAdd := redis.Scan(result, &xaddResult)
			conn.Close()
			conn = getConn()
			errSendXReadGroup := conn.Send("XREADGROUP", "GROUP", groupName, consumerName, "STREAMS", "testStream", ">")
			result, errResultXReadGroup := redis.Values(conn.Do(""))
			_, errScanXReadGroup := redis.Scan(result, &xreadgroupResult)
			conn.Close()
			conn = getConn()
			commander := New(conn)
			errCmd := commander.XPending(&xpendingResult, "testStream", groupName).Commit()

			Expect(errSend).To(BeNil())
			Expect(errScan).To(BeNil())
			Expect(errResult).To(BeNil())
			Expect(errResultXReadGroup).To(BeNil())
			Expect(errSendXReadGroup).To(BeNil())
			Expect(errScanXReadGroup).To(BeNil())
			Expect(errResultXAdd).To(BeNil())
			Expect(errSendXAdd).To(BeNil())
			Expect(errScanXAdd).To(BeNil())
			Expect(errCmd).To(BeNil())
			Expect(xgroupCreateResult).To(Equal("OK"))
			Expect(xaddResult).To(Not(Equal("")))
			Expect(xreadgroupResult[0].Messages[0].Fields.Key).To(Equal(key))
			Expect(xpendingResult.Count).To(Equal(1))
			Expect(xpendingResult.ConsumerList[0]).To(Equal(Consumer{ConsumerName: consumerName, CountPending: "1"}))
		})

		It("should return the real results of a valid XPENDING with StartID EndID Count Consumer", func() {
			groupName := "testGroup"
			consumerName := "testConsumer"
			key := faker.Word()
			var xgroupCreateResult string
			var xaddResult string
			var xreadgroupResult []Stream
			var xpendingResult []MessageWithDetail
			conn := getConn()
			errSend := conn.Send("XGROUP", "CREATE", "testStream", groupName, "0-0", "MKSTREAM")
			result, errResult := redis.Values(conn.Do(""))
			_, errScan := redis.Scan(result, &xgroupCreateResult)
			conn.Close()
			conn = getConn()
			errSendXAdd := conn.Send("XADD", "testStream", "*", "Key", key)
			result, errResultXAdd := redis.Values(conn.Do(""))
			_, errScanXAdd := redis.Scan(result, &xaddResult)
			conn.Close()
			conn = getConn()
			errSendXReadGroup := conn.Send("XREADGROUP", "GROUP", groupName, consumerName, "STREAMS", "testStream", ">")
			result, errResultXReadGroup := redis.Values(conn.Do(""))
			_, errScanXReadGroup := redis.Scan(result, &xreadgroupResult)
			conn.Close()
			conn = getConn()
			commander := New(conn)
			errCmd := commander.XPending(&xpendingResult, "testStream", groupName, XPendingOptionStartEndCount{StartID: "-", EndID: "+", Count: 1}, XPendingOptionConsumer{Consumer: consumerName}).Commit()

			Expect(errSend).To(BeNil())
			Expect(errScan).To(BeNil())
			Expect(errResult).To(BeNil())
			Expect(errResultXReadGroup).To(BeNil())
			Expect(errSendXReadGroup).To(BeNil())
			Expect(errScanXReadGroup).To(BeNil())
			Expect(errResultXAdd).To(BeNil())
			Expect(errSendXAdd).To(BeNil())
			Expect(errScanXAdd).To(BeNil())
			Expect(errCmd).To(BeNil())
			Expect(xgroupCreateResult).To(Equal("OK"))
			Expect(xaddResult).To(Not(Equal("")))
			Expect(xreadgroupResult[0].Messages[0].Fields.Key).To(Equal(key))
			Expect(len(xpendingResult)).To(Equal(1))
			Expect(xpendingResult[0].MessageID).To(Equal(xaddResult))
		})
	})

	Describe("XCLAIM", func() {
		It("should return the real results of a valid XCLAIM", func() {
			groupName := "testGroup"
			consumerName1 := "testConsumer1"
			consumerName2 := "testConsumer2"
			key1 := faker.Word()
			key2 := faker.Word()
			var xgroupCreateResult string
			var xaddResult string
			var xaddResult2 string
			var xreadgroupResult []Stream
			var xreadgroupResult2 []Stream
			var xclaimResult []Message
			conn := getConn()
			errSend := conn.Send("XGROUP", "CREATE", "testStream", groupName, "0-0", "MKSTREAM")
			result, errResult := redis.Values(conn.Do(""))
			_, errScan := redis.Scan(result, &xgroupCreateResult)
			conn.Close()
			conn = getConn()
			errSendXAdd := conn.Send("XADD", "testStream", "*", "Key", key1)
			result, errResultXAdd := redis.Values(conn.Do(""))
			_, errScanXAdd := redis.Scan(result, &xaddResult)
			conn.Close()
			conn = getConn()
			errSendXReadGroup := conn.Send("XREADGROUP", "GROUP", groupName, consumerName1, "STREAMS", "testStream", ">")
			result, errResultXReadGroup := redis.Values(conn.Do(""))
			_, errScanXReadGroup := redis.Scan(result, &xreadgroupResult)
			conn.Close()
			conn = getConn()
			errSendXAdd2 := conn.Send("XADD", "testStream", "*", "Key", key2)
			result, errResultXAdd2 := redis.Values(conn.Do(""))
			_, errScanXAdd2 := redis.Scan(result, &xaddResult2)
			conn.Close()
			conn = getConn()
			errSendXReadGroup2 := conn.Send("XREADGROUP", "GROUP", groupName, consumerName2, "STREAMS", "testStream", ">")
			result, errResultXReadGroup2 := redis.Values(conn.Do(""))
			_, errScanXReadGroup2 := redis.Scan(result, &xreadgroupResult2)
			conn.Close()
			// xclaim needs minimum idle time for message before start moving it to another consumer
			time.Sleep(150 * time.Millisecond)
			conn = getConn()
			commander := New(conn)
			errCmd := commander.XClaim(&xclaimResult, "testStream", groupName, consumerName2, 100, []string{xaddResult}).Commit()

			Expect(errSend).To(BeNil())
			Expect(errScan).To(BeNil())
			Expect(errResult).To(BeNil())
			Expect(errResultXReadGroup).To(BeNil())
			Expect(errSendXReadGroup).To(BeNil())
			Expect(errScanXReadGroup).To(BeNil())
			Expect(errResultXReadGroup2).To(BeNil())
			Expect(errSendXReadGroup2).To(BeNil())
			Expect(errScanXReadGroup2).To(BeNil())
			Expect(errResultXAdd).To(BeNil())
			Expect(errSendXAdd).To(BeNil())
			Expect(errScanXAdd).To(BeNil())
			Expect(errResultXAdd2).To(BeNil())
			Expect(errSendXAdd2).To(BeNil())
			Expect(errScanXAdd2).To(BeNil())
			Expect(errCmd).To(BeNil())
			Expect(xaddResult).To(Not(Equal("")))
			Expect(xaddResult2).To(Not(Equal("")))
			Expect(xgroupCreateResult).To(Equal("OK"))
			Expect(xreadgroupResult[0].Messages[0].Fields.Key).To(Equal(key1))
			Expect(xreadgroupResult2[0].Messages[0].Fields.Key).To(Equal(key2))
			Expect(xclaimResult[0].ID).To(Equal(xaddResult))
			Expect(xclaimResult[0].Fields.Key).To(Equal(key1))
		})

		It("should return the real results of a valid XCLAIM with JUSTID", func() {
			groupName := "testGroup"
			consumerName1 := "testConsumer1"
			consumerName2 := "testConsumer2"
			key1 := faker.Word()
			key2 := faker.Word()
			var xgroupCreateResult string
			var xaddResult string
			var xaddResult2 string
			var xreadgroupResult []Stream
			var xreadgroupResult2 []Stream
			var xclaimResult []string
			conn := getConn()
			errSend := conn.Send("XGROUP", "CREATE", "testStream", groupName, "0-0", "MKSTREAM")
			result, errResult := redis.Values(conn.Do(""))
			_, errScan := redis.Scan(result, &xgroupCreateResult)
			conn.Close()
			conn = getConn()
			errSendXAdd := conn.Send("XADD", "testStream", "*", "Key", key1)
			result, errResultXAdd := redis.Values(conn.Do(""))
			_, errScanXAdd := redis.Scan(result, &xaddResult)
			conn.Close()
			conn = getConn()
			errSendXReadGroup := conn.Send("XREADGROUP", "GROUP", groupName, consumerName1, "STREAMS", "testStream", ">")
			result, errResultXReadGroup := redis.Values(conn.Do(""))
			_, errScanXReadGroup := redis.Scan(result, &xreadgroupResult)
			conn.Close()
			conn = getConn()
			errSendXAdd2 := conn.Send("XADD", "testStream", "*", "Key", key2)
			result, errResultXAdd2 := redis.Values(conn.Do(""))
			_, errScanXAdd2 := redis.Scan(result, &xaddResult2)
			conn.Close()
			conn = getConn()
			errSendXReadGroup2 := conn.Send("XREADGROUP", "GROUP", groupName, consumerName2, "STREAMS", "testStream", ">")
			result, errResultXReadGroup2 := redis.Values(conn.Do(""))
			_, errScanXReadGroup2 := redis.Scan(result, &xreadgroupResult2)
			conn.Close()
			// xclaim needs minimum idle time for message before start moving it to another consumer
			time.Sleep(150 * time.Millisecond)
			conn = getConn()
			commander := New(conn)
			errCmd := commander.XClaim(&xclaimResult, "testStream", groupName, consumerName2, 100, []string{xaddResult}, XClaimOptionJustID{}).Commit()

			Expect(errSend).To(BeNil())
			Expect(errScan).To(BeNil())
			Expect(errResult).To(BeNil())
			Expect(errResultXReadGroup).To(BeNil())
			Expect(errSendXReadGroup).To(BeNil())
			Expect(errScanXReadGroup).To(BeNil())
			Expect(errResultXReadGroup2).To(BeNil())
			Expect(errSendXReadGroup2).To(BeNil())
			Expect(errScanXReadGroup2).To(BeNil())
			Expect(errResultXAdd).To(BeNil())
			Expect(errSendXAdd).To(BeNil())
			Expect(errScanXAdd).To(BeNil())
			Expect(errResultXAdd2).To(BeNil())
			Expect(errSendXAdd2).To(BeNil())
			Expect(errScanXAdd2).To(BeNil())
			Expect(errCmd).To(BeNil())
			Expect(xaddResult).To(Not(Equal("")))
			Expect(xaddResult2).To(Not(Equal("")))
			Expect(xgroupCreateResult).To(Equal("OK"))
			Expect(xreadgroupResult[0].Messages[0].Fields.Key).To(Equal(key1))
			Expect(xreadgroupResult2[0].Messages[0].Fields.Key).To(Equal(key2))
			Expect(xclaimResult[0]).To(Equal(xaddResult))
		})

		It("should return the real results of a valid XCLAIM with FORCE", func() {
			groupName := "testGroup"
			consumerName1 := "testConsumer1"
			key1 := faker.Word()
			key2 := faker.Word()
			var xgroupCreateResult string
			var xaddResult string
			var xaddResult2 string
			var xreadgroupResult []Stream
			var xclaimResult []Message
			conn := getConn()
			errSend := conn.Send("XGROUP", "CREATE", "testStream", groupName, "0-0", "MKSTREAM")
			result, errResult := redis.Values(conn.Do(""))
			_, errScan := redis.Scan(result, &xgroupCreateResult)
			conn.Close()
			conn = getConn()
			errSendXAdd := conn.Send("XADD", "testStream", "*", "Key", key1)
			result, errResultXAdd := redis.Values(conn.Do(""))
			_, errScanXAdd := redis.Scan(result, &xaddResult)
			conn.Close()
			conn = getConn()
			errSendXReadGroup := conn.Send("XREADGROUP", "GROUP", groupName, consumerName1, "STREAMS", "testStream", ">")
			result, errResultXReadGroup := redis.Values(conn.Do(""))
			_, errScanXReadGroup := redis.Scan(result, &xreadgroupResult)
			conn.Close()
			conn = getConn()
			errSendXAdd2 := conn.Send("XADD", "testStream", "*", "Key", key2)
			result, errResultXAdd2 := redis.Values(conn.Do(""))
			_, errScanXAdd2 := redis.Scan(result, &xaddResult2)
			conn.Close()
			// xclaim needs minimum idle time for message before start moving it to another consumer
			time.Sleep(150 * time.Millisecond)
			conn = getConn()
			commander := New(conn)
			errCmd := commander.XClaim(&xclaimResult, "testStream", groupName, consumerName1, 100, []string{xaddResult2}, XClaimOptionForce{}).Commit()

			Expect(errSend).To(BeNil())
			Expect(errScan).To(BeNil())
			Expect(errResult).To(BeNil())
			Expect(errResultXReadGroup).To(BeNil())
			Expect(errSendXReadGroup).To(BeNil())
			Expect(errScanXReadGroup).To(BeNil())
			Expect(errResultXAdd).To(BeNil())
			Expect(errSendXAdd).To(BeNil())
			Expect(errScanXAdd).To(BeNil())
			Expect(errResultXAdd2).To(BeNil())
			Expect(errSendXAdd2).To(BeNil())
			Expect(errScanXAdd2).To(BeNil())
			Expect(errCmd).To(BeNil())
			Expect(xaddResult).To(Not(Equal("")))
			Expect(xaddResult2).To(Not(Equal("")))
			Expect(xgroupCreateResult).To(Equal("OK"))
			Expect(xreadgroupResult[0].Messages[0].Fields.Key).To(Equal(key1))
			Expect(xclaimResult[0].ID).To(Equal(xaddResult2))
			Expect(xclaimResult[0].Fields.Key).To(Equal(key2))
		})

	})

	Describe("EXISTS", func() {
		It("should return the real results of a valid EXISTS", func() {
			key1 := "SomeKey1"
			value1 := faker.Word()
			key2 := "SomeKey2"
			value2 := faker.Word()
			conn := getConn()
			var setResult1 string
			var setResult2 string
			var existsResult int
			errSend := conn.Send("SET", key1, value1)
			results, errResult := redis.Values(conn.Do(""))
			_, errScan := redis.Scan(results, &setResult1)
			conn = getConn()
			errSend2 := conn.Send("SET", key2, value2)
			results, errResult2 := redis.Values(conn.Do(""))
			_, errScan2 := redis.Scan(results, &setResult2)
			conn = getConn()
			commander := New(conn)
			errCmd := commander.Exists(&existsResult, "SomeKey1", "SomeKey2", "SomeKey3").Commit()

			Expect(errSend).To(BeNil())
			Expect(errScan).To(BeNil())
			Expect(errResult).To(BeNil())
			Expect(errSend2).To(BeNil())
			Expect(errScan2).To(BeNil())
			Expect(errResult2).To(BeNil())
			Expect(errCmd).To(BeNil())
			Expect(setResult1).To(Equal("OK"))
			Expect(setResult2).To(Equal("OK"))
			Expect(existsResult).To(Equal(2))
		})

		It("should return the real results of a not-existing-key EXISTS", func() {
			key := "SomeKey"
			value := faker.Word()
			conn := getConn()
			var setResult string
			var existsResult int
			errSend := conn.Send("SET", key, value)
			results, errResult := redis.Values(conn.Do(""))
			_, errScan := redis.Scan(results, &setResult)
			conn = getConn()
			commander := New(conn)
			errCmd := commander.Exists(&existsResult, "NotExistingKey").Commit()

			Expect(errSend).To(BeNil())
			Expect(errScan).To(BeNil())
			Expect(errResult).To(BeNil())
			Expect(errCmd).To(BeNil())
			Expect(setResult).To(Equal("OK"))
			Expect(existsResult).To(Equal(0))
		})
	})

	Describe("HSET", func() {
		It("should return the real results of a valid HSET", func() {
			key := "SomeKey"
			strValue := faker.Word()
			intValue := faker.RandomUnixTime()
			var values []interface{}
			values = append(values, strValue)
			values = append(values, intValue)
			conn := getConn()
			var hSetResult int
			var hGetResultInt int64
			var hGetResultStr string
			cmd := New(conn)
			errCmd := cmd.HSet(&hSetResult, key, []string{"strValue", "intValue"}, values).Commit()
			conn = getConn()
			errSend1 := conn.Send("HGET", key, "strValue")
			results, errResult1 := redis.Values(conn.Do(""))
			_, errScan1 := redis.Scan(results, &hGetResultStr)
			conn = getConn()
			errSend2 := conn.Send("HGET", key, "intValue")
			results, errResult2 := redis.Values(conn.Do(""))
			_, errScan2 := redis.Scan(results, &hGetResultInt)

			Expect(errSend1).To(BeNil())
			Expect(errScan1).To(BeNil())
			Expect(errResult1).To(BeNil())
			Expect(errSend2).To(BeNil())
			Expect(errScan2).To(BeNil())
			Expect(errResult2).To(BeNil())
			Expect(errCmd).To(BeNil())
			Expect(hSetResult).To(Equal(2))
			Expect(hGetResultInt).To(Equal(intValue))
			Expect(hGetResultStr).To(Equal(strValue))
		})
	})

	Describe("HGET", func() {
		It("should return the real results of a valid HSET", func() {
			key := "SomeKey"
			value := faker.Word()
			conn := getConn()
			var hGetResult string
			var hSetResult int
			errSend := conn.Send("HSET", key, "field", value)
			results, errResult := redis.Values(conn.Do(""))
			_, errScan := redis.Scan(results, &hSetResult)
			conn = getConn()
			cmd := New(conn)
			errCmd := cmd.HGet(&hGetResult, key,"field").Commit()

			Expect(errSend).To(BeNil())
			Expect(errScan).To(BeNil())
			Expect(errResult).To(BeNil())
			Expect(errCmd).To(BeNil())
			Expect(hSetResult).To(Equal(1))
			Expect(hGetResult).To(Equal(value))
		})
	})


	Describe("Integration test command and commit", func() {
		It("should return the error of resuing closed connection", func() {
			pool, errpool := bluto.GetPool(getCorrectConfig())
			var pingResult1 string
			var pingResult2 string
			conn := pool.Get()
			commander := New(conn)
			errCmd1 := commander.Ping(&pingResult1).Commit()
			commander = New(conn)
			errCmd2 := commander.Ping(&pingResult2).Commit()

			Expect(errpool).To(BeNil())
			Expect(pingResult1).To(Equal("PONG"))
			Expect(pingResult2).To(Equal(""))
			Expect(errCmd1).To(BeNil())
			Expect(errCmd2).To(Not(BeNil()))
		})

		It("should return the error of a invalid config", func() {
			pool, errpool := bluto.GetPool(getWrongConfig())
			var commandResult interface{}
			var pingResult string
			conn := pool.Get()
			commander := New(conn)
			errCmd := commander.Command(&commandResult, "NotExistCommand").Ping(&pingResult).Commit()

			Expect(errpool).To(BeNil())
			Expect(commandResult).To(BeNil())
			Expect(pingResult).To(Equal(""))
			Expect(errCmd).To(Not(BeNil()))
		})

		It("should return the results of a valid chain of expire commands", func() {
			conn := getConn()
			commander := New(conn)
			key := "SomeKey"
			var selectResult string
			var setResult string
			var expireResult1 bool
			var expireResult2 bool
			var getResult1 int
			var getResult2 int

			errCmd := commander.
				Select(&selectResult, 0).
				Set(&setResult, key, 9).
				Expire(&expireResult1, key, 1).
				Expire(&expireResult2, "NotExistKey", 1).
				Get(&getResult1, key).
				Commit()
			Expect(errCmd).To(BeNil())
			Expect(setResult).To(Equal("OK"))
			Expect(expireResult1).To(Equal(true))
			Expect(expireResult2).To(Equal(false))
			Expect(getResult1).To(Equal(9))
			//wait to expire key
			time.Sleep(1100 * time.Millisecond)
			conn = getConn()
			commander = New(conn)
			errCmd = commander.
				Select(&selectResult, 0).
				Get(&getResult2, key).
				Commit()

			Expect(errCmd).To(BeNil())
			Expect(getResult2).To(Equal(0))
		})

		It("should return the results of a valid chain of del and flush commands", func() {
			conn := getConn()
			cmd := New(conn)
			key1 := "SomeKey1"
			key2 := "SomeKey2"
			var selectResult string
			var setResult1 string
			var setResult2 string
			var keysResult []string
			var delResult int
			var getResult1 int
			var getResult2 int
			var flushResult string

			errCmd := cmd.
				Select(&selectResult, 0).
				Set(&setResult1, key1, 9).
				Set(&setResult2, key2, 9).
				Keys(&keysResult, "*Key*").
				Del(&delResult, key1, "NotExistKey").
				Get(&getResult1, key1).
				FlushAll(&flushResult, FlushAllOptionAsync{}).
				Get(&getResult2, key2).
				Commit()

			Expect(errCmd).To(BeNil())
			Expect(setResult1).To(Equal("OK"))
			Expect(setResult2).To(Equal("OK"))
			Expect(keysResult).To(ContainElements("SomeKey1", "SomeKey2"))
			Expect(delResult).To(Equal(1))
			Expect(getResult1).To(Equal(0))
			Expect(flushResult).To(Equal("OK"))
			Expect(getResult2).To(Equal(0))
		})

		It("should return the results of a valid chain of commands", func() {
			conn := getConn()
			commander := New(conn)
			key := "someKey"
			pingMsg := "PingMessage"
			var selectResult string
			var setResult string
			var incrResult int64
			var getResult int
			var decrResult int64
			var pingResult string

			errCmd := commander.
				Select(&selectResult, 0).
				Set(&setResult, key, 9, SetOptionEX{EX: 2}, SetOptionNX{}).
				Incr(&incrResult, key).
				Get(&getResult, key).
				Decr(&decrResult, key).
				Ping(&pingResult, PingOptionMessage{pingMsg}).
				Commit()

			Expect(errCmd).To(BeNil())
			Expect(selectResult).To(Equal("OK"))
			Expect(setResult).To(Equal("OK"))
			Expect(incrResult).To(Equal(int64(10)))
			Expect(getResult).To(Equal(10))
			Expect(decrResult).To(Equal(int64(9)))
			Expect(pingResult).To(Equal(pingMsg))
		})

		It("should return the errors of an invalid chain of commands", func() {
			conn := getConn()
			commander := New(conn)
			key := "someKey"
			var selectResult string
			var setResult string
			var nonExistentResult interface{}
			var incrResult int64
			var getResult int

			errCmd := commander.
				Select(&selectResult, 0).
				Set(&setResult, key, 9).
				Command(&nonExistentResult, "SOMENONEXISTENTCOMMAND", key, 9).
				Incr(&incrResult, key).
				Get(&getResult, key).
				Commit()

			Expect(errCmd).To(Not(BeNil()))
			Expect(selectResult).To(Equal("OK"))
			Expect(setResult).To(Equal("OK"))
			Expect(nonExistentResult).To(BeNil())
			Expect(incrResult).To(Equal(int64(0)))
			Expect(getResult).To(Equal(0))
		})

		It("should return the errors of an invalid redis config", func() {
			conn := getWrongConn()
			commander := New(conn)
			var selectResult string
			cmdErr := commander.
				Select(&selectResult, 0).
				Commit()

			Expect(cmdErr).To(Not(BeNil()))
			Expect(selectResult).To(Equal(""))
		})

		It("should return the errors of an invalid result type", func() {
			conn := getConn()
			commander := New(conn)
			var pingResult int
			cmdErr := commander.
				Command(&pingResult, "PING").
				Commit()

			Expect(cmdErr).To(Not(BeNil()))
			Expect(pingResult).To(Equal(0))
		})

	})
})
