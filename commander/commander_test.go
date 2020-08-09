package commander

import (
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/bxcodec/faker/v3"

	"github.com/gomodule/redigo/redis"

	"github.com/stretchr/testify/suite"
)

func getPool() *redis.Pool {
	address := os.Getenv("REDIS_ADDRESS")
	pool := &redis.Pool{
		Dial: func() (redis.Conn, error) {
			return redis.Dial(
				"tcp",
				address,
				redis.DialPassword(""),
				redis.DialConnectTimeout(5*time.Second),
				redis.DialReadTimeout(5*time.Second),
				redis.DialWriteTimeout(5*time.Second),
			)
		},
		MaxIdle:         10,
		MaxActive:       10,
		IdleTimeout:     60 * time.Second,
		Wait:            true,
		MaxConnLifetime: 120 * time.Second,
	}
	return pool
}

// CommanderSuite test Commander methods
type CommanderSuite struct {
	suite.Suite
	pool *redis.Pool
}

// TestCommanderSuite run suite tests
func TestCommanderSuite(t *testing.T) {
	suite.Run(t, new(CommanderSuite))
}

// SetupAllTest run before all tests and setup redis pool
func (suite *CommanderSuite) SetupSuite() {
	suite.pool = getPool()
}

// TearDownSuite run after all tests and close redis pool
func (suite *CommanderSuite) TearDownSuite() {
	err := suite.pool.Close()
	suite.NoError(err)
}

// TearDownTest run after each tests and Clear redis
func (suite *CommanderSuite) TearDownTest() {
	conn := suite.pool.Get()
	cmd := New(conn)
	var flushResult string
	err := cmd.
		FlushAll(&flushResult, false).
		Commit()
	suite.Nil(err)
	suite.Equal(flushResult, "OK")
}

func (suite *CommanderSuite) TestNew() {
	conn := suite.pool.Get()
	defer func() {
		err := conn.Close()
		if err != nil {
			panic(err)
		}
	}()
	cmd := New(conn)
	suite.NotNil(cmd)
	suite.IsType(cmd, &Commander{})
}

func (suite *CommanderSuite) TestGet() {
	key := "SomeKey"
	value := faker.Word()
	conn := suite.pool.Get()
	var setResult string
	errSend := conn.Send("SET", key, value)
	results, errResult := redis.Values(conn.Do(""))
	conn.Close()
	_, errScan := redis.Scan(results, &setResult)
	conn = suite.pool.Get()
	cmd := New(conn)
	var getResult string
	cmdErr := cmd.
		Get(&getResult, key).
		Commit()
	suite.Nil(errSend)
	suite.Nil(errScan)
	suite.Nil(errResult)
	suite.Nil(cmdErr)
	suite.Equal(setResult, "OK")
	suite.Equal(getResult, value)
}

func (suite *CommanderSuite) TestSelect() {
	conn := suite.pool.Get()
	cmd := New(conn)
	var selectResult string
	index := rand.Intn(10)
	cmdErr := cmd.
		Select(&selectResult, index).
		Commit()
	suite.Nil(cmdErr)
	suite.Equal(selectResult, "OK")
}

func (suite *CommanderSuite) TestExpire() {
	key := "SomeKey"
	value := faker.Word()
	conn := suite.pool.Get()
	var setResult string
	errSend := conn.Send("SET", key, value)
	results, errResult := redis.Values(conn.Do(""))
	_, errScan := redis.Scan(results, &setResult)
	conn.Close()
	conn = suite.pool.Get()
	cmd := New(conn)
	var expireResult int
	cmdErr := cmd.Expire(&expireResult, key, 1).Commit()
	time.Sleep(1100 * time.Millisecond)
	conn = suite.pool.Get()
	var getResult string
	errSendGet := conn.Send("GET", key)
	results, errResultGet := redis.Values(conn.Do(""))
	_, errScanGet := redis.Scan(results, &getResult)
	conn.Close()

	suite.Nil(errSend)
	suite.Nil(errScan)
	suite.Nil(errResult)
	suite.Nil(cmdErr)
	suite.Nil(errSendGet)
	suite.Nil(errScanGet)
	suite.Nil(errResultGet)
	suite.Equal(setResult, "OK")
	suite.Equal(expireResult, 1)
	suite.Equal(getResult, "")
}

func (suite *CommanderSuite) TestDel() {
	key := "SomeKey"
	value := faker.Word()
	conn := suite.pool.Get()
	var setResult string
	errSend := conn.Send("SET", key, value)
	results, errResult := redis.Values(conn.Do(""))
	_, errScan := redis.Scan(results, &setResult)
	conn.Close()
	conn = suite.pool.Get()
	cmd := New(conn)
	var delResult int
	cmdErr := cmd.Del(&delResult, key).Commit()
	conn = suite.pool.Get()
	var getResult string
	errSendGet := conn.Send("GET", key)
	results, errResultGet := redis.Values(conn.Do(""))
	_, errScanGet := redis.Scan(results, &getResult)
	conn.Close()

	suite.Nil(errSend)
	suite.Nil(errScan)
	suite.Nil(errResult)
	suite.Nil(cmdErr)
	suite.Nil(errSendGet)
	suite.Nil(errScanGet)
	suite.Nil(errResultGet)
	suite.Equal(setResult, "OK")
	suite.Equal(delResult, 1)
	suite.Equal(getResult, "")
}

func (suite *CommanderSuite) TestIncr() {
	key := "SomeKey"
	randIntList, _ := faker.RandomInt(10, 100)
	value := randIntList[0]
	conn := suite.pool.Get()
	var setResult string
	errSend := conn.Send("SET", key, value)
	results, errResult := redis.Values(conn.Do(""))
	_, errScan := redis.Scan(results, &setResult)
	conn.Close()
	conn = suite.pool.Get()
	cmd := New(conn)
	var incrResult int
	cmdErr := cmd.Incr(&incrResult, key).Commit()
	conn = suite.pool.Get()
	var getResult int
	errSendGet := conn.Send("GET", key)
	results, errResultGet := redis.Values(conn.Do(""))
	_, errScanGet := redis.Scan(results, &getResult)
	conn.Close()

	suite.Nil(errSend)
	suite.Nil(errScan)
	suite.Nil(errResult)
	suite.Nil(cmdErr)
	suite.Nil(errSendGet)
	suite.Nil(errScanGet)
	suite.Nil(errResultGet)
	suite.Equal(setResult, "OK")
	suite.Equal(incrResult, value+1)
	suite.Equal(getResult, value+1)
}

func (suite *CommanderSuite) TestDecr() {
	key := "SomeKey"
	randIntList, _ := faker.RandomInt(10, 100)
	value := randIntList[0]
	conn := suite.pool.Get()
	var setResult string
	errSend := conn.Send("SET", key, value)
	results, errResult := redis.Values(conn.Do(""))
	_, errScan := redis.Scan(results, &setResult)
	conn.Close()
	conn = suite.pool.Get()
	cmd := New(conn)
	var incrResult int
	cmdErr := cmd.Decr(&incrResult, key).Commit()
	conn = suite.pool.Get()
	var getResult int
	errSendGet := conn.Send("GET", key)
	results, errResultGet := redis.Values(conn.Do(""))
	_, errScanGet := redis.Scan(results, &getResult)
	conn.Close()

	suite.Nil(errSend)
	suite.Nil(errScan)
	suite.Nil(errResult)
	suite.Nil(cmdErr)
	suite.Nil(errSendGet)
	suite.Nil(errScanGet)
	suite.Nil(errResultGet)
	suite.Equal(setResult, "OK")
	suite.Equal(incrResult, value-1)
	suite.Equal(getResult, value-1)
}

func (suite *CommanderSuite) TestPing() {
	conn := suite.pool.Get()
	cmd := New(conn)
	var pingResult string
	cmdErr := cmd.Ping(&pingResult, "PingMsg").Commit()
	suite.Nil(cmdErr)
	suite.Equal(pingResult, "PingMsg")
}

func (suite *CommanderSuite) TestKeys() {
	key1 := "SomeKey1"
	value1 := faker.Word()
	key2 := "SomeKey2"
	value2 := faker.Word()
	conn := suite.pool.Get()
	var setResult string
	errSend1 := conn.Send("SET", key1, value1)
	errSend2 := conn.Send("SET", key2, value2)
	results, errResult := redis.Values(conn.Do(""))
	_, errScan := redis.Scan(results, &setResult)
	conn.Close()
	conn = suite.pool.Get()
	cmd := New(conn)
	var keysResult []string
	cmdErr := cmd.Keys(&keysResult, "*Key*").Commit()
	suite.Nil(errSend1)
	suite.Nil(errSend2)
	suite.Nil(errScan)
	suite.Nil(errResult)
	suite.Nil(cmdErr)
	suite.Equal(setResult, "OK")
	suite.Contains(keysResult, key1, key2)
}

func (suite *CommanderSuite) TestFlushAll() {
	key := "SomeKey"
	value := faker.Word()
	conn := suite.pool.Get()
	var setResult string
	errSend := conn.Send("SET", key, value)
	results, errResult := redis.Values(conn.Do(""))
	_, errScan := redis.Scan(results, &setResult)
	conn.Close()
	conn = suite.pool.Get()
	cmd := New(conn)
	var flushResult string
	cmdErr := cmd.FlushAll(&flushResult, true).Commit()
	conn = suite.pool.Get()
	var getResult string
	errSendGet := conn.Send("GET", key)
	results, errResultGet := redis.Values(conn.Do(""))
	_, errScanGet := redis.Scan(results, &getResult)
	conn.Close()

	suite.Nil(errSend)
	suite.Nil(errScan)
	suite.Nil(errResult)
	suite.Nil(cmdErr)
	suite.Nil(errSendGet)
	suite.Nil(errScanGet)
	suite.Nil(errResultGet)
	suite.Equal(setResult, "OK")
	suite.Equal(flushResult, "OK")
	suite.Equal(getResult, "")
}

func (suite *CommanderSuite) TestSet() {
	key := "SomeKey"
	value := faker.Word()
	conn := suite.pool.Get()
	cmd := New(conn)
	var setResult string
	cmdErr := cmd.
		Set(&setResult, key, value, SetOption{}).
		Commit()
	conn = suite.pool.Get()
	var getResult string
	errSend := conn.Send("GET", key)
	results, errResult := redis.Values(conn.Do(""))
	_, errScan := redis.Scan(results, &getResult)
	conn.Close()

	suite.Nil(errSend)
	suite.Nil(errScan)
	suite.Nil(errResult)
	suite.Nil(cmdErr)
	suite.Equal(setResult, "OK")
	suite.Equal(getResult, value)
}

func (suite *CommanderSuite) TestSetWithEX() {
	key := "SomeKey"
	value := faker.Word()
	conn := suite.pool.Get()
	cmd := New(conn)
	var setResult string
	cmdErr := cmd.
		Set(&setResult, key, value, SetOption{EX: 1}).
		Commit()
	time.Sleep(1100 * time.Millisecond)
	conn = suite.pool.Get()
	var getResult string
	errSend := conn.Send("GET", key)
	results, errResult := redis.Values(conn.Do(""))
	_, errScan := redis.Scan(results, &getResult)
	conn.Close()

	suite.Nil(errSend)
	suite.Nil(errScan)
	suite.Nil(errResult)
	suite.Nil(cmdErr)
	suite.Equal(setResult, "OK")
	suite.Equal(getResult, "")
}

func (suite *CommanderSuite) TestSetWithPX() {
	key := "SomeKey"
	value := faker.Word()
	conn := suite.pool.Get()
	cmd := New(conn)
	var setResult string
	cmdErr := cmd.
		Set(&setResult, key, value, SetOption{PX: 1000}).
		Commit()
	time.Sleep(1100 * time.Millisecond)
	conn = suite.pool.Get()
	defer conn.Close()
	var getResult string
	errSend := conn.Send("GET", key)
	results, errResult := redis.Values(conn.Do(""))
	_, errScan := redis.Scan(results, &getResult)

	suite.Nil(errSend)
	suite.Nil(errScan)
	suite.Nil(errResult)
	suite.Nil(cmdErr)
	suite.Equal(setResult, "OK")
	suite.Equal(getResult, "")
}

func (suite *CommanderSuite) TestSetWithNX() {
	key := "SomeKey"
	value := faker.Word()
	newValue := faker.Word()
	conn := suite.pool.Get()
	var setResult string
	errSend := conn.Send("SET", key, value)
	results, errResult := redis.Values(conn.Do(""))
	_, errScan := redis.Scan(results, &setResult)
	conn.Close()
	conn = suite.pool.Get()
	cmd := New(conn)
	var setResultWithNX string
	cmdErr := cmd.
		Set(&setResultWithNX, key, newValue, SetOption{NX: true}).
		Commit()
	conn = suite.pool.Get()
	var getResult string
	errSendGet := conn.Send("GET", key)
	results, errResultGet := redis.Values(conn.Do(""))
	_, errScanGet := redis.Scan(results, &getResult)
	conn.Close()

	suite.Nil(errSend)
	suite.Nil(errScan)
	suite.Nil(errResult)
	suite.Nil(cmdErr)
	suite.Nil(errSendGet)
	suite.Nil(errScanGet)
	suite.Nil(errResultGet)
	suite.Equal(setResult, "OK")
	suite.Equal(setResultWithNX, "")
	suite.Equal(getResult, value)
}

func (suite *CommanderSuite) TestSetWithXX() {
	key := "SomeKey"
	value := faker.Word()
	conn := suite.pool.Get()
	cmd := New(conn)
	var setResult string
	cmdErr := cmd.
		Set(&setResult, key, value, SetOption{XX: true}).
		Commit()
	conn = suite.pool.Get()
	var getResult string
	errSend := conn.Send("GET", key)
	results, errResult := redis.Values(conn.Do(""))
	_, errScan := redis.Scan(results, &getResult)
	conn.Close()

	suite.Nil(errSend)
	suite.Nil(errScan)
	suite.Nil(errResult)
	suite.Nil(cmdErr)
	suite.Equal(setResult, "")
	suite.Equal(getResult, "")
}
