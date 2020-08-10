package commander

import (
	"math/rand"
	"testing"

	"github.com/bxcodec/faker/v3"
	"github.com/rafaeljusto/redigomock"

	"github.com/stretchr/testify/assert"
)

func TestNew(t *testing.T) {
	conn := redigomock.NewConn()
	defer func() {
		err := conn.Close()
		if err != nil {
			panic(err)
		}
	}()
	cmd := New(conn)
	assert.NotNil(t, cmd)
	assert.IsType(t, cmd, &Commander{})
}

func TestGet(t *testing.T) {
	key := "SomeKey"
	value := faker.Word()
	conn := redigomock.NewConn()
	conn.Command("GET", key).Expect(value)
	cmd := New(conn)
	var getResult string
	cmdErr := cmd.
		Get(&getResult, key).
		Commit()
	assert.Nil(t, cmdErr)
	assert.Equal(t, getResult, value)
}

func TestSelect(t *testing.T) {
	conn := redigomock.NewConn()
	conn.Command("SELECT").Expect("OK")
	cmd := New(conn)
	var selectResult string
	index := rand.Intn(10)
	cmdErr := cmd.
		Select(&selectResult, index).
		Commit()
	assert.Nil(t, cmdErr)
	assert.Equal(t, selectResult, "OK")
}

func TestExpire(t *testing.T) {
	key := "SomeKey"
	conn := redigomock.NewConn()
	conn.Command("EXPIRE", key, 1).Expect(int64(1))
	cmd := New(conn)
	var expireResult int
	cmdErr := cmd.Expire(&expireResult, key, 1).Commit()

	assert.Nil(t, cmdErr)
	assert.Equal(t, expireResult, 1)
}

func TestDel(t *testing.T) {
	key := "SomeKey"
	conn := redigomock.NewConn()
	conn.Command("DEL", key).Expect(int64(1))
	cmd := New(conn)
	var delResult int
	cmdErr := cmd.Del(&delResult, key).Commit()

	assert.Nil(t, cmdErr)
	assert.Equal(t, delResult, 1)
}

func TestIncr(t *testing.T) {
	key := "SomeKey"
	randIntList, _ := faker.RandomInt(10, 100)
	value := randIntList[0]
	conn := redigomock.NewConn()
	conn.Command("INCR", key).Expect(int64(value + 1))
	cmd := New(conn)
	var incrResult int
	cmdErr := cmd.Incr(&incrResult, key).Commit()

	assert.Nil(t, cmdErr)
	assert.Equal(t, incrResult, value+1)
}

func TestDecr(t *testing.T) {
	key := "SomeKey"
	randIntList, _ := faker.RandomInt(10, 100)
	value := randIntList[0]
	conn := redigomock.NewConn()
	conn.Command("DECR", key).Expect(int64(value - 1))
	cmd := New(conn)
	var incrResult int
	cmdErr := cmd.Decr(&incrResult, key).Commit()

	assert.Nil(t, cmdErr)
	assert.Equal(t, incrResult, value-1)
}

func TestPing(t *testing.T) {
	conn := redigomock.NewConn()
	conn.Command("PING", "PingMsg").Expect("PingMsg")
	cmd := New(conn)
	var pingResult string
	cmdErr := cmd.Ping(&pingResult, "PingMsg").Commit()
	assert.Nil(t, cmdErr)
	assert.Equal(t, pingResult, "PingMsg")
}

func TestKeys(t *testing.T) {
	key1 := "SomeKey1"
	key2 := "SomeKey2"
	conn := redigomock.NewConn()
	conn.Command("KEYS", "*Key*").ExpectStringSlice(key1, key2)
	cmd := New(conn)
	var keysResult []string
	cmdErr := cmd.Keys(&keysResult, "*Key*").Commit()

	assert.Nil(t, cmdErr)
	assert.Contains(t, keysResult, key1, key2)
}

func TestFlushAll(t *testing.T) {
	conn := redigomock.NewConn()
	conn.Command("FLUSHALL", "ASYNC").Expect("OK")
	cmd := New(conn)
	var flushResult string
	cmdErr := cmd.FlushAll(&flushResult, true).Commit()

	assert.Nil(t, cmdErr)
	assert.Equal(t, flushResult, "OK")
}

func TestSet(t *testing.T) {
	key := "SomeKey"
	value := faker.Word()
	conn := redigomock.NewConn()
	conn.Command("SET", key, value).Expect("OK")
	cmd := New(conn)
	var setResult string
	cmdErr := cmd.
		Set(&setResult, key, value, SetOption{}).
		Commit()

	assert.Nil(t, cmdErr)
	assert.Equal(t, setResult, "OK")
}

func TestSetWithEX(t *testing.T) {
	key := "SomeKey"
	value := faker.Word()
	conn := redigomock.NewConn()
	conn.Command("SET", key, value, "EX", 1).Expect("OK")
	cmd := New(conn)
	var setResult string
	cmdErr := cmd.
		Set(&setResult, key, value, SetOption{EX: 1}).
		Commit()

	assert.Nil(t, cmdErr)
	assert.Equal(t, setResult, "OK")
}

func TestSetWithPX(t *testing.T) {
	key := "SomeKey"
	value := faker.Word()
	conn := redigomock.NewConn()
	conn.Command("SET", key, value, "PX", 1000).Expect("OK")
	cmd := New(conn)
	var setResult string
	cmdErr := cmd.
		Set(&setResult, key, value, SetOption{PX: 1000}).
		Commit()

	assert.Nil(t, cmdErr)
	assert.Equal(t, setResult, "OK")
}

func TestSetWithNX(t *testing.T) {
	key := "SomeKey"
	value := faker.Word()
	conn := redigomock.NewConn()
	conn.Command("SET", key, value, "NX").Expect("OK")
	cmd := New(conn)
	var setResult string
	cmdErr := cmd.
		Set(&setResult, key, value, SetOption{NX: true}).
		Commit()

	assert.Nil(t, cmdErr)
	assert.Equal(t, setResult, "OK")
}

func TestSetWithXX(t *testing.T) {
	key := "SomeKey"
	value := faker.Word()
	conn := redigomock.NewConn()
	conn.Command("SET", key, value, "XX").Expect("OK")
	cmd := New(conn)
	var setResult string
	cmdErr := cmd.
		Set(&setResult, key, value, SetOption{XX: true}).
		Commit()

	assert.Nil(t, cmdErr)
	assert.Equal(t, setResult, "OK")
}

func TestSetWithKEEPTTL(t *testing.T) {
	key := "SomeKey"
	value := faker.Word()
	conn := redigomock.NewConn()
	conn.Command("SET", key, value, "KEEPTTL").Expect("OK")
	cmd := New(conn)
	var setResult string
	cmdErr := cmd.
		Set(&setResult, key, value, SetOption{KEEPTTL: true}).
		Commit()

	assert.Nil(t, cmdErr)
	assert.Equal(t, setResult, "OK")
}
