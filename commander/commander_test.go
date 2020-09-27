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
	errCmd := cmd.
		Get(&getResult, key).
		Commit()
	assert.Nil(t, errCmd)
	assert.Equal(t, getResult, value)
}

func TestSelect(t *testing.T) {
	conn := redigomock.NewConn()
	conn.Command("SELECT").Expect("OK")
	cmd := New(conn)
	var selectResult string
	index := rand.Intn(10)
	errCmd := cmd.
		Select(&selectResult, index).
		Commit()
	assert.Nil(t, errCmd)
	assert.Equal(t, selectResult, "OK")
}

func TestExpire(t *testing.T) {
	key := "SomeKey"
	conn := redigomock.NewConn()
	conn.Command("EXPIRE", key, 1).Expect(int64(1))
	cmd := New(conn)
	var expireResult bool
	errCmd := cmd.Expire(&expireResult, key, 1).Commit()

	assert.Nil(t, errCmd)
	assert.Equal(t, expireResult, true)
}

func TestDel(t *testing.T) {
	key := "SomeKey"
	conn := redigomock.NewConn()
	conn.Command("DEL", key).Expect(int64(1))
	cmd := New(conn)
	var delResult int
	errCmd := cmd.Del(&delResult, key).Commit()

	assert.Nil(t, errCmd)
	assert.Equal(t, delResult, 1)
}

func TestIncr(t *testing.T) {
	key := "SomeKey"
	randIntList, _ := faker.RandomInt(10, 100)
	value := randIntList[0]
	conn := redigomock.NewConn()
	conn.Command("INCR", key).Expect(int64(value + 1))
	cmd := New(conn)
	var incrResult int64
	errCmd := cmd.Incr(&incrResult, key).Commit()

	assert.Nil(t, errCmd)
	assert.Equal(t, incrResult, int64(value+1))
}

func TestDecr(t *testing.T) {
	key := "SomeKey"
	randIntList, _ := faker.RandomInt(10, 100)
	value := randIntList[0]
	conn := redigomock.NewConn()
	conn.Command("DECR", key).Expect(int64(value - 1))
	cmd := New(conn)
	var incrResult int64
	errCmd := cmd.Decr(&incrResult, key).Commit()

	assert.Nil(t, errCmd)
	assert.Equal(t, incrResult, int64(value-1))
}

func TestPing(t *testing.T) {
	conn := redigomock.NewConn()
	conn.Command("PING", "PingMsg").Expect("PingMsg")
	cmd := New(conn)
	var pingResult string
	errCmd := cmd.Ping(&pingResult, PingOptionMessage{Message: "PingMsg"}).Commit()
	assert.Nil(t, errCmd)
	assert.Equal(t, pingResult, "PingMsg")
}

func TestKeys(t *testing.T) {
	key1 := "SomeKey1"
	key2 := "SomeKey2"
	conn := redigomock.NewConn()
	conn.Command("KEYS", "*Key*").ExpectStringSlice(key1, key2)
	cmd := New(conn)
	var keysResult []string
	errCmd := cmd.Keys(&keysResult, "*Key*").Commit()

	assert.Nil(t, errCmd)
	assert.Contains(t, keysResult, key1, key2)
}

func TestFlushAll(t *testing.T) {
	conn := redigomock.NewConn()
	conn.Command("FLUSHALL", "ASYNC").Expect("OK")
	cmd := New(conn)
	var flushResult string
	errCmd := cmd.FlushAll(&flushResult, FlushAllOptionAsync{}).Commit()

	assert.Nil(t, errCmd)
	assert.Equal(t, flushResult, "OK")
}

func TestSet(t *testing.T) {
	key := "SomeKey"
	value := faker.Word()
	conn := redigomock.NewConn()
	conn.Command("SET", key, value).Expect("OK")
	cmd := New(conn)
	var setResult string
	errCmd := cmd.
		Set(&setResult, key, value).
		Commit()

	assert.Nil(t, errCmd)
	assert.Equal(t, setResult, "OK")
}

func TestSetWithEX(t *testing.T) {
	key := "SomeKey"
	value := faker.Word()
	conn := redigomock.NewConn()
	conn.Command("SET", key, value, "EX", uint64(1)).Expect("OK")
	cmd := New(conn)
	var setResult string
	errCmd := cmd.
		Set(&setResult, key, value, SetOptionEX{1}).
		Commit()

	assert.Nil(t, errCmd)
	assert.Equal(t, setResult, "OK")
}

func TestSetWithPX(t *testing.T) {
	key := "SomeKey"
	value := faker.Word()
	conn := redigomock.NewConn()
	conn.Command("SET", key, value, "PX", uint64(1000)).Expect("OK")
	cmd := New(conn)
	var setResult string
	errCmd := cmd.
		Set(&setResult, key, value, SetOptionPX{1000}).
		Commit()

	assert.Nil(t, errCmd)
	assert.Equal(t, setResult, "OK")
}

func TestSetWithNX(t *testing.T) {
	key := "SomeKey"
	value := faker.Word()
	conn := redigomock.NewConn()
	conn.Command("SET", key, value, "NX").Expect("OK")
	cmd := New(conn)
	var setResult string
	errCmd := cmd.
		Set(&setResult, key, value, SetOptionNX{}).
		Commit()

	assert.Nil(t, errCmd)
	assert.Equal(t, setResult, "OK")
}

func TestSetWithXX(t *testing.T) {
	key := "SomeKey"
	value := faker.Word()
	conn := redigomock.NewConn()
	conn.Command("SET", key, value, "XX").Expect("OK")
	cmd := New(conn)
	var setResult string
	errCmd := cmd.
		Set(&setResult, key, value, SetOptionXX{}).
		Commit()

	assert.Nil(t, errCmd)
	assert.Equal(t, setResult, "OK")
}

func TestSetWithKEEPTTL(t *testing.T) {
	key := "SomeKey"
	value := faker.Word()
	conn := redigomock.NewConn()
	conn.Command("SET", key, value, "KEEPTTL").Expect("OK")
	cmd := New(conn)
	var setResult string
	errCmd := cmd.
		Set(&setResult, key, value, SetOptionKeepTTL{}).
		Commit()

	assert.Nil(t, errCmd)
	assert.Equal(t, setResult, "OK")
}

func TestXadd(t *testing.T) {
	key := "SomeKey"
	value := faker.Word()
	streamNmae := "testStream"
	conn := redigomock.NewConn()
	conn.Command("XADD", streamNmae, "*", key, value).Expect("OK")
	cmd := New(conn)
	var xaddResult string
	errCmd := cmd.
		XAdd(&xaddResult, streamNmae, "*", map[string]string{key: value}).
		Commit()
	assert.Nil(t, errCmd)
	assert.Equal(t, xaddResult, "OK")
}
