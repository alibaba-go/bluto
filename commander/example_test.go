package commander_test

import (
	"fmt"

	"github.com/alibaba-go/bluto/bluto"
	"github.com/alibaba-go/bluto/commander"
)

var ex uint64 = 1
var nx bool = true

// Example shows more complicated and dynamic use of options
// for example ex and nx variables are determined in run-time
func ExampleCommander_Set_optionSlice() {
	bluto, _ := bluto.New(bluto.Config{
		Address:               "localhost:6379",
		ConnectTimeoutSeconds: 10,
		ReadTimeoutSeconds:    10,
	})
	defer bluto.ClosePool()

	options := []commander.SetOption{}
	if nx != false {
		options = append(options, commander.SetOptionNX{})
	}
	if ex != 0 {
		options = append(options, commander.SetOptionEX{EX: ex})
	}

	var setResult string
	var getResult string

	bluto.Borrow().
		Set(&setResult, "SomeKey", "SomeValue", options...).
		Get(&getResult, "SomeKey").
		Commit()

	fmt.Println(getResult)

	// output: SomeValue
}
