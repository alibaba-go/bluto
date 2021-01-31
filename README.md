[![Go](https://github.com/alibaba-go/bluto/workflows/Go/badge.svg)](https://github.com/alibaba-go/bluto/actions)
[![GoDoc](https://pkg.go.dev/badge/github.com/alibaba-go/bluto?status.svg)](https://pkg.go.dev/github.com/alibaba-go/bluto?tab=doc)
[![Go Report Card](https://goreportcard.com/badge/github.com/alibaba-go/bluto)](https://goreportcard.com/report/github.com/alibaba-go/bluto)
[![CII Best Practices](https://bestpractices.coreinfrastructure.org/projects/4289/badge)](https://bestpractices.coreinfrastructure.org/projects/4289)
[![Coverage](https://codecov.io/gh/alibaba-go/bluto/branch/master/graph/badge.svg)](https://codecov.io/gh/alibaba-go/bluto)


# Bluto
Bluto is a golang implementation of Redis client based on [Redigo](https://github.com/gomodule/redigo). The client manages a connection pool for each node and uses goroutine to execute as concurrently as possible, leading to its high efficiency and low latency.

**Supported**:
* Most commands of keys, strings, lists, sets, sorted sets, hashes.


## Documentation
[API Reference](https://pkg.go.dev/github.com/alibaba-go/bluto)

## Installation
Install Bluto with go tool:
```
    go get github.com/alibaba-go/bluto/...
```
    
## Usage
To use Redis cluster, you need to import the package and create a new Bluto client
with a config:
```go
import "https://github.com/alibaba-go/bluto"

bluto, err := bluto.New(
    &bluto.config{
    Address:              "localhost:6379",
    Password              "password"
    ConnectTimeoutSeconds: 10,
    ReadTimeoutSeconds:    10,
    })
```

### Basic
Bluto gives you a commander by calling Borrow(), an interface to run Redis commands (GET, SELECT, etc.) over a Redis connection pool that simplifies all the pool's management.

**RESTRICTION**: Please be sure the first argument in commander's args is result and all commands should ends with Commit().The optional arguments are passed as variadic args.

See full redis commands: 

```go
bluto.Borrow().Set(&setResult, "key", "value", SetOptionKEEPTTL{}).Commit()
bluto.Borrow().Get(&getResult, "key").Commit()
bluto.Borrow().Incr(&incrResult, "key").Commit()
```
You can also chain redis commands like this:
```go
bluto.Borrow().Select(&selectResult, 2).Set(&setResult, "key", "value",SetOptionKEEPTTL{}).Incr(&incrResult, "key").Commit()
bluto.Borrow().Select(&selectResult, 2).Get(&getResult, "key").Decr(&decrResult, "key").Del(&delResult, "key").Commit()
```
Also, you can use Values and Scan to convert replies to multiple values with different types.

### Command's Options
You can pass options as variadic args as last arguments.
The Options for each command is interface which is satisfied by defined option structs.
You can pass multiple options like this:
```go
bluto.Borrow().Set(&setResult, "key", "value", SetOptionEX{EX:1}, SetOptionNX{}, SetOptionKEEPTTL{}).Commit()
```
For more advanced example look at [example](https://pkg.go.dev/github.com/alibaba-go/bluto/commander#example-Commander.Set-OptionSlice)

## Contributing
See [CONTRIBUTING.md](https://github.com/alibaba-go/bluto/blob/master/CONTRIBUTING.md).

## Contact
Bug reports and feature requests are welcome.
If you have any question, please email us at rd@alibaba.ir.

## License
Released under MIT License 
