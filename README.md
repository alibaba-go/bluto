# Bluto
Bluto is a golang implementation of redis client based on [Redigo](https://github.com/gomodule/redigo).The client manages a connection pool 
for each node, uses goroutine to execute as concurrently as possible, which leads 
to its high efficiency and low lantency.

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
To use redis cluster, you need import the package and create a new bluto client
with an options:
```go
import ""

bluto, err := bluto.New(
    &bluto.config{
    Address:              "localhost:6379",
    Password              "password"
    ConnectTimeoutSeconds: 10,
    ReadTimeoutSeconds:    10,
    })
```

### Basic
Bluto gives you a commander by calling Borrow() which is interface to run redis commands exp.(GET, SELECT , ...) over a pool of redis connection which simplify all the managments of pool. 


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
You can pass multipe options like this:
```go
bluto.Borrow().Set(&setResult, "key", "value", SetOptionEX{EX:1}, SetOptionNX{}, SetOptionKEEPTTL{}).Commit()
```
## Contact
Bug reports and feature requests are welcome.
If you have any question, please email us at rd@alibaba.ir.

## License
Released under MIT License 
