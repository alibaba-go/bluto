/*package bluto is redis library wrapper over redigo which adds type safety, chained commands and
managed connection pool which makes it easier for developers to use the library and reduce runtime bugs.

bluto instance is completely thread-safe which means you can pass bluto objects
to different goroutines without race condition, it also automatically returns finished
connections to connection pool.

pool also supports config for connections like timeout and health-check for better management.

commander is borrowed from bluto instance which can be used to chain multiple commands together and
run all of them with commit. This chaining reduces the time when the connection is kept so it improves
the pool connection reuse latency.

*/

package bluto
