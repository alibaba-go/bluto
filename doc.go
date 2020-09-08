/*package bluto is redis library wraper over redigo which add type safty, chained command and
managed connection pool which makes it easier for developer to use the library and reduce runtime bugs.

bluto instance is compilitly thread-safe which means you can pass bluto object
to diffretent go-routines without race condition it also automaticly return finished
connection to connection pool.

pool also support config for connections like timeout and health-check for better managment.

commander is borrowed from bluto instance which can be used to chain multipe commands toghether and
run all of them with commit.This chaining reduce the time when the connection is kept so it improves
the pool connection reuse latency.


*/

package bluto
