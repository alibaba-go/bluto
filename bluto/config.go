package bluto

// Config is used to get initialization configs for Pool
type Config struct {
	// ---------------------------------------- dial options
	Network               string
	Address               string
	Password              string
	ConnectTimeoutSeconds int
	ReadTimeoutSeconds    int
	WriteTimeoutSeconds   int
	KeepAliveSeconds      int

	// ---------------------------------------- pool options
	MaxIdle                int
	MaxActive              int
	IdleTimeoutSeconds     int
	MaxConnLifetimeSeconds int
}
