package plumtree

type Config struct {
	Fanout                     int `env:"PLUMTREE_FANOUT"`
	AnnounceInterval           int `env:"PLUMTREE_ANNOUNCE_INTERVAL"`
	MissingMsgTimeout          int `env:"PLUMTREE_MISSING_MSG_TIMEOUT"`
}
