package plumtree

type Config struct {
	Fanout                     int
	AnnounceInterval           int
	MissingMsgTimeout          int
	SecondaryMissingMsgTimeout int
}
