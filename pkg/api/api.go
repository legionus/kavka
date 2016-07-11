package api

var (
	Version         = "/v1"
	TopicsPath      = Version + "/topics"
	BlobsPath       = Version + "/blobs"
	InfoPath        = Version + "/info"
	InfoTopicsPath  = InfoPath + "/topics"
	PingPath        = "/ping"
	JSONPath        = Version + "/json"
	JSONTopicsPath  = JSONPath + "/topics"
	EtcdMembersPath = Version + "/etcd/members"
)
