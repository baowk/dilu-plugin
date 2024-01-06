package mq

type IQueue interface {
	String() string
	Publish(data []byte) error
	Register(name string, f ConsumerFunc)
	Run()
	Shutdown()
}

type ConsumerFunc func(data []byte) error

// type IMessager interface {
// 	SetID(string)
// 	SetStream(string)
// 	SetValues(map[string]interface{})
// 	GetID() string
// 	GetStream() string
// 	GetValues() map[string]interface{}
// 	GetPrefix() string
// 	SetPrefix(string)
// 	SetErrorCount(count int)
// 	GetErrorCount() int
// }
