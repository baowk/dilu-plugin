package mq

import (
	"sync"
)

type BaseMsg struct {
	prefix string
	ID     string
	Stream string
	Values map[string]interface{}
}

type Message struct {
	BaseMsg
	ErrorCount int
	mux        sync.RWMutex
}

func (m *Message) GetID() string {
	return m.ID
}

func (m *Message) GetStream() string {
	m.mux.Lock()
	defer m.mux.Unlock()
	return m.Stream
}

func (m *Message) GetValues() map[string]interface{} {
	m.mux.Lock()
	defer m.mux.Unlock()
	return m.Values
}

func (m *Message) SetID(id string) {
	m.ID = id
}

func (m *Message) SetStream(stream string) {
	m.mux.Lock()
	defer m.mux.Unlock()
	m.Stream = stream
}

func (m *Message) SetValues(values map[string]interface{}) {
	m.mux.Lock()
	defer m.mux.Unlock()
	m.Values = values
}

func (m *Message) GetPrefix() (prefix string) {
	m.mux.Lock()
	defer m.mux.Unlock()
	return m.prefix
}

func (m *Message) SetPrefix(prefix string) {
	m.mux.Lock()
	defer m.mux.Unlock()
	if m.Values == nil {
		m.Values = make(map[string]interface{})
	}
	m.prefix = prefix
}

func (m *Message) SetErrorCount(count int) {
	m.ErrorCount = count
}

func (m *Message) GetErrorCount() int {
	return m.ErrorCount
}
