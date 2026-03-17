package nsq

import (
	"encoding/hex"
	"encoding/json"
	"fmt"

	"github.com/nsqio/go-nsq"
	"github.com/zhiyunliu/glue/queue"
	"github.com/zhiyunliu/golibs/bytesconv"
	"github.com/zhiyunliu/golibs/xtypes"
)

type nsqMessage struct {
	retryCount int64
	msg        *nsq.Message
	objMsg     queue.Message
	err        error
}

func (m *nsqMessage) MessageId() string {
	if m.msg == nil {
		return ""
	}
	return hex.EncodeToString(m.msg.ID[:])
}

func (m *nsqMessage) RetryCount() int64 {
	if m.retryCount > 0 {
		return m.retryCount
	}
	if m.msg != nil {
		m.retryCount = int64(m.msg.Attempts)
		if m.retryCount > 0 {
			m.retryCount--
		}
	}
	return m.retryCount
}

func (m *nsqMessage) Ack() error {
	m.err = nil
	return nil
}

func (m *nsqMessage) Nack(err error) error {
	m.err = err
	return nil
}

func (m *nsqMessage) Original() string {
	if m.msg == nil {
		return ""
	}
	return bytesconv.BytesToString(m.msg.Body)
}

func (m *nsqMessage) GetMessage() queue.Message {
	if m.objMsg == nil && m.msg != nil {
		m.objMsg = newMsgBody(m.msg.Body)
	}
	return m.objMsg
}

func (m *nsqMessage) Error() error {
	return m.err
}

func newMsgBody(data []byte) queue.Message {
	if !json.Valid(data) {
		panic(fmt.Errorf("nsq msg data is invalid json: %s", string(data)))
	}
	msgItem := &queue.MsgItem{
		HeaderMap: make(xtypes.SMap),
	}
	_ = json.Unmarshal(data, msgItem)
	return msgItem
}

