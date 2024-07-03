package kafka

import (
	"context"
	"errors"
	"github.com/IBM/sarama"
	"github.com/ehsaniara/scheduler/interfaces/interfacesfakes"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"strings"
	"testing"
	"time"
)

func TestNewConsumerGroup(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()

	fakeConsumerGroup := &interfacesfakes.FakeConsumerGroup{}
	fakeActionDispatcher := func(message *sarama.ConsumerMessage) { return }

	dispatchSchedulerTopics := strings.Split("test_topic", `,`)

	c := NewConsumer(dispatchSchedulerTopics, fakeConsumerGroup, fakeActionDispatcher)
	go c.Start(ctx)
	time.Sleep(time.Millisecond * 100)
	assert.Greater(t, fakeConsumerGroup.ConsumeCallCount(), 1)
	c.Stop()

}

func TestNewConsumerGroupErrClosedConsumerGroup(t *testing.T) {
	ctx := context.Background()

	fakeConsumerGroup := &interfacesfakes.FakeConsumerGroup{}
	fakeActionDispatcher := func(message *sarama.ConsumerMessage) { return }

	dispatchSchedulerTopics := strings.Split("test_topic", `,`)

	fakeConsumerGroup.ConsumeReturns(sarama.ErrClosedConsumerGroup)
	c := NewConsumer(dispatchSchedulerTopics, fakeConsumerGroup, fakeActionDispatcher)
	c.Start(ctx)
	//give error after it get called once
	assert.Equal(t, 1, fakeConsumerGroup.ConsumeCallCount())
	c.Stop()
}

func TestNewConsumerGroupContextSomeError(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)

	fakeConsumerGroup := &interfacesfakes.FakeConsumerGroup{}
	fakeActionDispatcher := func(message *sarama.ConsumerMessage) { return }

	dispatchSchedulerTopics := strings.Split("test_topic", `,`)

	fakeConsumerGroup.ConsumeReturns(errors.New("test error"))
	c := NewConsumer(dispatchSchedulerTopics, fakeConsumerGroup, fakeActionDispatcher)
	go c.Start(ctx)
	cancel()
	//give error after it get called once
	assert.Equal(t, 0, fakeConsumerGroup.ConsumeCallCount())
	c.Stop()
}

func TestNewConsumerGroupContextError(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)

	fakeConsumerGroup := &interfacesfakes.FakeConsumerGroup{}
	fakeActionDispatcher := func(message *sarama.ConsumerMessage) { return }

	dispatchSchedulerTopics := strings.Split("test_topic", `,`)

	c := NewConsumer(dispatchSchedulerTopics, fakeConsumerGroup, fakeActionDispatcher)
	ctx.Done()
	go c.Start(ctx)
	cancel()
	//give error after it get called once
	assert.Equal(t, 0, fakeConsumerGroup.ConsumeCallCount())
	c.Stop()
}

func TestConsumeClaim(t *testing.T) {
	session := new(MockConsumerGroupSession)
	claim := new(MockConsumerGroupClaim)
	fakeConsumerGroup := &interfacesfakes.FakeConsumerGroup{}

	message := &sarama.ConsumerMessage{
		Value:     []byte("test message"),
		Timestamp: time.Now(),
		Topic:     "test-topic",
	}
	messages := make(chan *sarama.ConsumerMessage, 1)
	messages <- message
	close(messages)

	claim.On("Messages").Return((<-chan *sarama.ConsumerMessage)(messages))
	session.On("MarkMessage", message, "").Return(nil)

	dispatchMessage := func(msg *sarama.ConsumerMessage) {
		assert.Equal(t, "test message", string(msg.Value))
		return
	}

	newConsumer := NewConsumer(strings.Split("test_topic", `,`), fakeConsumerGroup, dispatchMessage)

	h := consumerGroupHandler{
		dispatchMessage: dispatchMessage,
		ready:           make(chan bool, 1),
	}

	err := h.ConsumeClaim(session, claim)
	assert.NoError(t, err)
	newConsumer.Stop()
	session.AssertCalled(t, "MarkMessage", message, "")
}

// MockConsumerGroupSession is a mock implementation of sarama.ConsumerGroupSession
type MockConsumerGroupSession struct {
	mock.Mock
}

func (m *MockConsumerGroupSession) Commit() {
	panic("implement me")
}

func (m *MockConsumerGroupSession) Claims() map[string][]int32 {
	args := m.Called()
	return args.Get(0).(map[string][]int32)
}

func (m *MockConsumerGroupSession) MemberID() string {
	args := m.Called()
	return args.String(0)
}

func (m *MockConsumerGroupSession) GenerationID() int32 {
	args := m.Called()
	return int32(args.Int(0))
}

func (m *MockConsumerGroupSession) MarkOffset(topic string, partition int32, offset int64, metadata string) {
	m.Called(topic, partition, offset, metadata)
}

func (m *MockConsumerGroupSession) ResetOffset(topic string, partition int32, offset int64, metadata string) {
	m.Called(topic, partition, offset, metadata)
}

func (m *MockConsumerGroupSession) MarkMessage(msg *sarama.ConsumerMessage, metadata string) {
	m.Called(msg, metadata)
}

func (m *MockConsumerGroupSession) Context() context.Context {
	args := m.Called()
	return args.Get(0).(context.Context)
}

// MockConsumerGroupClaim is a mock implementation of sarama.ConsumerGroupClaim
type MockConsumerGroupClaim struct {
	mock.Mock
}

func (m *MockConsumerGroupClaim) Topic() string {
	args := m.Called()
	return args.String(0)
}

func (m *MockConsumerGroupClaim) Partition() int32 {
	args := m.Called()
	return int32(args.Int(0))
}

func (m *MockConsumerGroupClaim) InitialOffset() int64 {
	args := m.Called()
	return int64(args.Int(0))
}

func (m *MockConsumerGroupClaim) HighWaterMarkOffset() int64 {
	args := m.Called()
	return int64(args.Int(0))
}

func (m *MockConsumerGroupClaim) Messages() <-chan *sarama.ConsumerMessage {
	args := m.Called()
	return args.Get(0).(<-chan *sarama.ConsumerMessage)
}
