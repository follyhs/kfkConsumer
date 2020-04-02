package kafkaConsumer

import (
	"fmt"
	"github.com/Shopify/sarama"
	"strings"
)

type ConsumerConfig struct {
	BrokerList []string // "10.141.10.66:9090,10.141.0.234:9090,10.141.39.56:9090"
	TimeoutMS  int      //  50
	Topic      string   // base-stats_topic
	GroupId    string   // group_1
	BatchSize  int      // 100
}

type ConsumerManager struct {
	dataBuf chan *sarama.ConsumerMessage
	close   chan bool
}

//订阅
func (c *ConsumerManager) InitConsumer(conf *ConsumerConfig) (*ConsumerManager, error) {
	config := sarama.NewConfig()
	config.Version = sarama.V0_10_2_0
	config.Consumer.Offsets.AutoCommit.Enable = true
	config.Consumer.Offsets.AutoCommit.Interval = 1 * time.Second
	c.dataBuf = make(chan *sarama.ConsumerMessage)
	c.close = make(chan bool)

	client, err := sarama.NewClient(BrokerList, config)
	if err != nil {
		fmt.Println(err)
		return err
	}

	offsetManager, err := sarama.NewOffsetManagerFromClient(GroupId, client)
	if err != nil {
		fmt.Println(err)
		return err
	}

	pids, err := client.Partitions(Topic)
	if err != nil {
		fmt.Println(err)
		return err
	}

	consumer, err := sarama.NewConsumerFromClient(client)
	if err != nil {
		fmt.Println(err)
	}
	defer consumer.Close()

	go func() {
		for {
			for _, v := range pids {
				consume(consumer, offsetManager, v)
			}
		}
	}()
}

func (c *ConsumerManager) Close() {
	c.close <- true
}

func (c *ConsumerManager) Pull() <-chan *sarama.ConsumerMessage {
	return c.dataBuf
}
func (c *ConsumerManager) consume(c sarama.Consumer, om sarama.OffsetManager, p int32) {
	pom, err := om.ManagePartition(c.Topic, p)
	if err != nil {
		fmt.Println(err)
	}
	defer pom.Close()

	offset, _ := pom.NextOffset()
	if offset == -1 {
		offset = sarama.OffsetOldest
	}

	pc, err := c.ConsumePartition(c.Topic, p, offset)
	if err != nil {
		fmt.Println(err)
	}
	defer pc.Close()

	select {
	case msg := <-pc.Messages():
		c.dataBuf <- msg
		pom.MarkOffset(msg.Offset+1, "")
	case err := <-pc.Errors():
		fmt.Printf("err :%s\n", err.Error())
	case <-c.close:
		//close()
	}
}
