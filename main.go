package main

import (
	"fmt"
	"sync"

	"github.com/Shopify/sarama"
	"github.com/json-iterator/go"
	"github.com/openzipkin/zipkin-go/model"
	"github.com/openzipkin/zipkin-go/reporter/kafka"
	"log"
	"os"
	"time"
	"flag"
)

var (
	wg          sync.WaitGroup
	json        = jsoniter.ConfigCompatibleWithStandardLibrary
	brokerList  = flag.String("brokers", os.Getenv("KAFKA_PEERS"), "The comma separated list of brokers in the Kafka cluster. You can also set the KAFKA_PEERS environment variable")
	topic       = flag.String("topic", "", "REQUIRED: the topic to consumer")
	zipkinTopic = flag.String("zipkin", "zipkin", "Produce message to zipkin Topic, default is zipkin")
)

type ZipkinFileData struct {
	TraceId        string             `json:"traceId"`
	ParentId       model.ID           `json:"parentId,omitempty"`
	ID             model.ID           `json:"id"`
	Name           string             `json:"name"`
	TimeStamp      int64              `json:"timestamp"`
	Duration       int                `json:"duration"`
	LocalEndpoint  model.Endpoint     `json:"localEndpoint"`
	RemoteEndpoint model.Endpoint     `json:"remoteEndpoint"`
	Kind           model.Kind         `json:"kind"`
	Annotations    []model.Annotation `json:"annotations,omitempty"`
	Tags           map[string]string  `json:"tags"`
}

func main() {
	flag.Parse()

	if *brokerList == "" {
		printUsageErrorAndExit("no -brokers specified. Alternatively, set the KAFKA_PEERS environment variable")
	}

	if *topic == "" {
		printUsageErrorAndExit("no -topic specified")
	}

	consumer, err := sarama.NewConsumer([]string{*brokerList}, nil)
	if err != nil {
		panic(err)
	}

	producer, err := sarama.NewAsyncProducer([]string{*brokerList}, nil)
	if err != nil {
		panic(err)
	}

	partitionList, err := consumer.Partitions(*topic)
	if err != nil {
		panic(err)
	}

	for partition := range partitionList {
		pc, err := consumer.ConsumePartition(*topic, int32(partition), sarama.OffsetNewest)
		if err != nil {
			println(err)
			panic(err)
		}

		defer pc.AsyncClose()

		wg.Add(1)

		go func(sarama.PartitionConsumer) {
			defer wg.Done()
			for msg := range pc.Messages() {
				//fmt.Printf("Partition:%d, Offset:%d, Key:%s, Value:%s\n", msg.Partition, msg.Offset, string(msg.Key), string(msg.Value))
				report, err := kafka.NewReporter([]string{*brokerList}, kafka.Topic(*zipkinTopic),
					kafka.Producer(producer), kafka.Logger(log.New(os.Stderr, "", log.LstdFlags)))
				if err != nil {
					panic(err)
				}
				defer report.Close()
				data := ZipkinFileData{}
				json.Unmarshal(msg.Value, &data)
				traceId, err := model.TraceIDFromHex(data.TraceId)
				if err != nil {
					panic(err)
				}
				span := model.SpanModel{
					Name: data.Name,
					SpanContext: model.SpanContext{
						TraceID:  traceId,
						ID:       data.ID,
						ParentID: &data.ParentId,
					},
					Kind:           data.Kind,
					Timestamp:      time.Unix(0, int64(data.TimeStamp*1000)),
					Duration:       time.Duration(int64(data.Duration * 1000)),
					LocalEndpoint:  &data.LocalEndpoint,
					RemoteEndpoint: &data.RemoteEndpoint,
					Annotations:    data.Annotations,
					Tags:           data.Tags,
				}
				report.Send(span)
				//fmt.Println("report send finish")
			}

		}(pc)
	}
	wg.Wait()
	consumer.Close()
}

func printUsageErrorAndExit(message string) {
	fmt.Fprintln(os.Stderr, "ERROR:", message)
	fmt.Fprintln(os.Stderr)
	fmt.Fprintln(os.Stderr, "Available command line options:")
	flag.PrintDefaults()
	os.Exit(64)
}
