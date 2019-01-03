package main

import (
	"context"
	"encoding/json"
	"io"

	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

type jsonCodec struct {
}

func (jc *jsonCodec) Marshal(v interface{}) ([]byte, error) {
	logrus.StandardLogger().Debugf("marshalling: %v", v)
	return json.Marshal(v)
}
func (jc *jsonCodec) Unmarshal(data []byte, v interface{}) error {
	logrus.StandardLogger().Debugf("unmarshalling: %v", string(data))
	return json.Unmarshal(data, &v)
}
func (jc *jsonCodec) String() string {
	return "application/json; encoding=utf-8"
}

func main() {
	logger := logrus.StandardLogger()
	logger.SetLevel(logrus.DebugLevel)
	conn, err := grpc.DialContext(context.Background(), "localhost:56113", grpc.WithInsecure())
	if err != nil {
		logger.WithError(err).Fatal("failed to create connection")
	}
	defer conn.Close()
	streamDesc := &grpc.StreamDesc{
		StreamName:    "query-json",
		ServerStreams: true,
	}
	ctx, cancelFn := context.WithCancel(context.Background())
	_ = cancelFn
	md := metadata.New(map[string]string{
		"X-Druid-Query-Id": "fake_id",
	})
	ctx = metadata.NewOutgoingContext(ctx, md)
	clientStream, err := conn.NewStream(ctx, streamDesc, "/org.apache.druid/query-json", grpc.CallCustomCodec(&jsonCodec{}))
	if err != nil {
		logger.WithError(err).Fatal("failed to create client stream")
	}
	jsonStr := `
	{
		"queryType": "topN",
		"dataSource": {
		  "type": "table",
		  "name": "Test datasource"
		},
		"virtualColumns": [],
		"dimension": {
		  "type": "default",
		  "dimension": "some dimension",
		  "outputName": "some dimension",
		  "outputType": "STRING"
		},
		"metric": {
		  "type": "numeric",
		  "metric": "some metric"
		},
		"threshold": 1,
		"intervals": {
		  "type": "LegacySegmentSpec",
		  "intervals": [
			"2017-01-01T00:00:00.000Z/2018-01-01T00:00:00.000Z"
		  ]
		},
		"granularity": {
		  "type": "all"
		},
		"aggregations": [
		  {
			"type": "count",
			"name": "some metric"
		  }
		]
	  }	  
	`
	var v interface{}
	if err := json.Unmarshal([]byte(jsonStr), &v); err != nil {
		logger.WithError(err).Fatal("failed to marshal json string")
	}
	if err := clientStream.SendMsg(v); err != nil {
		logger.WithError(err).Fatal("failed to send message")
	}
	if err := clientStream.CloseSend(); err != nil {
		logger.WithError(err).Fatal("failed to close send")
	}
	// This will send a cancel request:
	// cancelFn()
	for {
		if err := clientStream.RecvMsg(&v); err != nil {
			if err == io.EOF {
				break
			}
			logger.WithError(err).Fatal("failed to receive message")
		}
		logger.WithField("message", v).Debug("received message")
	}
	logger.Debug("exiting")
}
