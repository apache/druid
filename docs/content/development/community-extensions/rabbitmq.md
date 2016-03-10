---
layout: doc_page
---

# RabbitMQ

## Firehose

#### RabbitMQFirehose

This firehose ingests events from a define rabbit-mq queue.

**Note:** Add **amqp-client-3.2.1.jar** to lib directory of druid to use this firehose.

A sample spec for rabbitmq firehose:

```json
"firehose" : {
   "type" : "rabbitmq",
   "connection" : {
     "host": "localhost",
     "port": "5672",
     "username": "test-dude",
     "password": "test-word",
     "virtualHost": "test-vhost",
     "uri": "amqp://mqserver:1234/vhost"
   },
   "config" : {
     "exchange": "test-exchange",
     "queue" : "druidtest",
     "routingKey": "#",
     "durable": "true",
     "exclusive": "false",
     "autoDelete": "false",
     "maxRetries": "10",
     "retryIntervalSeconds": "1",
     "maxDurationSeconds": "300"
   }
}
```

|property|description|default|required?|
|--------|-----------|-------|---------|
|type|This should be "rabbitmq"|N/A|yes|
|host|The hostname of the RabbitMQ broker to connect to|localhost|no|
|port|The port number to connect to on the RabbitMQ broker|5672|no|
|username|The username to use to connect to RabbitMQ|guest|no|
|password|The password to use to connect to RabbitMQ|guest|no|
|virtualHost|The virtual host to connect to|/|no|
|uri|The URI string to use to connect to RabbitMQ| |no|
|exchange|The exchange to connect to| |yes|
|queue|The queue to connect to or create| |yes|
|routingKey|The routing key to use to bind the queue to the exchange| |yes|
|durable|Whether the queue should be durable|false|no|
|exclusive|Whether the queue should be exclusive|false|no|
|autoDelete|Whether the queue should auto-delete on disconnect|false|no|
|maxRetries|The max number of reconnection retry attempts| |yes|
|retryIntervalSeconds|The reconnection interval| |yes|
|maxDurationSeconds|The max duration of trying to reconnect| |yes|
