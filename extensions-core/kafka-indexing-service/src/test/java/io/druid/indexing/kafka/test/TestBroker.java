/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.indexing.kafka.test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.SystemTime$;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import scala.Some;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

public class TestBroker implements Closeable
{
  private final static Random RANDOM = new Random();

  private final String zookeeperConnect;
  private final File directory;
  private final int id;
  private final Map<String, String> brokerProps;

  private volatile KafkaServer server;

  public TestBroker(String zookeeperConnect, File directory, int id, Map<String, String> brokerProps)
  {
    this.zookeeperConnect = zookeeperConnect;
    this.directory = directory;
    this.id = id;
    this.brokerProps = brokerProps == null ? ImmutableMap.<String, String>of() : brokerProps;
  }

  public void start()
  {
    final Properties props = new Properties();
    props.setProperty("zookeeper.connect", zookeeperConnect);
    props.setProperty("zookeeper.session.timeout.ms", "30000");
    props.setProperty("zookeeper.connection.timeout.ms", "30000");
    props.setProperty("log.dirs", directory.toString());
    props.setProperty("broker.id", String.valueOf(id));
    props.setProperty("port", String.valueOf(new Random().nextInt(9999) + 10000));
    props.putAll(brokerProps);

    final KafkaConfig config = new KafkaConfig(props);

    server = new KafkaServer(config, SystemTime$.MODULE$, Some.apply(String.format("TestingBroker[%d]-", id)));
    server.startup();
  }

  public int getPort()
  {
    return server.socketServer().config().port();
  }

  public KafkaProducer<byte[], byte[]> newProducer()
  {
    return new KafkaProducer(producerProperties());
  }

  public KafkaConsumer<byte[], byte[]> newConsumer()
  {
    return new KafkaConsumer(consumerProperties());
  }

  public Map<String, String> producerProperties()
  {
    final Map<String, String> props = Maps.newHashMap();
    props.put("bootstrap.servers", String.format("localhost:%d", getPort()));
    props.put("key.serializer", ByteArraySerializer.class.getName());
    props.put("value.serializer", ByteArraySerializer.class.getName());
    props.put("acks", "all");
    return props;
  }

  public Map<String, String> consumerProperties()
  {
    final Map<String, String> props = Maps.newHashMap();
    props.put("bootstrap.servers", String.format("localhost:%d", getPort()));
    props.put("key.deserializer", ByteArrayDeserializer.class.getName());
    props.put("value.deserializer", ByteArrayDeserializer.class.getName());
    props.put("group.id", String.valueOf(RANDOM.nextInt()));
    props.put("auto.offset.reset", "earliest");
    return props;
  }

  @Override
  public void close() throws IOException
  {
    if (server != null) {
      server.shutdown();
      server.awaitShutdown();
    }
  }
}
