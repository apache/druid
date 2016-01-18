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

package io.druid.examples.rabbitmq;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.rabbitmq.client.ConnectionFactory;
import io.druid.firehose.rabbitmq.JacksonifiedConnectionFactory;
import io.druid.firehose.rabbitmq.RabbitMQFirehoseConfig;
import io.druid.firehose.rabbitmq.RabbitMQFirehoseFactory;
import io.druid.jackson.DefaultObjectMapper;
import org.junit.Assert;
import org.junit.Test;

/**
 */
public class RabbitMQFirehoseFactoryTest
{
  private static final ObjectMapper mapper = new DefaultObjectMapper();

  @Test
  public void testSerde() throws Exception
  {
    RabbitMQFirehoseConfig config = new RabbitMQFirehoseConfig(
        "test",
        "test2",
        "test3",
        true,
        true,
        true,
        5,
        10,
        20
    );

    JacksonifiedConnectionFactory connectionFactory = new JacksonifiedConnectionFactory(
        "foo",
        9978,
        "user",
        "pw",
        "host",
        null,
        5,
        10,
        11,
        12,
        ImmutableMap.<String, Object>of("hi", "bye")
    );

    RabbitMQFirehoseFactory factory = new RabbitMQFirehoseFactory(
        connectionFactory,
        config,
        null
    );

    byte[] bytes = mapper.writeValueAsBytes(factory);
    RabbitMQFirehoseFactory factory2 = mapper.readValue(bytes, RabbitMQFirehoseFactory.class);
    byte[] bytes2 = mapper.writeValueAsBytes(factory2);

    Assert.assertArrayEquals(bytes, bytes2);

    Assert.assertEquals(factory.getConfig(), factory2.getConfig());
    Assert.assertEquals(factory.getConnectionFactory(), factory2.getConnectionFactory());
  }

  @Test
  public void testDefaultSerde() throws Exception
  {
    RabbitMQFirehoseConfig config = RabbitMQFirehoseConfig.makeDefaultConfig();

    JacksonifiedConnectionFactory connectionFactory = JacksonifiedConnectionFactory.makeDefaultConnectionFactory();

    RabbitMQFirehoseFactory factory = new RabbitMQFirehoseFactory(
        connectionFactory,
        config,
        null
    );

    byte[] bytes = mapper.writeValueAsBytes(factory);
    RabbitMQFirehoseFactory factory2 = mapper.readValue(bytes, RabbitMQFirehoseFactory.class);
    byte[] bytes2 = mapper.writeValueAsBytes(factory2);

    Assert.assertArrayEquals(bytes, bytes2);

    Assert.assertEquals(factory.getConfig(), factory2.getConfig());
    Assert.assertEquals(factory.getConnectionFactory(), factory2.getConnectionFactory());

    Assert.assertEquals(300, factory2.getConfig().getMaxDurationSeconds());

    Assert.assertEquals(ConnectionFactory.DEFAULT_HOST, factory2.getConnectionFactory().getHost());
    Assert.assertEquals(ConnectionFactory.DEFAULT_USER, factory2.getConnectionFactory().getUsername());
    Assert.assertEquals(ConnectionFactory.DEFAULT_AMQP_PORT, factory2.getConnectionFactory().getPort());
  }
}
