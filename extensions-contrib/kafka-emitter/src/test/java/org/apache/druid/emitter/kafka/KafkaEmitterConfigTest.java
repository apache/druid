/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.emitter.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.exc.ValueInstantiationException;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class KafkaEmitterConfigTest
{
  private ObjectMapper mapper = new DefaultObjectMapper();

  @Before
  public void setUp()
  {
    mapper.setInjectableValues(new InjectableValues.Std().addValue(ObjectMapper.class, new DefaultObjectMapper()));
  }

  @Test
  public void testSerDeserKafkaEmitterConfig() throws IOException
  {
    KafkaEmitterConfig kafkaEmitterConfig = new KafkaEmitterConfig("hostname", null, "metricTest",
        "alertTest", "requestTest", "metadataTest",
        "clusterNameTest", ImmutableMap.<String, String>builder()
        .put("testKey", "testValue").build()
    );
    String kafkaEmitterConfigString = mapper.writeValueAsString(kafkaEmitterConfig);
    KafkaEmitterConfig kafkaEmitterConfigExpected = mapper.readerFor(KafkaEmitterConfig.class)
        .readValue(kafkaEmitterConfigString);
    Assert.assertEquals(kafkaEmitterConfigExpected, kafkaEmitterConfig);
  }

  @Test
  public void testSerDeserKafkaEmitterConfigNullRequestTopic() throws IOException
  {
    KafkaEmitterConfig kafkaEmitterConfig = new KafkaEmitterConfig("hostname", null, "metricTest",
        "alertTest", null, "metadataTest",
        "clusterNameTest", ImmutableMap.<String, String>builder()
        .put("testKey", "testValue").build()
    );
    String kafkaEmitterConfigString = mapper.writeValueAsString(kafkaEmitterConfig);
    KafkaEmitterConfig kafkaEmitterConfigExpected = mapper.readerFor(KafkaEmitterConfig.class)
        .readValue(kafkaEmitterConfigString);
    Assert.assertEquals(kafkaEmitterConfigExpected, kafkaEmitterConfig);
  }

  @Test
  public void testSerDeserKafkaEmitterConfigNullMetricsTopic() throws IOException
  {
    Set<KafkaEmitterConfig.EventType> eventTypeSet = new HashSet<KafkaEmitterConfig.EventType>();
    eventTypeSet.add(KafkaEmitterConfig.EventType.SEGMENT_METADATA);
    KafkaEmitterConfig kafkaEmitterConfig = new KafkaEmitterConfig("hostname", eventTypeSet, null,
        null, null, "metadataTest",
        "clusterNameTest", ImmutableMap.<String, String>builder()
        .put("testKey", "testValue").build()
    );
    String kafkaEmitterConfigString = mapper.writeValueAsString(kafkaEmitterConfig);
    KafkaEmitterConfig kafkaEmitterConfigExpected = mapper.readerFor(KafkaEmitterConfig.class)
        .readValue(kafkaEmitterConfigString);
    Assert.assertEquals(kafkaEmitterConfigExpected, kafkaEmitterConfig);
  }

  @Test
  public void testSerDeNotRequiredKafkaProducerConfig()
  {
    KafkaEmitterConfig kafkaEmitterConfig = new KafkaEmitterConfig("localhost:9092", null, "metricTest",
        "alertTest", null, "metadataTest",
        "clusterNameTest", null
    );
    try {
      @SuppressWarnings("unused")
      KafkaEmitter emitter = new KafkaEmitter(kafkaEmitterConfig, mapper);
    }
    catch (NullPointerException e) {
      Assert.fail();
    }
  }

  @Test
  public void testDeserializeEventTypesWithDifferentCase() throws JsonProcessingException
  {
    Assert.assertEquals(KafkaEmitterConfig.EventType.SEGMENT_METADATA, mapper.readValue("\"segment_metadata\"", KafkaEmitterConfig.EventType.class));
    Assert.assertEquals(KafkaEmitterConfig.EventType.ALERTS, mapper.readValue("\"alerts\"", KafkaEmitterConfig.EventType.class));
    Assert.assertThrows(ValueInstantiationException.class, () -> mapper.readValue("\"segmentMetadata\"", KafkaEmitterConfig.EventType.class));
  }

  @Test
  public void testJacksonModules()
  {
    Assert.assertTrue(new KafkaEmitterModule().getJacksonModules().isEmpty());
  }
}
