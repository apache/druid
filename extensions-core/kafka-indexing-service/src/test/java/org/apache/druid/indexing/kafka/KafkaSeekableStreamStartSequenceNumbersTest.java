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

package org.apache.druid.indexing.kafka;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.exc.InvalidTypeIdException;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Injector;
import com.google.inject.name.Names;
import org.apache.druid.data.input.kafka.KafkaTopicPartition;
import org.apache.druid.guice.StartupInjectorBuilder;
import org.apache.druid.indexing.seekablestream.SeekableStreamStartSequenceNumbers;
import org.apache.druid.initialization.CoreInjectorBuilder;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.java.util.common.jackson.JacksonUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;
import java.util.Set;

public class KafkaSeekableStreamStartSequenceNumbersTest
{
  private static final ObjectMapper OBJECT_MAPPER = createObjectMapper();

  @Test
  public void testSerde() throws Exception
  {
    final String stream = "theStream";
    final Map<KafkaTopicPartition, Long> offsetMap = ImmutableMap.of(
        new KafkaTopicPartition(false, null, 1), 2L,
        new KafkaTopicPartition(false, null, 3), 4L
    );

    Set<KafkaTopicPartition> exclusivePartitions = ImmutableSet.of(new KafkaTopicPartition(false, null, 1));


    final KafkaSeekableStreamStartSequenceNumbers partitions = new KafkaSeekableStreamStartSequenceNumbers(
        stream,
        null,
        offsetMap,
        null,
        exclusivePartitions
    );
    final String serializedString = OBJECT_MAPPER.writeValueAsString(partitions);

    // Check round-trip.
    final SeekableStreamStartSequenceNumbers<KafkaTopicPartition, Long> partitions2 = OBJECT_MAPPER.readValue(
        serializedString,
        new TypeReference<>() {}
    );

    Assert.assertEquals(
        "Round trip",
        partitions,
        new KafkaSeekableStreamStartSequenceNumbers(
            partitions2.getStream(),
            partitions2.getTopic(),
            partitions2.getPartitionSequenceNumberMap(),
            partitions2.getPartitionOffsetMap(),
            partitions2.getExclusivePartitions()
        )
    );

    // Check backwards compatibility.
    final Map<String, Object> asMap = OBJECT_MAPPER.readValue(
        serializedString,
        JacksonUtils.TYPE_REFERENCE_MAP_STRING_OBJECT
    );

    Assert.assertEquals(stream, asMap.get("stream"));
    Assert.assertEquals(stream, asMap.get("topic"));

    // Jackson will deserialize the maps as string -> int maps, not int -> long.
    Assert.assertEquals(
        offsetMap,
        OBJECT_MAPPER.convertValue(asMap.get("partitionSequenceNumberMap"), new TypeReference<Map<KafkaTopicPartition, Long>>() {})
    );
    Assert.assertEquals(
        offsetMap,
        OBJECT_MAPPER.convertValue(asMap.get("partitionOffsetMap"), new TypeReference<Map<KafkaTopicPartition, Long>>() {})
    );

    Assert.assertEquals(
        exclusivePartitions,
        OBJECT_MAPPER.convertValue(asMap.get("exclusivePartitions"), new TypeReference<Set<KafkaTopicPartition>>() {})
    );

    // check that KafkaSeekableStreamStartSequenceNumbers not registered with mapper, so no possible collision
    // when deserializing it from String / bytes
    boolean expectedExceptionThrown = false;
    try {
      OBJECT_MAPPER.readValue(
          serializedString,
          KafkaSeekableStreamStartSequenceNumbers.class
      );
    }
    catch (InvalidTypeIdException e) {
      expectedExceptionThrown = true;
    }

    Assert.assertTrue("KafkaSeekableStreamStartSequenceNumbers should not be registered type", expectedExceptionThrown);
  }

  private static ObjectMapper createObjectMapper()
  {
    DruidModule module = new KafkaIndexTaskModule();
    final Injector injector = new CoreInjectorBuilder(new StartupInjectorBuilder().build())
        .addModule(
            binder -> {
              binder.bindConstant().annotatedWith(Names.named("serviceName")).to("test");
              binder.bindConstant().annotatedWith(Names.named("servicePort")).to(8000);
              binder.bindConstant().annotatedWith(Names.named("tlsServicePort")).to(9000);
            }
        )
        .build();

    ObjectMapper objectMapper = injector.getInstance(ObjectMapper.class);
    module.getJacksonModules().forEach(objectMapper::registerModule);
    return objectMapper;
  }
}
