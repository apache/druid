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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Injector;
import com.google.inject.name.Names;
import org.apache.druid.data.input.kafka.KafkaTopicPartition;
import org.apache.druid.guice.StartupInjectorBuilder;
import org.apache.druid.indexing.overlord.DataSourceMetadata;
import org.apache.druid.indexing.seekablestream.SeekableStreamEndSequenceNumbers;
import org.apache.druid.indexing.seekablestream.SeekableStreamStartSequenceNumbers;
import org.apache.druid.initialization.CoreInjectorBuilder;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.utils.CollectionUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

public class KafkaDataSourceMetadataTest
{
  private static final KafkaDataSourceMetadata START0 = startMetadata(ImmutableMap.of());
  private static final KafkaDataSourceMetadata START1 = startMetadata(ImmutableMap.of(0, 2L, 1, 3L));
  private static final KafkaDataSourceMetadata START2 = startMetadata(ImmutableMap.of(0, 2L, 1, 4L, 2, 5L));
  private static final KafkaDataSourceMetadata START3 = startMetadata(ImmutableMap.of(0, 2L, 2, 5L));
  private static final KafkaDataSourceMetadata END0 = endMetadata(ImmutableMap.of());
  private static final KafkaDataSourceMetadata END1 = endMetadata(ImmutableMap.of(0, 2L, 2, 5L));
  private static final KafkaDataSourceMetadata END2 = endMetadata(ImmutableMap.of(0, 2L, 1, 4L));

  @Test
  public void testMatches()
  {
    Assert.assertTrue(START0.matches(START0));
    Assert.assertTrue(START0.matches(START1));
    Assert.assertTrue(START0.matches(START2));
    Assert.assertTrue(START0.matches(START3));

    Assert.assertTrue(START1.matches(START0));
    Assert.assertTrue(START1.matches(START1));
    Assert.assertFalse(START1.matches(START2));
    Assert.assertTrue(START1.matches(START3));

    Assert.assertTrue(START2.matches(START0));
    Assert.assertFalse(START2.matches(START1));
    Assert.assertTrue(START2.matches(START2));
    Assert.assertTrue(START2.matches(START3));

    Assert.assertTrue(START3.matches(START0));
    Assert.assertTrue(START3.matches(START1));
    Assert.assertTrue(START3.matches(START2));
    Assert.assertTrue(START3.matches(START3));

    Assert.assertTrue(END0.matches(END0));
    Assert.assertTrue(END0.matches(END1));
    Assert.assertTrue(END0.matches(END2));

    Assert.assertTrue(END1.matches(END0));
    Assert.assertTrue(END1.matches(END1));
    Assert.assertTrue(END1.matches(END2));

    Assert.assertTrue(END2.matches(END0));
    Assert.assertTrue(END2.matches(END1));
    Assert.assertTrue(END2.matches(END2));
  }

  @Test
  public void testIsValidStart()
  {
    Assert.assertTrue(START0.isValidStart());
    Assert.assertTrue(START1.isValidStart());
    Assert.assertTrue(START2.isValidStart());
    Assert.assertTrue(START3.isValidStart());
  }

  @Test
  public void testPlus()
  {
    Assert.assertEquals(
        startMetadata(ImmutableMap.of(0, 2L, 1, 3L, 2, 5L)),
        START1.plus(START3)
    );

    Assert.assertEquals(
        startMetadata(ImmutableMap.of(0, 2L, 1, 4L, 2, 5L)),
        START0.plus(START2)
    );

    Assert.assertEquals(
        startMetadata(ImmutableMap.of(0, 2L, 1, 4L, 2, 5L)),
        START1.plus(START2)
    );

    Assert.assertEquals(
        startMetadata(ImmutableMap.of(0, 2L, 1, 3L, 2, 5L)),
        START2.plus(START1)
    );

    Assert.assertEquals(
        startMetadata(ImmutableMap.of(0, 2L, 1, 4L, 2, 5L)),
        START2.plus(START2)
    );

    Assert.assertEquals(
        endMetadata(ImmutableMap.of(0, 2L, 2, 5L)),
        END0.plus(END1)
    );

    Assert.assertEquals(
        endMetadata(ImmutableMap.of(0, 2L, 1, 4L, 2, 5L)),
        END1.plus(END2)
    );
  }

  @Test
  public void testMinus()
  {
    Assert.assertEquals(
        startMetadata(ImmutableMap.of(1, 3L)),
        START1.minus(START3)
    );

    Assert.assertEquals(
        startMetadata(ImmutableMap.of()),
        START0.minus(START2)
    );

    Assert.assertEquals(
        startMetadata(ImmutableMap.of()),
        START1.minus(START2)
    );

    Assert.assertEquals(
        startMetadata(ImmutableMap.of(2, 5L)),
        START2.minus(START1)
    );

    Assert.assertEquals(
        startMetadata(ImmutableMap.of()),
        START2.minus(START2)
    );

    Assert.assertEquals(
        endMetadata(ImmutableMap.of(1, 4L)),
        END2.minus(END1)
    );

    Assert.assertEquals(
        endMetadata(ImmutableMap.of(2, 5L)),
        END1.minus(END2)
    );
  }

  @Test
  public void testKafkaDataSourceMetadataSerdeRoundTrip() throws JsonProcessingException
  {
    ObjectMapper jsonMapper = createObjectMapper();

    KafkaDataSourceMetadata kdm1 = endMetadata(ImmutableMap.of());
    String kdmStr1 = jsonMapper.writeValueAsString(kdm1);
    DataSourceMetadata dsMeta1 = jsonMapper.readValue(kdmStr1, DataSourceMetadata.class);
    Assert.assertEquals(kdm1, dsMeta1);

    KafkaDataSourceMetadata kdm2 = endMetadata(ImmutableMap.of(1, 3L));
    String kdmStr2 = jsonMapper.writeValueAsString(kdm2);
    DataSourceMetadata dsMeta2 = jsonMapper.readValue(kdmStr2, DataSourceMetadata.class);
    Assert.assertEquals(kdm2, dsMeta2);
  }

  @Test
  public void testKafkaDataSourceMetadataSerde() throws JsonProcessingException
  {
    ObjectMapper jsonMapper = createObjectMapper();
    KafkaDataSourceMetadata expectedKdm1 = endMetadata(ImmutableMap.of(1, 3L));
    String kdmStr1 = "{\"type\":\"kafka\",\"partitions\":{\"type\":\"end\",\"stream\":\"foo\",\"topic\":\"foo\",\"partitionSequenceNumberMap\":{\"1\":3},\"partitionOffsetMap\":{\"1\":3},\"exclusivePartitions\":[]}}\n";
    DataSourceMetadata dsMeta1 = jsonMapper.readValue(kdmStr1, DataSourceMetadata.class);
    Assert.assertEquals(dsMeta1, expectedKdm1);

    KafkaDataSourceMetadata expectedKdm2 = endMetadata(ImmutableMap.of(1, 3L, 2, 1900L));
    String kdmStr2 = "{\"type\":\"kafka\",\"partitions\":{\"type\":\"end\",\"stream\":\"foo\",\"topic\":\"food\",\"partitionSequenceNumberMap\":{\"1\":3, \"2\":1900},\"partitionOffsetMap\":{\"1\":3, \"2\":1900},\"exclusivePartitions\":[]}}\n";
    DataSourceMetadata dsMeta2 = jsonMapper.readValue(kdmStr2, DataSourceMetadata.class);
    Assert.assertEquals(dsMeta2, expectedKdm2);
  }

  private static KafkaDataSourceMetadata startMetadata(Map<Integer, Long> offsets)
  {
    Map<KafkaTopicPartition, Long> newOffsets = CollectionUtils.mapKeys(
        offsets,
        k -> new KafkaTopicPartition(
            false,
            "foo",
            k
        )
    );
    return new KafkaDataSourceMetadata(new SeekableStreamStartSequenceNumbers<>("foo", newOffsets, ImmutableSet.of()));
  }

  private static KafkaDataSourceMetadata endMetadata(Map<Integer, Long> offsets)
  {
    Map<KafkaTopicPartition, Long> newOffsets = CollectionUtils.mapKeys(
        offsets,
        k -> new KafkaTopicPartition(
            false,
            "foo",
            k
        )
    );
    return new KafkaDataSourceMetadata(new SeekableStreamEndSequenceNumbers<>("foo", newOffsets));
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
        ).build();

    ObjectMapper objectMapper = injector.getInstance(ObjectMapper.class);
    module.getJacksonModules().forEach(objectMapper::registerModule);
    return objectMapper;
  }
}
