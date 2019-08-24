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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.curator.test.TestingCluster;
import org.apache.druid.client.cache.MapCache;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.FloatDimensionSchema;
import org.apache.druid.data.input.impl.JSONParseSpec;
import org.apache.druid.data.input.impl.LongDimensionSchema;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.data.input.impl.StringInputRowParser;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.indexing.kafka.supervisor.KafkaSupervisorIOConfig;
import org.apache.druid.indexing.kafka.supervisor.KafkaSupervisorSpec;
import org.apache.druid.indexing.kafka.test.TestBroker;
import org.apache.druid.indexing.overlord.sampler.FirehoseSampler;
import org.apache.druid.indexing.overlord.sampler.SamplerCache;
import org.apache.druid.indexing.overlord.sampler.SamplerConfig;
import org.apache.druid.indexing.overlord.sampler.SamplerResponse;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.parsers.JSONPathSpec;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.DoubleSumAggregatorFactory;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.indexing.granularity.UniformGranularitySpec;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class KafkaSamplerSpecTest
{
  private static final ObjectMapper OBJECT_MAPPER = TestHelper.makeJsonMapper();
  private static final String TOPIC = "sampling";
  private static final DataSchema DATA_SCHEMA = new DataSchema(
      "test_ds",
      OBJECT_MAPPER.convertValue(
          new StringInputRowParser(
              new JSONParseSpec(
                  new TimestampSpec("timestamp", "iso", null),
                  new DimensionsSpec(
                      Arrays.asList(
                          new StringDimensionSchema("dim1"),
                          new StringDimensionSchema("dim1t"),
                          new StringDimensionSchema("dim2"),
                          new LongDimensionSchema("dimLong"),
                          new FloatDimensionSchema("dimFloat")
                      ),
                      null,
                      null
                  ),
                  new JSONPathSpec(true, ImmutableList.of()),
                  ImmutableMap.of()
              ),
              StandardCharsets.UTF_8.name()
          ),
          Map.class
      ),
      new AggregatorFactory[]{
          new DoubleSumAggregatorFactory("met1sum", "met1"),
          new CountAggregatorFactory("rows")
      },
      new UniformGranularitySpec(Granularities.DAY, Granularities.NONE, null),
      null,
      OBJECT_MAPPER
  );

  private static TestingCluster zkServer;
  private static TestBroker kafkaServer;

  private static List<ProducerRecord<byte[], byte[]>> generateRecords(String topic)
  {
    return ImmutableList.of(
        new ProducerRecord<>(topic, 0, null, jb("2008", "a", "y", "10", "20.0", "1.0")),
        new ProducerRecord<>(topic, 0, null, jb("2009", "b", "y", "10", "20.0", "1.0")),
        new ProducerRecord<>(topic, 0, null, jb("2010", "c", "y", "10", "20.0", "1.0")),
        new ProducerRecord<>(topic, 0, null, jb("246140482-04-24T15:36:27.903Z", "x", "z", "10", "20.0", "1.0")),
        new ProducerRecord<>(topic, 0, null, StringUtils.toUtf8("unparseable")),
        new ProducerRecord<>(topic, 0, null, null)
    );
  }

  @BeforeClass
  public static void setupClass() throws Exception
  {
    zkServer = new TestingCluster(1);
    zkServer.start();

    kafkaServer = new TestBroker(zkServer.getConnectString(), null, 1, ImmutableMap.of("num.partitions", "2"));
    kafkaServer.start();
  }

  @AfterClass
  public static void tearDownClass() throws Exception
  {
    kafkaServer.close();
    zkServer.stop();
  }

  @Test(timeout = 30_000L)
  public void testSample()
  {
    insertData(generateRecords(TOPIC));

    KafkaSupervisorSpec supervisorSpec = new KafkaSupervisorSpec(
        DATA_SCHEMA,
        null,
        new KafkaSupervisorIOConfig(
            TOPIC,
            null,
            null,
            null,
            kafkaServer.consumerProperties(),
            null,
            null,
            null,
            true,
            null,
            null,
            null
        ),
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null
    );

    KafkaSamplerSpec samplerSpec = new KafkaSamplerSpec(
        supervisorSpec,
        new SamplerConfig(5, null, null, null),
        new FirehoseSampler(OBJECT_MAPPER, new SamplerCache(MapCache.create(100000))),
        OBJECT_MAPPER
    );

    SamplerResponse response = samplerSpec.sample();

    Assert.assertNotNull(response.getCacheKey());
    Assert.assertEquals(5, (int) response.getNumRowsRead());
    Assert.assertEquals(3, (int) response.getNumRowsIndexed());
    Assert.assertEquals(5, response.getData().size());

    Iterator<SamplerResponse.SamplerResponseRow> it = response.getData().iterator();

    Assert.assertEquals(new SamplerResponse.SamplerResponseRow(
        "{\"timestamp\":\"2008\",\"dim1\":\"a\",\"dim2\":\"y\",\"dimLong\":\"10\",\"dimFloat\":\"20.0\",\"met1\":\"1.0\"}",
        ImmutableMap.<String, Object>builder()
            .put("__time", 1199145600000L)
            .put("dim1", "a")
            .put("dim2", "y")
            .put("dimLong", 10L)
            .put("dimFloat", 20.0F)
            .put("rows", 1L)
            .put("met1sum", 1.0)
            .build(),
        null,
        null
    ), it.next());
    Assert.assertEquals(new SamplerResponse.SamplerResponseRow(
        "{\"timestamp\":\"2009\",\"dim1\":\"b\",\"dim2\":\"y\",\"dimLong\":\"10\",\"dimFloat\":\"20.0\",\"met1\":\"1.0\"}",
        ImmutableMap.<String, Object>builder()
            .put("__time", 1230768000000L)
            .put("dim1", "b")
            .put("dim2", "y")
            .put("dimLong", 10L)
            .put("dimFloat", 20.0F)
            .put("rows", 1L)
            .put("met1sum", 1.0)
            .build(),
        null,
        null
    ), it.next());
    Assert.assertEquals(new SamplerResponse.SamplerResponseRow(
        "{\"timestamp\":\"2010\",\"dim1\":\"c\",\"dim2\":\"y\",\"dimLong\":\"10\",\"dimFloat\":\"20.0\",\"met1\":\"1.0\"}",
        ImmutableMap.<String, Object>builder()
            .put("__time", 1262304000000L)
            .put("dim1", "c")
            .put("dim2", "y")
            .put("dimLong", 10L)
            .put("dimFloat", 20.0F)
            .put("rows", 1L)
            .put("met1sum", 1.0)
            .build(),
        null,
        null
    ), it.next());
    Assert.assertEquals(new SamplerResponse.SamplerResponseRow(
        "{\"timestamp\":\"246140482-04-24T15:36:27.903Z\",\"dim1\":\"x\",\"dim2\":\"z\",\"dimLong\":\"10\",\"dimFloat\":\"20.0\",\"met1\":\"1.0\"}",
        null,
        true,
        "Timestamp cannot be represented as a long: [MapBasedInputRow{timestamp=246140482-04-24T15:36:27.903Z, event={timestamp=246140482-04-24T15:36:27.903Z, dim1=x, dim2=z, dimLong=10, dimFloat=20.0, met1=1.0}, dimensions=[dim1, dim1t, dim2, dimLong, dimFloat]}]"
    ), it.next());
    Assert.assertEquals(new SamplerResponse.SamplerResponseRow(
        "unparseable",
        null,
        true,
        "Unable to parse row [unparseable]"
    ), it.next());

    Assert.assertFalse(it.hasNext());
  }

  private static void insertData(List<ProducerRecord<byte[], byte[]>> data)
  {
    try (final KafkaProducer<byte[], byte[]> kafkaProducer = kafkaServer.newProducer()) {
      kafkaProducer.initTransactions();
      kafkaProducer.beginTransaction();

      data.forEach(kafkaProducer::send);

      kafkaProducer.commitTransaction();
    }
  }

  private static byte[] jb(String timestamp, String dim1, String dim2, String dimLong, String dimFloat, String met1)
  {
    try {
      return new ObjectMapper().writeValueAsBytes(
          ImmutableMap.builder()
                      .put("timestamp", timestamp)
                      .put("dim1", dim1)
                      .put("dim2", dim2)
                      .put("dimLong", dimLong)
                      .put("dimFloat", dimFloat)
                      .put("met1", met1)
                      .build()
      );
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
