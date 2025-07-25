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
import org.apache.druid.client.indexing.SamplerResponse;
import org.apache.druid.client.indexing.SamplerSpec;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.FloatDimensionSchema;
import org.apache.druid.data.input.impl.InputRowParser;
import org.apache.druid.data.input.impl.JSONParseSpec;
import org.apache.druid.data.input.impl.JsonInputFormat;
import org.apache.druid.data.input.impl.LongDimensionSchema;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.data.input.impl.StringInputRowParser;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.data.input.kafkainput.KafkaInputFormat;
import org.apache.druid.indexer.granularity.UniformGranularitySpec;
import org.apache.druid.indexing.kafka.supervisor.KafkaSupervisorIOConfig;
import org.apache.druid.indexing.kafka.supervisor.KafkaSupervisorSpec;
import org.apache.druid.indexing.kafka.test.TestBroker;
import org.apache.druid.indexing.overlord.sampler.InputSourceSampler;
import org.apache.druid.indexing.overlord.sampler.SamplerConfig;
import org.apache.druid.indexing.overlord.sampler.SamplerException;
import org.apache.druid.indexing.overlord.sampler.SamplerTestUtils;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.parsers.JSONPathSpec;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.DoubleSumAggregatorFactory;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.server.security.Action;
import org.apache.druid.server.security.Resource;
import org.apache.druid.server.security.ResourceAction;
import org.apache.druid.server.security.ResourceType;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

public class KafkaSamplerSpecTest extends InitializedNullHandlingTest
{
  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  private static final ObjectMapper OBJECT_MAPPER = TestHelper.makeJsonMapper();
  private static final String TOPIC = "sampling";
  private static final DataSchema DATA_SCHEMA =
      DataSchema.builder()
                .withDataSource("test_ds")
                .withTimestamp(new TimestampSpec("timestamp", "iso", null))
                .withDimensions(
                    new StringDimensionSchema("dim1"),
                    new StringDimensionSchema("dim1t"),
                    new StringDimensionSchema("dim2"),
                    new LongDimensionSchema("dimLong"),
                    new FloatDimensionSchema("dimFloat")
                )
                .withAggregators(
                    new DoubleSumAggregatorFactory("met1sum", "met1"),
                    new CountAggregatorFactory("rows")
                )
                .withGranularity(
                    new UniformGranularitySpec(Granularities.DAY, Granularities.NONE, null)
                )
                .build();

  private static final DataSchema DATA_SCHEMA_KAFKA_TIMESTAMP =
      DataSchema.builder(DATA_SCHEMA)
                .withTimestamp(new TimestampSpec("kafka.timestamp", "iso", null))
                .build();

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

  @Test
  public void testSample()
  {
    insertData(generateRecords(TOPIC));

    KafkaSupervisorSpec supervisorSpec = new KafkaSupervisorSpec(
        null,
        null,
        DATA_SCHEMA,
        null,
        new KafkaSupervisorIOConfig(
            TOPIC,
            null,
            new JsonInputFormat(JSONPathSpec.DEFAULT, null, null, null, null),
            null,
            null,
            null,
            kafkaServer.consumerProperties(),
            null,
            null,
            null,
            null,
            null,
            true,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            false
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
        new SamplerConfig(5, 5_000, null, null),
        new InputSourceSampler(OBJECT_MAPPER),
        OBJECT_MAPPER
    );

    runSamplerAndCompareResponse(samplerSpec, true);
  }

  @Test
  public void testSampleWithTopicPattern()
  {
    insertData(generateRecords(TOPIC));

    KafkaSupervisorSpec supervisorSpec = new KafkaSupervisorSpec(
        null,
        null,
        DATA_SCHEMA,
        null,
        new KafkaSupervisorIOConfig(
            null,
            Pattern.quote(TOPIC),
            new JsonInputFormat(JSONPathSpec.DEFAULT, null, null, null, null),
            null,
            null,
            null,
            kafkaServer.consumerProperties(),
            null,
            null,
            null,
            null,
            null,
            true,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            false
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
        new SamplerConfig(5, 5_000, null, null),
        new InputSourceSampler(OBJECT_MAPPER),
        OBJECT_MAPPER
    );

    runSamplerAndCompareResponse(samplerSpec, true);
  }

  @Test
  public void testSampleKafkaInputFormat()
  {
    insertData(generateRecords(TOPIC));

    KafkaSupervisorSpec supervisorSpec = new KafkaSupervisorSpec(
        null,
        null,
        DATA_SCHEMA_KAFKA_TIMESTAMP,
        null,
        new KafkaSupervisorIOConfig(
            TOPIC,
            null,
            new KafkaInputFormat(
                null,
                null,
                new JsonInputFormat(JSONPathSpec.DEFAULT, null, null, null, null),
                null,
                null,
                null,
                null
            ),

            null,
            null,
            null,
            kafkaServer.consumerProperties(),
            null,
            null,
            null,
            null,
            null,
            true,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            false
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
        new SamplerConfig(5, 5_000, null, null),
        new InputSourceSampler(OBJECT_MAPPER),
        OBJECT_MAPPER
    );

    SamplerResponse response = samplerSpec.sample();

    Assert.assertEquals(5, response.getNumRowsRead());
    // we can parse an extra row compared to other generated data samples because we are using kafka timestamp
    // for timestamp
    Assert.assertEquals(4, response.getNumRowsIndexed());
    Assert.assertEquals(5, response.getData().size());

    Iterator<SamplerResponse.SamplerResponseRow> it = response.getData().iterator();

    SamplerResponse.SamplerResponseRow nextRow;
    Map<String, Object> rawInput;
    Map<String, Object> parsedInput;

    for (int i = 0; i < 4; i++) {
      nextRow = it.next();
      Assert.assertNull(nextRow.isUnparseable());
      rawInput = nextRow.getInput();
      parsedInput = nextRow.getParsed();
      Assert.assertTrue(rawInput.containsKey("kafka.timestamp"));
      Assert.assertEquals(rawInput.get("kafka.timestamp"), parsedInput.get("__time"));
    }
    nextRow = it.next();
    Assert.assertTrue(nextRow.isUnparseable());

    Assert.assertFalse(it.hasNext());
  }

  @Test
  public void testWithInputRowParser() throws IOException
  {
    insertData(generateRecords(TOPIC));

    ObjectMapper objectMapper = new DefaultObjectMapper();
    TimestampSpec timestampSpec = new TimestampSpec("timestamp", "iso", null);
    DimensionsSpec dimensionsSpec = new DimensionsSpec(
        Arrays.asList(
            new StringDimensionSchema("dim1"),
            new StringDimensionSchema("dim1t"),
            new StringDimensionSchema("dim2"),
            new LongDimensionSchema("dimLong"),
            new FloatDimensionSchema("dimFloat")
        )
    );
    InputRowParser parser = new StringInputRowParser(new JSONParseSpec(timestampSpec, dimensionsSpec, JSONPathSpec.DEFAULT, null, null), "UTF8");

    DataSchema dataSchema = DataSchema.builder()
                                      .withDataSource("test_ds")
                                      .withParserMap(
                                          objectMapper.readValue(objectMapper.writeValueAsBytes(parser), Map.class)
                                      )
                                      .withAggregators(
                                          new DoubleSumAggregatorFactory("met1sum", "met1"),
                                          new CountAggregatorFactory("rows")
                                      )
                                      .withGranularity(new UniformGranularitySpec(Granularities.DAY, Granularities.NONE, null))
                                      .withObjectMapper(objectMapper)
                                      .build();

    KafkaSupervisorSpec supervisorSpec = new KafkaSupervisorSpec(
        null,
        null,
        dataSchema,
        null,
        new KafkaSupervisorIOConfig(
            TOPIC,
            null,
            null,
            null,
            null,
            null,
            kafkaServer.consumerProperties(),
            null,
            null,
            null,
            null,
            null,
            true,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            false
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
        new SamplerConfig(5, 5_000, null, null),
        new InputSourceSampler(new DefaultObjectMapper()),
        OBJECT_MAPPER
    );

    runSamplerAndCompareResponse(samplerSpec, false);
  }

  private static void runSamplerAndCompareResponse(SamplerSpec samplerSpec, boolean useInputFormat)
  {
    SamplerResponse response = samplerSpec.sample();

    Assert.assertEquals(5, response.getNumRowsRead());
    Assert.assertEquals(3, response.getNumRowsIndexed());
    Assert.assertEquals(5, response.getData().size());

    Iterator<SamplerResponse.SamplerResponseRow> it = response.getData().iterator();

    Assert.assertEquals(new SamplerResponse.SamplerResponseRow(
        new SamplerTestUtils.MapAllowingNullValuesBuilder<String, Object>()
            .put("timestamp", "2008")
            .put("dim1", "a")
            .put("dim2", "y")
            .put("dimLong", "10")
            .put("dimFloat", "20.0")
            .put("met1", "1.0")
            .build(),
        new SamplerTestUtils.MapAllowingNullValuesBuilder<String, Object>()
            .put("__time", 1199145600000L)
            .put("dim1", "a")
            .put("dim1t", null)
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
        new SamplerTestUtils.MapAllowingNullValuesBuilder<String, Object>()
            .put("timestamp", "2009")
            .put("dim1", "b")
            .put("dim2", "y")
            .put("dimLong", "10")
            .put("dimFloat", "20.0")
            .put("met1", "1.0")
            .build(),
        new SamplerTestUtils.MapAllowingNullValuesBuilder<String, Object>()
            .put("__time", 1230768000000L)
            .put("dim1", "b")
            .put("dim1t", null)
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
        new SamplerTestUtils.MapAllowingNullValuesBuilder<String, Object>()
            .put("timestamp", "2010")
            .put("dim1", "c")
            .put("dim2", "y")
            .put("dimLong", "10")
            .put("dimFloat", "20.0")
            .put("met1", "1.0")
            .build(),
        new SamplerTestUtils.MapAllowingNullValuesBuilder<String, Object>()
            .put("__time", 1262304000000L)
            .put("dim1", "c")
            .put("dim1t", null)
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
        new SamplerTestUtils.MapAllowingNullValuesBuilder<String, Object>()
            .put("timestamp", "246140482-04-24T15:36:27.903Z")
            .put("dim1", "x")
            .put("dim2", "z")
            .put("dimLong", "10")
            .put("dimFloat", "20.0")
            .put("met1", "1.0")
            .build(),
        null,
        true,
        "Encountered row with timestamp[246140482-04-24T15:36:27.903Z] that cannot be represented as a long: [{timestamp=246140482-04-24T15:36:27.903Z, dim1=x, dim2=z, dimLong=10, dimFloat=20.0, met1=1.0}]"
    ), it.next());
    Assert.assertEquals(new SamplerResponse.SamplerResponseRow(
        null,
        null,
        true,
        "Unable to parse row [unparseable]" + (useInputFormat ? " into JSON" : "")
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

  @Test
  public void testInvalidKafkaConfig()
  {
    KafkaSupervisorSpec supervisorSpec = new KafkaSupervisorSpec(
        null,
        null,
        DATA_SCHEMA,
        null,
        new KafkaSupervisorIOConfig(
            TOPIC,
            null,
            new JsonInputFormat(JSONPathSpec.DEFAULT, null, null, null, null),
            null,
            null,
            null,

            // invalid bootstrap server
            ImmutableMap.of("bootstrap.servers", "127.0.0.1"),

            null,
            null,
            null,
            null,
            null,
            true,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            false
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
        new InputSourceSampler(OBJECT_MAPPER),
        OBJECT_MAPPER
    );

    expectedException.expect(SamplerException.class);
    expectedException.expectMessage("Invalid url in bootstrap.servers");
    samplerSpec.sample();
  }

  @Test
  public void testGetInputSourceResources()
  {
    KafkaSupervisorSpec supervisorSpec = new KafkaSupervisorSpec(
        null,
        null,
        DATA_SCHEMA,
        null,
        new KafkaSupervisorIOConfig(
            TOPIC,
            null,
            new JsonInputFormat(JSONPathSpec.DEFAULT, null, null, null, null),
            null,
            null,
            null,

            // invalid bootstrap server
            ImmutableMap.of("bootstrap.servers", "127.0.0.1"),

            null,
            null,
            null,
            null,
            null,
            true,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            false
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
        new InputSourceSampler(OBJECT_MAPPER),
        OBJECT_MAPPER
    );

    Assert.assertEquals(
        Collections.singleton(
            new ResourceAction(new Resource(
                KafkaIndexTaskModule.SCHEME,
                ResourceType.EXTERNAL
            ), Action.READ)),
        samplerSpec.getInputSourceResources()
    );
  }
}
