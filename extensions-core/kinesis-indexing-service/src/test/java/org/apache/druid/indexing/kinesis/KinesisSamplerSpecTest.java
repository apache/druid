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

package org.apache.druid.indexing.kinesis;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.client.indexing.SamplerResponse;
import org.apache.druid.common.aws.AWSCredentialsConfig;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.data.input.impl.ByteEntity;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.FloatDimensionSchema;
import org.apache.druid.data.input.impl.JsonInputFormat;
import org.apache.druid.data.input.impl.LongDimensionSchema;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.indexing.kinesis.supervisor.KinesisSupervisorIOConfig;
import org.apache.druid.indexing.kinesis.supervisor.KinesisSupervisorSpec;
import org.apache.druid.indexing.overlord.sampler.InputSourceSampler;
import org.apache.druid.indexing.overlord.sampler.SamplerConfig;
import org.apache.druid.indexing.overlord.sampler.SamplerTestUtils;
import org.apache.druid.indexing.seekablestream.common.OrderedPartitionableRecord;
import org.apache.druid.indexing.seekablestream.common.StreamPartition;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.parsers.JSONPathSpec;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.DoubleSumAggregatorFactory;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.indexing.granularity.UniformGranularitySpec;
import org.easymock.EasyMock;
import org.easymock.EasyMockSupport;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class KinesisSamplerSpecTest extends EasyMockSupport
{
  private static final String STREAM = "sampling";
  private static final String SHARD_ID = "1";
  private static final DataSchema DATA_SCHEMA = new DataSchema(
      "test_ds",
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
      new AggregatorFactory[]{
          new DoubleSumAggregatorFactory("met1sum", "met1"),
          new CountAggregatorFactory("rows")
      },
      new UniformGranularitySpec(Granularities.DAY, Granularities.NONE, null),
      null
  );

  static {
    NullHandling.initializeForTests();
  }

  private final KinesisRecordSupplier recordSupplier = mock(KinesisRecordSupplier.class);

  private static List<OrderedPartitionableRecord<String, String, ByteEntity>> generateRecords(String stream)
  {
    return ImmutableList.of(
        new OrderedPartitionableRecord<>(stream, "1", "0", jb("2008", "a", "y", "10", "20.0", "1.0")),
        new OrderedPartitionableRecord<>(stream, "1", "1", jb("2009", "b", "y", "10", "20.0", "1.0")),
        new OrderedPartitionableRecord<>(stream, "1", "2", jb("2010", "c", "y", "10", "20.0", "1.0")),
        new OrderedPartitionableRecord<>(
            stream,
            "1",
            "5",
            jb("246140482-04-24T15:36:27.903Z", "x", "z", "10", "20.0", "1.0")
        ),
        new OrderedPartitionableRecord<>(
            stream,
            "1",
            "6",
            Collections.singletonList(new ByteEntity(StringUtils.toUtf8("unparseable")))
        ),
        new OrderedPartitionableRecord<>(stream, "1", "8", Collections.singletonList(new ByteEntity(StringUtils.toUtf8("{}"))))
    );
  }

  @Test(timeout = 10_000L)
  public void testSample() throws Exception
  {
    EasyMock.expect(recordSupplier.getPartitionIds(STREAM)).andReturn(ImmutableSet.of(SHARD_ID)).once();

    recordSupplier.assign(ImmutableSet.of(StreamPartition.of(STREAM, SHARD_ID)));
    EasyMock.expectLastCall().once();

    recordSupplier.seekToEarliest(ImmutableSet.of(StreamPartition.of(STREAM, SHARD_ID)));
    EasyMock.expectLastCall().once();

    EasyMock.expect(recordSupplier.poll(EasyMock.anyLong())).andReturn(generateRecords(STREAM)).once();

    recordSupplier.close();
    EasyMock.expectLastCall().once();

    replayAll();

    KinesisSupervisorSpec supervisorSpec = new KinesisSupervisorSpec(
        null,
        DATA_SCHEMA,
        null,
        new KinesisSupervisorIOConfig(
            STREAM,
            new JsonInputFormat(new JSONPathSpec(true, ImmutableList.of()), ImmutableMap.of(), false),
            null,
            null,
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
        null,
        null
    );

    KinesisSamplerSpec samplerSpec = new TestableKinesisSamplerSpec(
        supervisorSpec,
        new SamplerConfig(5, null),
        new InputSourceSampler(),
        null
    );

    SamplerResponse response = samplerSpec.sample();

    verifyAll();

    Assert.assertEquals(5, response.getNumRowsRead());
    Assert.assertEquals(3, response.getNumRowsIndexed());
    Assert.assertEquals(5, response.getData().size());

    Iterator<SamplerResponse.SamplerResponseRow> it = response.getData().iterator();

    Assert.assertEquals(new SamplerResponse.SamplerResponseRow(
        ImmutableMap.<String, Object>builder()
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
        ImmutableMap.<String, Object>builder()
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
        ImmutableMap.<String, Object>builder()
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
        ImmutableMap.<String, Object>builder()
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
        "Unable to parse row [unparseable] into JSON"
    ), it.next());

    Assert.assertFalse(it.hasNext());
  }

  private static List<ByteEntity> jb(String ts, String dim1, String dim2, String dimLong, String dimFloat, String met1)
  {
    try {
      return Collections.singletonList(new ByteEntity(new ObjectMapper().writeValueAsBytes(
          ImmutableMap.builder()
                      .put("timestamp", ts)
                      .put("dim1", dim1)
                      .put("dim2", dim2)
                      .put("dimLong", dimLong)
                      .put("dimFloat", dimFloat)
                      .put("met1", met1)
                      .build()
      )));
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private class TestableKinesisSamplerSpec extends KinesisSamplerSpec
  {
    private TestableKinesisSamplerSpec(
        KinesisSupervisorSpec ingestionSpec,
        SamplerConfig samplerConfig,
        InputSourceSampler inputSourceSampler,
        AWSCredentialsConfig awsCredentialsConfig
    )
    {
      super(ingestionSpec, samplerConfig, inputSourceSampler, awsCredentialsConfig);
    }

    @Override
    protected KinesisRecordSupplier createRecordSupplier()
    {
      return recordSupplier;
    }
  }
}
