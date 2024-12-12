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

package org.apache.druid.indexing.common.task;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.google.common.collect.ImmutableList;
import org.apache.druid.indexer.partitions.DynamicPartitionsSpec;
import org.apache.druid.indexer.partitions.HashedPartitionsSpec;
import org.apache.druid.indexing.common.task.IndexTask.IndexTuningConfig;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.data.CompressionFactory.LongEncodingStrategy;
import org.apache.druid.segment.data.CompressionStrategy;
import org.apache.druid.segment.data.RoaringBitmapSerdeFactory;
import org.apache.druid.segment.indexing.TuningConfig;
import org.apache.druid.segment.writeout.OffHeapMemorySegmentWriteOutMediumFactory;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;

public class IndexTaskSerdeTest
{
  private static final ObjectMapper MAPPER = new DefaultObjectMapper();

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @BeforeClass
  public static void setup()
  {
    MAPPER.registerSubtypes(new NamedType(IndexTuningConfig.class, "index"));
  }

  @Test
  public void testSerdeTuningConfigWithDynamicPartitionsSpec() throws IOException
  {
    final IndexTuningConfig tuningConfig = TuningConfigBuilder
        .forIndexTask()
        .withPartitionsSpec(new DynamicPartitionsSpec(1000, 2000L))
        .withForceGuaranteedRollup(false)
        .build();
    assertSerdeTuningConfig(tuningConfig);
  }

  @Test
  public void testSerdeTuningConfigWithHashedPartitionsSpec() throws IOException
  {
    final IndexTuningConfig tuningConfig = TuningConfigBuilder
        .forIndexTask()
        .withPartitionsSpec(new HashedPartitionsSpec(null, 10, null))
        .withForceGuaranteedRollup(true)
        .build();
    assertSerdeTuningConfig(tuningConfig);
  }

  @Test
  public void testSerdeTuningConfigWithDeprecatedDynamicPartitionsSpec() throws IOException
  {
    final IndexTuningConfig tuningConfig = TuningConfigBuilder
        .forIndexTask()
        .withMaxRowsPerSegment(1000)
        .withMaxRowsInMemory(100)
        .withMaxBytesInMemory(2000L)
        .withMaxTotalRows(3000L)
        .withForceGuaranteedRollup(false)
        .withPushTimeout(100L)
        .withAwaitSegmentAvailabilityTimeoutMillis(1L)
        .build();
    assertSerdeTuningConfig(tuningConfig);
  }

  @Test
  public void testSerdeTuningConfigWithDeprecatedHashedPartitionsSpec() throws IOException
  {
    final IndexTuningConfig tuningConfig = new IndexTuningConfig(
        null,
        null,
        null,
        100,
        2000L,
        null,
        null,
        null,
        10,
        ImmutableList.of("dim1", "dim2"),
        null,
        IndexSpec.builder()
                 .withBitmapSerdeFactory(RoaringBitmapSerdeFactory.getInstance())
                 .withDimensionCompression(CompressionStrategy.LZ4)
                 .withMetricCompression(CompressionStrategy.LZF)
                 .withLongEncoding(LongEncodingStrategy.LONGS)
                 .build(),
        null,
        null,
        false,
        null,
        null,
        100L,
        OffHeapMemorySegmentWriteOutMediumFactory.instance(),
        true,
        10,
        100,
        1234,
        null,
        null
    );
    assertSerdeTuningConfig(tuningConfig);
  }

  @Test
  public void testForceGuaranteedRollupWithDynamicPartitionsSpec()
  {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("DynamicPartitionsSpec cannot be used for perfect rollup");
    TuningConfigBuilder.forIndexTask()
                       .withForceGuaranteedRollup(true)
                       .withPartitionsSpec(new DynamicPartitionsSpec(1000, 2000L))
                       .build();
  }

  @Test
  public void testBestEffortRollupWithHashedPartitionsSpec()
  {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("DynamicPartitionsSpec must be used for best-effort rollup");
    TuningConfigBuilder.forIndexTask()
                       .withForceGuaranteedRollup(false)
                       .withPartitionsSpec(new HashedPartitionsSpec(null, 10, null))
                       .build();
  }

  private static void assertSerdeTuningConfig(IndexTuningConfig tuningConfig) throws IOException
  {
    final byte[] json = MAPPER.writeValueAsBytes(tuningConfig);
    final IndexTuningConfig fromJson = (IndexTuningConfig) MAPPER.readValue(json, TuningConfig.class);
    Assert.assertEquals(tuningConfig, fromJson);
  }
}
