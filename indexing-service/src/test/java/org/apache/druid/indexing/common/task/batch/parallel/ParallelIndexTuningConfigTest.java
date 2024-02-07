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

package org.apache.druid.indexing.common.task.batch.parallel;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.indexer.partitions.DynamicPartitionsSpec;
import org.apache.druid.indexer.partitions.HashedPartitionsSpec;
import org.apache.druid.indexer.partitions.SingleDimensionPartitionsSpec;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.data.CompressionFactory.LongEncodingStrategy;
import org.apache.druid.segment.data.CompressionStrategy;
import org.apache.druid.segment.data.RoaringBitmapSerdeFactory;
import org.apache.druid.segment.indexing.TuningConfig;
import org.apache.druid.segment.writeout.OffHeapMemorySegmentWriteOutMediumFactory;
import org.joda.time.Duration;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;

public class ParallelIndexTuningConfigTest
{
  private final ObjectMapper mapper = new DefaultObjectMapper();

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Before
  public void setup()
  {
    mapper.registerSubtypes(new NamedType(ParallelIndexTuningConfig.class, "index_parallel"));
  }

  @Test
  public void testSerdeDefault() throws IOException
  {
    final ParallelIndexTuningConfig tuningConfig = ParallelIndexTuningConfig.defaultConfig();
    final byte[] json = mapper.writeValueAsBytes(tuningConfig);
    final ParallelIndexTuningConfig fromJson = (ParallelIndexTuningConfig) mapper.readValue(json, TuningConfig.class);
    Assert.assertEquals(fromJson, tuningConfig);
  }

  @Test
  public void testSerdeWithMaxRowsPerSegment()
      throws IOException
  {
    final ParallelIndexTuningConfig tuningConfig = new ParallelIndexTuningConfig(
        null,
        null,
        null,
        10,
        1000L,
        null,
        null,
        null,
        null,
        new DynamicPartitionsSpec(100, 100L),
        IndexSpec.builder()
                 .withBitmapSerdeFactory(RoaringBitmapSerdeFactory.getInstance())
                 .withDimensionCompression(CompressionStrategy.UNCOMPRESSED)
                 .withMetricCompression(CompressionStrategy.LZF)
                 .withLongEncoding(LongEncodingStrategy.LONGS)
                 .build(),
        IndexSpec.DEFAULT,
        1,
        false,
        true,
        10000L,
        OffHeapMemorySegmentWriteOutMediumFactory.instance(),
        null,
        250,
        100,
        20L,
        new Duration(3600),
        128,
        null,
        null,
        false,
        null,
        null,
        null,
        null,
        null,
        2
    );
    final byte[] json = mapper.writeValueAsBytes(tuningConfig);
    final ParallelIndexTuningConfig fromJson = (ParallelIndexTuningConfig) mapper.readValue(json, TuningConfig.class);
    Assert.assertEquals(fromJson, tuningConfig);
  }

  @Test
  public void testSerdeWithMaxNumConcurrentSubTasks() throws IOException
  {
    final int maxNumConcurrentSubTasks = 250;
    final ParallelIndexTuningConfig tuningConfig = new ParallelIndexTuningConfig(
        null,
        null,
        null,
        10,
        1000L,
        null,
        null,
        null,
        null,
        new DynamicPartitionsSpec(100, 100L),
        IndexSpec.builder()
                 .withBitmapSerdeFactory(RoaringBitmapSerdeFactory.getInstance())
                 .withDimensionCompression(CompressionStrategy.UNCOMPRESSED)
                 .withMetricCompression(CompressionStrategy.LZF)
                 .withLongEncoding(LongEncodingStrategy.LONGS)
                 .build(),
        IndexSpec.DEFAULT,
        1,
        false,
        true,
        10000L,
        OffHeapMemorySegmentWriteOutMediumFactory.instance(),
        null,
        maxNumConcurrentSubTasks,
        100,
        20L,
        new Duration(3600),
        128,
        null,
        null,
        false,
        null,
        null,
        null,
        null,
        null,
        2
    );
    final byte[] json = mapper.writeValueAsBytes(tuningConfig);
    final ParallelIndexTuningConfig fromJson = (ParallelIndexTuningConfig) mapper.readValue(json, TuningConfig.class);
    Assert.assertEquals(fromJson, tuningConfig);
  }

  @Test
  public void testSerdeWithMaxNumSubTasks() throws IOException
  {
    final int maxNumSubTasks = 250;
    final ParallelIndexTuningConfig tuningConfig = new ParallelIndexTuningConfig(
        null,
        null,
        null,
        10,
        1000L,
        null,
        null,
        null,
        null,
        new DynamicPartitionsSpec(100, 100L),
        IndexSpec.builder()
                 .withBitmapSerdeFactory(RoaringBitmapSerdeFactory.getInstance())
                 .withDimensionCompression(CompressionStrategy.UNCOMPRESSED)
                 .withMetricCompression(CompressionStrategy.LZF)
                 .withLongEncoding(LongEncodingStrategy.LONGS)
                 .build(),
        IndexSpec.DEFAULT,
        1,
        false,
        true,
        10000L,
        OffHeapMemorySegmentWriteOutMediumFactory.instance(),
        maxNumSubTasks,
        null,
        100,
        20L,
        new Duration(3600),
        128,
        null,
        null,
        false,
        null,
        null,
        null,
        null,
        null,
        2
    );
    final byte[] json = mapper.writeValueAsBytes(tuningConfig);
    final ParallelIndexTuningConfig fromJson = (ParallelIndexTuningConfig) mapper.readValue(json, TuningConfig.class);
    Assert.assertEquals(fromJson, tuningConfig);
  }

  @Test
  public void testSerdeWithMaxNumSubTasksAndMaxNumConcurrentSubTasks()
  {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("Can't use both maxNumSubTasks and maxNumConcurrentSubTasks");
    final int maxNumSubTasks = 250;
    final ParallelIndexTuningConfig tuningConfig = new ParallelIndexTuningConfig(
        null,
        null,
        null,
        10,
        1000L,
        null,
        null,
        null,
        null,
        new DynamicPartitionsSpec(100, 100L),
        IndexSpec.builder()
                 .withBitmapSerdeFactory(RoaringBitmapSerdeFactory.getInstance())
                 .withDimensionCompression(CompressionStrategy.UNCOMPRESSED)
                 .withMetricCompression(CompressionStrategy.LZF)
                 .withLongEncoding(LongEncodingStrategy.LONGS)
                 .build(),
        IndexSpec.DEFAULT,
        1,
        false,
        true,
        10000L,
        OffHeapMemorySegmentWriteOutMediumFactory.instance(),
        maxNumSubTasks,
        maxNumSubTasks,
        100,
        20L,
        new Duration(3600),
        128,
        null,
        null,
        false,
        null,
        null,
        null,
        null,
        null,
        null
    );
  }

  @Test
  public void testConstructorWithHashedPartitionsSpecAndNonForceGuaranteedRollupFailToCreate()
  {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("DynamicPartitionsSpec must be used for best-effort rollup");
    final boolean forceGuaranteedRollup = false;
    new ParallelIndexTuningConfig(
        null,
        null,
        null,
        10,
        1000L,
        null,
        null,
        null,
        null,
        new HashedPartitionsSpec(null, 10, null),
        IndexSpec.builder()
                 .withBitmapSerdeFactory(RoaringBitmapSerdeFactory.getInstance())
                 .withDimensionCompression(CompressionStrategy.UNCOMPRESSED)
                 .withMetricCompression(CompressionStrategy.LZF)
                 .withLongEncoding(LongEncodingStrategy.LONGS)
                 .build(),
        IndexSpec.DEFAULT,
        1,
        forceGuaranteedRollup,
        true,
        10000L,
        OffHeapMemorySegmentWriteOutMediumFactory.instance(),
        null,
        10,
        100,
        20L,
        new Duration(3600),
        128,
        null,
        null,
        false,
        null,
        null,
        null,
        null,
        null,
        null
    );
  }

  @Test
  public void testConstructorWithSingleDimensionPartitionsSpecAndNonForceGuaranteedRollupFailToCreate()
  {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("DynamicPartitionsSpec must be used for best-effort rollup");
    final boolean forceGuaranteedRollup = false;
    new ParallelIndexTuningConfig(
        null,
        null,
        null,
        10,
        1000L,
        null,
        null,
        null,
        null,
        new SingleDimensionPartitionsSpec(100, null, "dim", false),
        IndexSpec.builder()
                 .withBitmapSerdeFactory(RoaringBitmapSerdeFactory.getInstance())
                 .withDimensionCompression(CompressionStrategy.UNCOMPRESSED)
                 .withMetricCompression(CompressionStrategy.LZF)
                 .withLongEncoding(LongEncodingStrategy.LONGS)
                 .build(),
        IndexSpec.DEFAULT,
        1,
        forceGuaranteedRollup,
        true,
        10000L,
        OffHeapMemorySegmentWriteOutMediumFactory.instance(),
        null,
        10,
        100,
        20L,
        new Duration(3600),
        128,
        null,
        null,
        false,
        null,
        null,
        null,
        null,
        null,
        null
    );
  }

  @Test
  public void testConstructorWithDynamicPartitionsSpecAndForceGuaranteedRollupFailToCreate()
  {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("cannot be used for perfect rollup");
    final boolean forceGuaranteedRollup = true;
    new ParallelIndexTuningConfig(
        null,
        null,
        null,
        10,
        1000L,
        null,
        null,
        null,
        null,
        new DynamicPartitionsSpec(100, null),
        IndexSpec.builder()
                 .withBitmapSerdeFactory(RoaringBitmapSerdeFactory.getInstance())
                 .withDimensionCompression(CompressionStrategy.UNCOMPRESSED)
                 .withMetricCompression(CompressionStrategy.LZF)
                 .withLongEncoding(LongEncodingStrategy.LONGS)
                 .build(),
        IndexSpec.DEFAULT,
        1,
        forceGuaranteedRollup,
        true,
        10000L,
        OffHeapMemorySegmentWriteOutMediumFactory.instance(),
        null,
        10,
        100,
        20L,
        new Duration(3600),
        128,
        null,
        null,
        false,
        null,
        null,
        null,
        null,
        null,
        null
    );
  }

  @Test
  public void testEqualsAndHashCode()
  {
    EqualsVerifier.forClass(ParallelIndexTuningConfig.class)
                  .usingGetClass()
                  .withPrefabValues(
                      IndexSpec.class,
                      IndexSpec.DEFAULT,
                      IndexSpec.builder().withDimensionCompression(CompressionStrategy.ZSTD).build()
                  )
                  .verify();
  }
}
