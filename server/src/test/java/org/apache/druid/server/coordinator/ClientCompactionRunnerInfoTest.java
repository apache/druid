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

package org.apache.druid.server.coordinator;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.data.input.SegmentsSplitHintSpec;
import org.apache.druid.indexer.CompactionEngine;
import org.apache.druid.indexer.partitions.DimensionRangePartitionsSpec;
import org.apache.druid.indexer.partitions.DynamicPartitionsSpec;
import org.apache.druid.indexer.partitions.HashedPartitionsSpec;
import org.apache.druid.indexer.partitions.PartitionsSpec;
import org.apache.druid.java.util.common.HumanReadableBytes;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.data.CompressionFactory;
import org.apache.druid.segment.data.CompressionStrategy;
import org.apache.druid.segment.writeout.TmpFileSegmentWriteOutMediumFactory;
import org.joda.time.Duration;
import org.joda.time.Period;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.Map;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ClientCompactionRunnerInfoTest
{
  @Test
  public void testHashedPartitionsSpecs()
  {
    assertFalse(
        ClientCompactionRunnerInfo.supportsCompactionConfig(
            createCompactionConfig(new HashedPartitionsSpec(100, null, null), Collections.emptyMap())
        ).isValid()
    );
  }

  @Test
  public void testrInvalidDynamicPartitionsSpecs()
  {
    assertFalse(
        ClientCompactionRunnerInfo.supportsCompactionConfig(
            createCompactionConfig(new DynamicPartitionsSpec(100, 100L), Collections.emptyMap())
        ).isValid()
    );
  }

  @Test
  public void testDynamicPartitionsSpecs()
  {
    assertTrue(ClientCompactionRunnerInfo.supportsCompactionConfig(
        createCompactionConfig(new DynamicPartitionsSpec(100, null), Collections.emptyMap())
    ).isValid());
  }

  @Test
  public void testDimensionRangePartitionsSpecs()
  {
    assertTrue(ClientCompactionRunnerInfo.supportsCompactionConfig(
        createCompactionConfig(
            new DimensionRangePartitionsSpec(100, null, ImmutableList.of("partitionDim"), false),
            Collections.emptyMap()
        )
    ).isValid());
  }

  @Test
  public void testWithFinalizeAggregationsFalse()
  {
    DataSourceCompactionConfig compactionConfig = createCompactionConfig(
        new DynamicPartitionsSpec(3, null),
        ImmutableMap.of(ClientCompactionRunnerInfo.MSQContext.CTX_FINALIZE_AGGREGATIONS, false)
    );
    Assert.assertFalse(ClientCompactionRunnerInfo.supportsCompactionConfig(compactionConfig).isValid());
  }

  private static DataSourceCompactionConfig createCompactionConfig(
      PartitionsSpec partitionsSpec,
      Map<String, Object> context
  )
  {
    final DataSourceCompactionConfig config = new DataSourceCompactionConfig(
        "dataSource",
        null,
        500L,
        10000,
        new Period(3600),
        createTuningConfig(partitionsSpec),
        null,
        null,
        null,
        null,
        null,
        CompactionEngine.MSQ,
        context
    );
    return config;
  }

  private static UserCompactionTaskQueryTuningConfig createTuningConfig(PartitionsSpec partitionsSpec)
  {
    final UserCompactionTaskQueryTuningConfig tuningConfig = new UserCompactionTaskQueryTuningConfig(
        40000,
        null,
        2000L,
        null,
        new SegmentsSplitHintSpec(new HumanReadableBytes(100000L), null),
        partitionsSpec,
        IndexSpec.builder()
                 .withDimensionCompression(CompressionStrategy.LZ4)
                 .withMetricCompression(CompressionStrategy.LZF)
                 .withLongEncoding(CompressionFactory.LongEncodingStrategy.LONGS)
                 .build(),
        IndexSpec.builder()
                 .withDimensionCompression(CompressionStrategy.LZ4)
                 .withMetricCompression(CompressionStrategy.UNCOMPRESSED)
                 .withLongEncoding(CompressionFactory.LongEncodingStrategy.AUTO)
                 .build(),
        2,
        1000L,
        TmpFileSegmentWriteOutMediumFactory.instance(),
        100,
        5,
        1000L,
        new Duration(3000L),
        7,
        1000,
        100,
        2
    );
    return tuningConfig;
  }
}
