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

package org.apache.druid.server.compaction;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.client.indexing.ClientCompactionTaskQueryTuningConfig;
import org.apache.druid.indexer.partitions.DimensionRangePartitionsSpec;
import org.apache.druid.indexer.partitions.DynamicPartitionsSpec;
import org.apache.druid.indexer.partitions.HashedPartitionsSpec;
import org.apache.druid.indexer.partitions.PartitionsSpec;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.segment.indexing.granularity.GranularitySpec;
import org.apache.druid.segment.indexing.granularity.UniformGranularitySpec;
import org.apache.druid.server.coordinator.DataSourceCompactionConfig;
import org.apache.druid.server.coordinator.UserCompactionTaskGranularityConfig;
import org.apache.druid.server.coordinator.UserCompactionTaskQueryTuningConfig;
import org.apache.druid.timeline.CompactionState;
import org.apache.druid.timeline.DataSegment;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

public class CompactionStatusTest
{
  private static final ObjectMapper OBJECT_MAPPER = new DefaultObjectMapper();
  private static final String DS_WIKI = "wiki";

  @Test
  public void testFindPartitionsSpecWhenGivenIsNull()
  {
    final ClientCompactionTaskQueryTuningConfig tuningConfig
        = ClientCompactionTaskQueryTuningConfig.from(null);
    Assert.assertEquals(
        new DynamicPartitionsSpec(null, Long.MAX_VALUE),
        CompactionStatus.findPartitionsSpecFromConfig(tuningConfig)
    );
  }

  @Test
  public void testFindPartitionsSpecWhenGivenIsDynamicWithNullMaxTotalRows()
  {
    final PartitionsSpec partitionsSpec = new DynamicPartitionsSpec(null, null);
    final ClientCompactionTaskQueryTuningConfig tuningConfig
        = ClientCompactionTaskQueryTuningConfig.from(createCompactionConfig(partitionsSpec));
    Assert.assertEquals(
        new DynamicPartitionsSpec(null, Long.MAX_VALUE),
        CompactionStatus.findPartitionsSpecFromConfig(tuningConfig)
    );
  }

  @Test
  public void testFindPartitionsSpecWhenGivenIsDynamicWithMaxTotalRows()
  {
    final PartitionsSpec partitionsSpec = new DynamicPartitionsSpec(null, 1000L);
    final ClientCompactionTaskQueryTuningConfig tuningConfig
        = ClientCompactionTaskQueryTuningConfig.from(createCompactionConfig(partitionsSpec));
    Assert.assertEquals(
        partitionsSpec,
        CompactionStatus.findPartitionsSpecFromConfig(tuningConfig)
    );
  }

  @Test
  public void testFindPartitionsSpecWhenGivenIsDynamicWithMaxRowsPerSegment()
  {
    final PartitionsSpec partitionsSpec = new DynamicPartitionsSpec(100, 1000L);
    final ClientCompactionTaskQueryTuningConfig tuningConfig
        = ClientCompactionTaskQueryTuningConfig.from(createCompactionConfig(partitionsSpec));
    Assert.assertEquals(
        partitionsSpec,
        CompactionStatus.findPartitionsSpecFromConfig(tuningConfig)
    );
  }

  @Test
  public void testFindPartitionsSpecFromConfigWithDeprecatedMaxRowsPerSegmentAndMaxTotalRowsReturnGivenValues()
  {
    final DataSourceCompactionConfig config = new DataSourceCompactionConfig(
        "datasource",
        null,
        null,
        100,
        null,
        new UserCompactionTaskQueryTuningConfig(
            null,
            null,
            null,
            1000L,
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
            null,
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
        null
    );
    Assert.assertEquals(
        new DynamicPartitionsSpec(100, 1000L),
        CompactionStatus.findPartitionsSpecFromConfig(
            ClientCompactionTaskQueryTuningConfig.from(config)
        )
    );
  }

  @Test
  public void testFindPartitionsSpecWhenGivenIsHashed()
  {
    final PartitionsSpec partitionsSpec =
        new HashedPartitionsSpec(null, 100, Collections.singletonList("dim"));
    final ClientCompactionTaskQueryTuningConfig tuningConfig
        = ClientCompactionTaskQueryTuningConfig.from(createCompactionConfig(partitionsSpec));
    Assert.assertEquals(
        partitionsSpec,
        CompactionStatus.findPartitionsSpecFromConfig(tuningConfig)
    );
  }

  @Test
  public void testFindPartitionsSpecWhenGivenIsRange()
  {
    final PartitionsSpec partitionsSpec =
        new DimensionRangePartitionsSpec(null, 10000, Collections.singletonList("dim"), false);
    final ClientCompactionTaskQueryTuningConfig tuningConfig
        = ClientCompactionTaskQueryTuningConfig.from(createCompactionConfig(partitionsSpec));
    Assert.assertEquals(
        partitionsSpec,
        CompactionStatus.findPartitionsSpecFromConfig(tuningConfig)
    );
  }

  @Test
  public void testStatusOnSegmentGranularityMismatch()
  {
    final GranularitySpec segmentGranularitySpec
        = new UniformGranularitySpec(Granularities.HOUR, null, null, null);
    final CompactionState segmentLastCompactionState = new CompactionState(
        null,
        null,
        null,
        null,
        null,
        segmentGranularitySpec.asMap(OBJECT_MAPPER)
    );
    final DataSegment segment
        = DataSegment.builder()
                     .dataSource(DS_WIKI)
                     .interval(Intervals.of("2013-01-01/PT1H"))
                     .size(100_000_000L)
                     .version("v1")
                     .lastCompactionState(segmentLastCompactionState)
                     .build();

    final CompactionStatus status = CompactionStatus.compute(
        SegmentsToCompact.from(Collections.singletonList(segment)),
        createCompactionConfig(Granularities.DAY),
        OBJECT_MAPPER
    );

    Assert.assertFalse(status.isComplete());
    Assert.assertEquals(
        "segmentGranularity",
        status.getReason()
    );
  }

  private static DataSourceCompactionConfig createCompactionConfig(
      Granularity segmentGranularity
  )
  {
    return new DataSourceCompactionConfig(
        DS_WIKI,
        null, null, null, null,
        createTuningConfig(
            new DimensionRangePartitionsSpec(1_000_000, null, Arrays.asList("countryName", "cityName"), false)
        ),
        new UserCompactionTaskGranularityConfig(segmentGranularity, null, null),
        null, null, null, null, null, null
    );
  }

  private static DataSourceCompactionConfig createCompactionConfig(
      PartitionsSpec partitionsSpec
  )
  {
    return new DataSourceCompactionConfig(
        DS_WIKI,
        null, null, null, null, createTuningConfig(partitionsSpec),
        null, null, null, null, null, null, null
    );
  }

  private static UserCompactionTaskQueryTuningConfig createTuningConfig(
      PartitionsSpec partitionsSpec
  )
  {
    return new UserCompactionTaskQueryTuningConfig(
        null,
        null, null, null, null, partitionsSpec, null, null, null,
        null, null, null, null, null, null, null, null, null, null
    );
  }
}
