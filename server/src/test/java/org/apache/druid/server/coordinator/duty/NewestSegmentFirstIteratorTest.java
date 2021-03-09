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

package org.apache.druid.server.coordinator.duty;

import org.apache.druid.client.indexing.ClientCompactionTaskQueryTuningConfig;
import org.apache.druid.com.google.common.collect.ImmutableList;
import org.apache.druid.com.google.common.collect.Lists;
import org.apache.druid.indexer.partitions.DynamicPartitionsSpec;
import org.apache.druid.indexer.partitions.HashedPartitionsSpec;
import org.apache.druid.indexer.partitions.SingleDimensionPartitionsSpec;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.server.coordinator.DataSourceCompactionConfig;
import org.apache.druid.server.coordinator.UserCompactionTaskQueryTuningConfig;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class NewestSegmentFirstIteratorTest
{
  @Test
  public void testFilterSkipIntervals()
  {
    final Interval totalInterval = Intervals.of("2018-01-01/2019-01-01");
    final List<Interval> expectedSkipIntervals = ImmutableList.of(
        Intervals.of("2018-01-15/2018-03-02"),
        Intervals.of("2018-07-23/2018-10-01"),
        Intervals.of("2018-10-02/2018-12-25"),
        Intervals.of("2018-12-31/2019-01-01")
    );
    final List<Interval> skipIntervals = NewestSegmentFirstIterator.filterSkipIntervals(
        totalInterval,
        Lists.newArrayList(
            Intervals.of("2017-12-01/2018-01-15"),
            Intervals.of("2018-03-02/2018-07-23"),
            Intervals.of("2018-10-01/2018-10-02"),
            Intervals.of("2018-12-25/2018-12-31")
        )
    );

    Assert.assertEquals(expectedSkipIntervals, skipIntervals);
  }

  @Test
  public void testAddSkipIntervalFromLatestAndSort()
  {
    final List<Interval> expectedIntervals = ImmutableList.of(
        Intervals.of("2018-12-24/2018-12-25"),
        Intervals.of("2018-12-29/2019-01-01")
    );
    final List<Interval> fullSkipIntervals = NewestSegmentFirstIterator.sortAndAddSkipIntervalFromLatest(
        DateTimes.of("2019-01-01"),
        new Period(72, 0, 0, 0),
        null,
        ImmutableList.of(
            Intervals.of("2018-12-30/2018-12-31"),
            Intervals.of("2018-12-24/2018-12-25")
        )
    );

    Assert.assertEquals(expectedIntervals, fullSkipIntervals);
  }

  @Test
  public void testFindPartitionsSpecFromConfigWithNullTuningConfigReturnDynamicPartitinosSpecWithMaxTotalRowsOfLongMax()
  {
    final DataSourceCompactionConfig config = new DataSourceCompactionConfig(
        "datasource",
        null,
        null,
        null,
        null,
        null,
        null,
        null
    );
    Assert.assertEquals(
        new DynamicPartitionsSpec(null, Long.MAX_VALUE),
        NewestSegmentFirstIterator.findPartitinosSpecFromConfig(
            ClientCompactionTaskQueryTuningConfig.from(config.getTuningConfig(), config.getMaxRowsPerSegment())
        )
    );
  }

  @Test
  public void testFindPartitionsSpecFromConfigWithNullMaxTotalRowsReturnLongMaxValue()
  {
    final DataSourceCompactionConfig config = new DataSourceCompactionConfig(
        "datasource",
        null,
        null,
        null,
        null,
        new UserCompactionTaskQueryTuningConfig(
            null,
            null,
            null,
            null,
            new DynamicPartitionsSpec(null, null),
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
        null
    );
    Assert.assertEquals(
        new DynamicPartitionsSpec(null, Long.MAX_VALUE),
        NewestSegmentFirstIterator.findPartitinosSpecFromConfig(
            ClientCompactionTaskQueryTuningConfig.from(config.getTuningConfig(), config.getMaxRowsPerSegment())
        )
    );
  }

  @Test
  public void testFindPartitionsSpecFromConfigWithNonNullMaxTotalRowsReturnGivenValue()
  {
    final DataSourceCompactionConfig config = new DataSourceCompactionConfig(
        "datasource",
        null,
        null,
        null,
        null,
        new UserCompactionTaskQueryTuningConfig(
            null,
            null,
            null,
            null,
            new DynamicPartitionsSpec(null, 1000L),
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
        null
    );
    Assert.assertEquals(
        new DynamicPartitionsSpec(null, 1000L),
        NewestSegmentFirstIterator.findPartitinosSpecFromConfig(
            ClientCompactionTaskQueryTuningConfig.from(config.getTuningConfig(), config.getMaxRowsPerSegment())
        )
    );
  }

  @Test
  public void testFindPartitionsSpecFromConfigWithNonNullMaxRowsPerSegmentReturnGivenValue()
  {
    final DataSourceCompactionConfig config = new DataSourceCompactionConfig(
        "datasource",
        null,
        null,
        null,
        null,
        new UserCompactionTaskQueryTuningConfig(
            null,
            null,
            null,
            null,
            new DynamicPartitionsSpec(100, 1000L),
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
        null
    );
    Assert.assertEquals(
        new DynamicPartitionsSpec(100, 1000L),
        NewestSegmentFirstIterator.findPartitinosSpecFromConfig(
            ClientCompactionTaskQueryTuningConfig.from(config.getTuningConfig(), config.getMaxRowsPerSegment())
        )
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
            null
        ),
        null,
        null
    );
    Assert.assertEquals(
        new DynamicPartitionsSpec(100, 1000L),
        NewestSegmentFirstIterator.findPartitinosSpecFromConfig(
            ClientCompactionTaskQueryTuningConfig.from(config.getTuningConfig(), config.getMaxRowsPerSegment())
        )
    );
  }

  @Test
  public void testFindPartitionsSpecFromConfigWithDeprecatedMaxRowsPerSegmentAndPartitionsSpecIgnoreDeprecatedOne()
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
            null,
            new DynamicPartitionsSpec(null, null),
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
        null
    );
    Assert.assertEquals(
        new DynamicPartitionsSpec(null, Long.MAX_VALUE),
        NewestSegmentFirstIterator.findPartitinosSpecFromConfig(
            ClientCompactionTaskQueryTuningConfig.from(config.getTuningConfig(), config.getMaxRowsPerSegment())
        )
    );
  }

  @Test
  public void testFindPartitionsSpecFromConfigWithDeprecatedMaxTotalRowsAndPartitionsSpecIgnoreDeprecatedOne()
  {
    final DataSourceCompactionConfig config = new DataSourceCompactionConfig(
        "datasource",
        null,
        null,
        null,
        null,
        new UserCompactionTaskQueryTuningConfig(
            null,
            null,
            1000L,
            null,
            new DynamicPartitionsSpec(null, null),
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
        null
    );
    Assert.assertEquals(
        new DynamicPartitionsSpec(null, Long.MAX_VALUE),
        NewestSegmentFirstIterator.findPartitinosSpecFromConfig(
            ClientCompactionTaskQueryTuningConfig.from(config.getTuningConfig(), config.getMaxRowsPerSegment())
        )
    );
  }

  @Test
  public void testFindPartitionsSpecFromConfigWithHashPartitionsSpec()
  {
    final DataSourceCompactionConfig config = new DataSourceCompactionConfig(
        "datasource",
        null,
        null,
        null,
        null,
        new UserCompactionTaskQueryTuningConfig(
            null,
            null,
            null,
            null,
            new HashedPartitionsSpec(null, 10, ImmutableList.of("dim")),
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
        null
    );
    Assert.assertEquals(
        new HashedPartitionsSpec(null, 10, ImmutableList.of("dim")),
        NewestSegmentFirstIterator.findPartitinosSpecFromConfig(
            ClientCompactionTaskQueryTuningConfig.from(config.getTuningConfig(), config.getMaxRowsPerSegment())
        )
    );
  }

  @Test
  public void testFindPartitionsSpecFromConfigWithRangePartitionsSpec()
  {
    final DataSourceCompactionConfig config = new DataSourceCompactionConfig(
        "datasource",
        null,
        null,
        null,
        null,
        new UserCompactionTaskQueryTuningConfig(
            null,
            null,
            null,
            null,
            new SingleDimensionPartitionsSpec(10000, null, "dim", false),
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
        null
    );
    Assert.assertEquals(
        new SingleDimensionPartitionsSpec(10000, null, "dim", false),
        NewestSegmentFirstIterator.findPartitinosSpecFromConfig(
            ClientCompactionTaskQueryTuningConfig.from(config.getTuningConfig(), config.getMaxRowsPerSegment())
        )
    );
  }
}
