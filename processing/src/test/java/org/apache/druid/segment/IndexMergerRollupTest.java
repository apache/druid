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

package org.apache.druid.segment;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.SerializablePairLongLong;
import org.apache.druid.query.aggregation.first.DoubleFirstAggregatorFactory;
import org.apache.druid.query.aggregation.first.FloatFirstAggregatorFactory;
import org.apache.druid.query.aggregation.first.LongFirstAggregatorFactory;
import org.apache.druid.query.aggregation.first.StringFirstAggregatorFactory;
import org.apache.druid.query.aggregation.last.DoubleLastAggregatorFactory;
import org.apache.druid.query.aggregation.last.FloatLastAggregatorFactory;
import org.apache.druid.query.aggregation.last.LongLastAggregatorFactory;
import org.apache.druid.query.aggregation.last.StringLastAggregatorFactory;
import org.apache.druid.segment.data.IncrementalIndexTest;
import org.apache.druid.segment.incremental.IncrementalIndex;
import org.apache.druid.segment.writeout.OffHeapMemorySegmentWriteOutMediumFactory;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class IndexMergerRollupTest extends InitializedNullHandlingTest
{

  private IndexMerger indexMerger;
  private IndexIO indexIO;
  private IndexSpec indexSpec;

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private final List<Map<String, Object>> strEventsList = Arrays.asList(
      ImmutableMap.of("d", "d1", "m", "m1"),
      ImmutableMap.of("d", "d1", "m", "m2")
  );

  private final List<Map<String, Object>> doubleEventsList = Arrays.asList(
      ImmutableMap.of("d", "d1", "m", 1.0d),
      ImmutableMap.of("d", "d1", "m", 2.0d)
  );

  private final List<Map<String, Object>> floatEventsList = Arrays.asList(
      ImmutableMap.of("d", "d1", "m", 1.0f),
      ImmutableMap.of("d", "d1", "m", 2.0f)
  );

  private final List<Map<String, Object>> longEventsList = Arrays.asList(
      ImmutableMap.of("d", "d1", "m", 1L),
      ImmutableMap.of("d", "d1", "m", 2L)
  );

  @Before
  public void setUp()
  {
    indexMerger = TestHelper
        .getTestIndexMergerV9(OffHeapMemorySegmentWriteOutMediumFactory.instance());
    indexIO = TestHelper.getTestIndexIO();
    indexSpec = new IndexSpec();
  }

  private QueryableIndex testFirstLastRollup(
      final List<Map<String, Object>> eventsList,
      final List<String> dimensions,
      final AggregatorFactory... aggregatorFactories
  ) throws Exception
  {
    final File tempDir = temporaryFolder.newFolder();

    List<QueryableIndex> indexes = new ArrayList<>();

    // set timestamp of all events to the same so that rollup will be applied to these events
    Instant time = Instant.now();

    for (Map<String, Object> events : eventsList) {
      IncrementalIndex toPersist = IncrementalIndexTest.createIndex(aggregatorFactories);

      toPersist.add(new MapBasedInputRow(time.toEpochMilli(), dimensions, events));
      indexes.add(indexIO.loadIndex(indexMerger.persist(toPersist, tempDir, indexSpec, null)));
    }

    File indexFile = indexMerger
        .mergeQueryableIndex(indexes, true, aggregatorFactories, tempDir, indexSpec, null, -1);
    return indexIO.loadIndex(indexFile);
  }

  @Test
  public void testStringFirstRollup() throws Exception
  {
    try (QueryableIndex mergedIndex = testFirstLastRollup(
        strEventsList,
        ImmutableList.of("d"),
        new StringFirstAggregatorFactory("m", "m", 1024)
    )) {
      Assert.assertEquals("Number of rows should be 1", 1, mergedIndex.getNumRows());
    }
  }

  @Test
  public void testStringLastRollup() throws Exception
  {
    try (QueryableIndex mergedIndex = testFirstLastRollup(
        strEventsList,
        ImmutableList.of("d"),
        new StringLastAggregatorFactory("m", "m", 1024)
    )) {
      Assert.assertEquals("Number of rows should be 1", 1, mergedIndex.getNumRows());
    }
  }

  @Test
  public void testDoubleFirstRollup() throws Exception
  {
    try (QueryableIndex mergedIndex = testFirstLastRollup(
        doubleEventsList,
        ImmutableList.of("d"),
        new DoubleFirstAggregatorFactory("m", "m")
    )) {
      Assert.assertEquals("Number of rows should be 1", 1, mergedIndex.getNumRows());
    }
  }

  @Test
  public void testDoubleLastRollup() throws Exception
  {
    try (QueryableIndex mergedIndex = testFirstLastRollup(
        doubleEventsList,
        ImmutableList.of("d"),
        new DoubleLastAggregatorFactory("m", "m")
    )) {
      Assert.assertEquals("Number of rows should be 1", 1, mergedIndex.getNumRows());
    }
  }

  @Test
  public void testFloatFirstRollup() throws Exception
  {
    try (QueryableIndex mergedIndex = testFirstLastRollup(
        floatEventsList,
        ImmutableList.of("d"),
        new FloatFirstAggregatorFactory("m", "m")
    )) {
      Assert.assertEquals("Number of rows should be 1", 1, mergedIndex.getNumRows());
    }
  }

  @Test
  public void testFloatLastRollup() throws Exception
  {
    try (QueryableIndex mergedIndex = testFirstLastRollup(
        floatEventsList,
        ImmutableList.of("d"),
        new FloatLastAggregatorFactory("m", "m")
    )) {
      Assert.assertEquals("Number of rows should be 1", 1, mergedIndex.getNumRows());
    }
  }

  @Test
  public void testLongFirstRollup() throws Exception
  {
    try (QueryableIndex mergedIndex = testFirstLastRollup(
        longEventsList,
        ImmutableList.of("d"),
        new LongFirstAggregatorFactory("m", "m")
    )) {
      Assert.assertEquals("Number of rows should be 1", 1, mergedIndex.getNumRows());
    }
  }

  @Test
  public void testLongLastRollup() throws Exception
  {
    try (QueryableIndex mergedIndex = testFirstLastRollup(
        longEventsList,
        ImmutableList.of("d"),
        new LongLastAggregatorFactory("m", "m")
    )) {
      Assert.assertEquals("Number of rows should be 1", 1, mergedIndex.getNumRows());
    }
  }

  @Test
  public void testLongFirstRollupWithNull() throws Exception
  {
    final List<Map<String, Object>> longEventsList = Arrays.asList(
        // m == null
        ImmutableMap.of("d", "d1"),

        ImmutableMap.of("d", "d1", "m", 1L),
        ImmutableMap.of("d", "d1", "m", 2L)
    );

    try (QueryableIndex mergedIndex = testFirstLastRollup(
        longEventsList,
        ImmutableList.of("d"),
        new LongFirstAggregatorFactory("m", "m")
    )) {
      Assert.assertEquals("Number of rows should be 1", 1, mergedIndex.getNumRows());

      Object o = mergedIndex.getColumnHolder("m")
                            .getColumn()
                            .makeColumnValueSelector(new SimpleAscendingOffset(0))
                            .getObject();
      Assert.assertEquals(o.getClass(), SerializablePairLongLong.class);

      // since input events have the same timestamp, longFirst aggregator should return the first event
      if (NullHandling.replaceWithDefault()) {
        Assert.assertEquals(0L, ((SerializablePairLongLong) o).rhs.longValue());
      } else {
        Assert.assertEquals(null, ((SerializablePairLongLong) o).rhs);
      }
    }
  }
}
