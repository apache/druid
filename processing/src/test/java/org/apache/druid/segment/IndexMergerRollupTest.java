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
import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.query.aggregation.AggregatorFactory;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class IndexMergerRollupTest extends InitializedNullHandlingTest
{

  private IndexMerger indexMerger;
  private IndexIO indexIO;
  private IndexSpec indexSpec;

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Before
  public void setUp()
  {
    indexMerger = TestHelper
        .getTestIndexMergerV9(OffHeapMemorySegmentWriteOutMediumFactory.instance());
    indexIO = TestHelper.getTestIndexIO();
    indexSpec = IndexSpec.DEFAULT;
  }

  private void testFirstLastRollup(
      AggregatorFactory[] aggregatorFactories
  ) throws Exception
  {
    List<Map<String, Object>> eventsList = Arrays.asList(
        new HashMap<>(
            ImmutableMap.of(
                "d", "d1",
                "m", "m2",
                "l", 124L,
                "f", 124.0F,
                "dl", 124.5D
            )),
        new HashMap<>(
            ImmutableMap.of(
                "d", "d1",
                "m", "m2",
                "l", 124L,
                "f", 124.0F,
                "dl", 124.5D
            ))
    );

    final File tempDir = temporaryFolder.newFolder();

    List<QueryableIndex> indexes = new ArrayList<>();
    Instant time = Instant.now();

    for (Map<String, Object> events : eventsList) {
      IncrementalIndex toPersist = IncrementalIndexTest.createIndex(aggregatorFactories);

      toPersist.add(new MapBasedInputRow(time.toEpochMilli(), ImmutableList.of("d"), events));
      indexes.add(indexIO.loadIndex(indexMerger.persist(toPersist, tempDir, indexSpec, null)));
    }

    File indexFile = indexMerger
        .mergeQueryableIndex(indexes, true, aggregatorFactories, tempDir, indexSpec, null, -1);
    try (QueryableIndex mergedIndex = indexIO.loadIndex(indexFile)) {
      Assert.assertEquals("Number of rows should be 1", 1, mergedIndex.getNumRows());
    }
  }

  @Test
  public void testFirstRollup() throws Exception
  {
    AggregatorFactory[] aggregatorFactories = new AggregatorFactory[]{
        new StringFirstAggregatorFactory("m", "m", null, 1024),
        new LongFirstAggregatorFactory("l", "l", null),
        new FloatFirstAggregatorFactory("f", "f", null),
        new DoubleFirstAggregatorFactory("dl", "dl", null),
    };
    testFirstLastRollup(aggregatorFactories);
  }

  @Test
  public void testLastRollup() throws Exception
  {
    AggregatorFactory[] aggregatorFactories = new AggregatorFactory[]{
        new StringLastAggregatorFactory("m", "m", null, 1024),
        new LongLastAggregatorFactory("l", "l", null),
        new FloatLastAggregatorFactory("f", "f", null),
        new DoubleLastAggregatorFactory("dl", "dl", null),
    };
    testFirstLastRollup(aggregatorFactories);
  }
}
