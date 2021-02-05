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

package org.apache.druid.segment.loading;

import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.jackson.SegmentizerModule;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.expression.TestExprMacroTable;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.IndexMerger;
import org.apache.druid.segment.IndexMergerV9;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.SegmentLazyLoadFailCallback;
import org.apache.druid.segment.TestIndex;
import org.apache.druid.segment.incremental.IncrementalIndex;
import org.apache.druid.segment.join.table.BroadcastSegmentIndexedTable;
import org.apache.druid.segment.join.table.IndexedTable;
import org.apache.druid.segment.writeout.OffHeapMemorySegmentWriteOutMediumFactory;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.apache.druid.timeline.DataSegment;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Set;

public class BroadcastJoinableMMappedQueryableSegmentizerFactoryTest extends InitializedNullHandlingTest
{
  private static final String TABLE_NAME = "test";
  private static final Set<String> KEY_COLUMNS =
      ImmutableSet.of("market", "longNumericNull", "doubleNumericNull", "floatNumericNull", "partial_null_column");

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void testSegmentizer() throws IOException, SegmentLoadingException
  {
    final ObjectMapper mapper = new DefaultObjectMapper();
    mapper.registerModule(new SegmentizerModule());
    final IndexIO indexIO = new IndexIO(mapper, () -> 0);
    mapper.setInjectableValues(
        new InjectableValues.Std()
            .addValue(ExprMacroTable.class.getName(), TestExprMacroTable.INSTANCE)
            .addValue(ObjectMapper.class.getName(), mapper)
            .addValue(IndexIO.class, indexIO)
            .addValue(DataSegment.PruneSpecsHolder.class, DataSegment.PruneSpecsHolder.DEFAULT)
    );

    IndexMerger indexMerger = new IndexMergerV9(mapper, indexIO, OffHeapMemorySegmentWriteOutMediumFactory.instance());

    SegmentizerFactory expectedFactory = new BroadcastJoinableMMappedQueryableSegmentizerFactory(
        indexIO,
        KEY_COLUMNS
    );
    Interval testInterval = Intervals.of("2011-01-12T00:00:00.000Z/2011-05-01T00:00:00.000Z");
    IncrementalIndex data = TestIndex.makeRealtimeIndex("druid.sample.numeric.tsv");

    List<String> columnNames = data.getColumnNames();
    File segment = new File(temporaryFolder.newFolder(), "segment");
    File persistedSegmentRoot = indexMerger.persist(
        data,
        testInterval,
        segment,
        new IndexSpec(
            null,
            null,
            null,
            null,
            expectedFactory
        ),
        null
    );

    File factoryJson = new File(persistedSegmentRoot, "factory.json");
    Assert.assertTrue(factoryJson.exists());
    SegmentizerFactory factory = mapper.readValue(factoryJson, SegmentizerFactory.class);
    Assert.assertTrue(factory instanceof BroadcastJoinableMMappedQueryableSegmentizerFactory);
    Assert.assertEquals(expectedFactory, factory);

    // load a segment
    final DataSegment dataSegment = new DataSegment(
        TABLE_NAME,
        testInterval,
        DateTimes.nowUtc().toString(),
        ImmutableMap.of(),
        columnNames,
        ImmutableList.of(),
        null,
        null,
        persistedSegmentRoot.getTotalSpace()
    );
    final Segment loaded = factory.factorize(dataSegment, persistedSegmentRoot, false, SegmentLazyLoadFailCallback.NOOP);

    final BroadcastSegmentIndexedTable table = (BroadcastSegmentIndexedTable) loaded.as(IndexedTable.class);
    Assert.assertNotNull(table);
  }
}
