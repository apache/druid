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

package org.apache.druid.query.groupby.epinephelinae.vector;

import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.query.QueryRunnerTestHelper;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.DoubleSumAggregatorFactory;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.filter.InDimFilter;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.GroupByQueryConfig;
import org.apache.druid.query.groupby.GroupByQueryRunnerTest;
import org.apache.druid.query.groupby.GroupingEngine;
import org.apache.druid.query.groupby.ResultRow;
import org.apache.druid.query.groupby.epinephelinae.GroupByQueryEngine;
import org.apache.druid.query.groupby.epinephelinae.VectorGrouper;
import org.apache.druid.query.groupby.epinephelinae.vector.VectorGroupByEngine.VectorGroupByEngineIterator;
import org.apache.druid.segment.ColumnProcessors;
import org.apache.druid.segment.CursorFactory;
import org.apache.druid.segment.CursorHolder;
import org.apache.druid.segment.QueryableIndexCursorFactory;
import org.apache.druid.segment.QueryableIndexTimeBoundaryInspector;
import org.apache.druid.segment.TestIndex;
import org.apache.druid.segment.vector.VectorCursor;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.joda.time.Interval;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class VectorGroupByEngineIteratorTest extends InitializedNullHandlingTest
{
  @Test
  public void testCreateOneGrouperAndCloseItWhenClose() throws IOException
  {
    final Interval interval = TestIndex.DATA_INTERVAL;
    final AggregatorFactory factory = new DoubleSumAggregatorFactory("index", "index");
    final GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setGranularity(QueryRunnerTestHelper.DAY_GRAN)
        .setInterval(interval)
        .setDimensions(new DefaultDimensionSpec("market", null, null))
        .setAggregatorSpecs(factory)
        .build();
    final CursorFactory cursorFactory = new QueryableIndexCursorFactory(TestIndex.getMMappedTestIndex());
    final QueryableIndexTimeBoundaryInspector timeBoundaryInspector =
        QueryableIndexTimeBoundaryInspector.create(TestIndex.getMMappedTestIndex());
    final CursorHolder cursorHolder = cursorFactory.makeCursorHolder(
        GroupingEngine.makeCursorBuildSpec(query, null)
    );
    final ByteBuffer byteBuffer = ByteBuffer.wrap(new byte[4096]);
    final VectorCursor cursor = cursorHolder.asVectorCursor();
    final List<GroupByVectorColumnSelector> dimensions = query.getDimensions().stream().map(
        dimensionSpec ->
            ColumnProcessors.makeVectorProcessor(
                dimensionSpec,
                GroupByVectorColumnProcessorFactory.instance(),
                cursor.getColumnSelectorFactory()
            )
    ).collect(Collectors.toList());
    final MutableObject<VectorGrouper> grouperCaptor = new MutableObject<>();
    final VectorGroupByEngineIterator iterator = new VectorGroupByEngineIterator(
        query,
        new GroupByQueryConfig(),
        GroupByQueryRunnerTest.DEFAULT_PROCESSING_CONFIG,
        timeBoundaryInspector,
        cursor,
        cursorHolder.getTimeOrder(),
        interval,
        dimensions,
        byteBuffer,
        null
    )
    {
      @Override
      VectorGrouper makeGrouper()
      {
        grouperCaptor.setValue(Mockito.spy(super.makeGrouper()));
        return grouperCaptor.getValue();
      }
    };
    iterator.close();
    Mockito.verify(grouperCaptor.getValue()).close();
    cursorHolder.close();
  }

  @Test
  public void testFilterCompactionFitsSmallBuffer() throws IOException
  {
    final CursorFactory cursorFactory = new QueryableIndexCursorFactory(TestIndex.getMMappedTestIndex());
    final QueryableIndexTimeBoundaryInspector timeBoundaryInspector =
        QueryableIndexTimeBoundaryInspector.create(TestIndex.getMMappedTestIndex());
    for (final boolean vectorize : List.of(false, true)) {
      final GroupByQuery query = GroupByQuery.builder()
          .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
          .setInterval(TestIndex.DATA_INTERVAL)
          .setGranularity(Granularities.ALL)
          .setDimensions(new DefaultDimensionSpec("quality", "alias"))
          .setDimFilter(new InDimFilter("quality", Set.of("business", "automotive")))
          .setAggregatorSpecs(new CountAggregatorFactory("rows"))
          .setContext(Map.of("vectorSize", 7, "enableFilterDictionaryCompaction", true))
          .build();
      Map<Object, Object> expected = null;
      for (final boolean forceHash : List.of(true, false)) {
        final GroupByQueryConfig config = new GroupByQueryConfig()
        {
          @Override
          public boolean isForceHashAggregation()
          {
            return forceHash;
          }
        }.withOverrides(query);
        // Two groups plus the missing slot fit in 32 bytes; the original dictionary and hash table do not.
        final ByteBuffer buffer = ByteBuffer.allocate(forceHash ? 65536 : 32);
        try (final CursorHolder cursorHolder = cursorFactory.makeCursorHolder(GroupingEngine.makeCursorBuildSpec(query, null))) {
          final Sequence<ResultRow> results = vectorize
              ? VectorGroupByEngine.process(
                  query,
                  timeBoundaryInspector,
                  cursorHolder,
                  buffer,
                  null,
                  query.getSingleInterval(),
                  config,
                  GroupByQueryRunnerTest.DEFAULT_PROCESSING_CONFIG
              )
              : GroupByQueryEngine.process(
                  query,
                  timeBoundaryInspector,
                  cursorHolder,
                  GroupingEngine.makeCursorBuildSpec(query, null),
                  buffer,
                  null,
                  config,
                  GroupByQueryRunnerTest.DEFAULT_PROCESSING_CONFIG
              );
          final Map<Object, Object> counts = new HashMap<>();
          for (final ResultRow row : results.toList()) {
            counts.put(row.get(query.getResultRowDimensionStart()), row.get(query.getResultRowAggregatorStart()));
          }
          Assertions.assertEquals(2, counts.size());
          if (forceHash) {
            expected = counts;
          } else {
            Assertions.assertEquals(expected, counts);
          }
        }
      }
    }
  }

}
