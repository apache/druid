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
import org.apache.druid.query.QueryRunnerTestHelper;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.DoubleSumAggregatorFactory;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.GroupByQueryConfig;
import org.apache.druid.query.groupby.GroupByQueryRunnerTest;
import org.apache.druid.query.groupby.GroupingEngine;
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
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
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
}
