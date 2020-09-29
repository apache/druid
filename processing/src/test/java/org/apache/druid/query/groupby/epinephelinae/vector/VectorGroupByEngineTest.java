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

import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.query.QueryRunnerTestHelper;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.DoubleSumAggregatorFactory;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.GroupByQueryConfig;
import org.apache.druid.query.groupby.ResultRow;
import org.apache.druid.segment.QueryableIndexStorageAdapter;
import org.apache.druid.segment.StorageAdapter;
import org.apache.druid.segment.TestIndex;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.joda.time.Interval;
import org.junit.Test;
import org.mockito.Mockito;

import java.nio.ByteBuffer;

public class VectorGroupByEngineTest extends InitializedNullHandlingTest
{
  @Test
  public void testProcessCreateNewGrouperWhenDelegateIsCreated()
  {
    final Interval interval = TestIndex.DATA_INTERVAL;
    final AggregatorFactory factory = Mockito.spy(new DoubleSumAggregatorFactory("index", "index"));
    final GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setGranularity(QueryRunnerTestHelper.DAY_GRAN)
        .setInterval(interval)
        .setDimensions(new DefaultDimensionSpec("market", null, null))
        .setAggregatorSpecs(factory)
        .build();
    final StorageAdapter storageAdapter = Mockito.spy(new QueryableIndexStorageAdapter(TestIndex.getMMappedTestIndex()));
    final ByteBuffer byteBuffer = ByteBuffer.wrap(new byte[4096]);

    final Sequence<ResultRow> sequence = VectorGroupByEngine.process(
        query,
        storageAdapter,
        byteBuffer,
        null,
        null,
        interval,
        new GroupByQueryConfig()
    );
    sequence.toList();
    // GroupByQueryEngineV2.getCardinalityForArrayAggregation() should be called whenever it creates a new grouper.
    Mockito.verify(storageAdapter, Mockito.times(94)).getDimensionCardinality("market");
  }
}
