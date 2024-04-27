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

package org.apache.druid.segment.incremental;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.guice.NestedDataModule;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.query.aggregation.LongMaxAggregator;
import org.apache.druid.query.aggregation.LongMaxAggregatorFactory;
import org.apache.druid.segment.CloserRule;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.easymock.EasyMock;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collection;
import java.util.Collections;

@RunWith(Parameterized.class)
public class IncrementalIndexIngestionTest extends InitializedNullHandlingTest
{
  private static final int MAX_ROWS = 100_000;

  public final IncrementalIndexCreator indexCreator;

  @Rule
  public final CloserRule closer = new CloserRule(false);

  public IncrementalIndexIngestionTest(String indexType) throws JsonProcessingException
  {
    NestedDataModule.registerHandlersAndSerde();
    indexCreator = closer.closeLater(new IncrementalIndexCreator(indexType, (builder, args) -> builder
        .setIndexSchema((IncrementalIndexSchema) args[0])
        .setMaxRowCount(MAX_ROWS)
        .build()
    ));
  }

  @Parameterized.Parameters(name = "{index}: {0}")
  public static Collection<?> constructorFeeder()
  {
    return IncrementalIndexCreator.getAppendableIndexTypes();
  }

  @Test
  public void testOnHeapIncrementalIndexClose() throws Exception
  {
    // Prepare the mocks & set close() call count expectation to 1
    Aggregator mockedAggregator = EasyMock.createMock(LongMaxAggregator.class);
    mockedAggregator.close();
    EasyMock.expectLastCall().times(1);

    final IncrementalIndex genericIndex = indexCreator.createIndex(
        new IncrementalIndexSchema.Builder()
            .withQueryGranularity(Granularities.MINUTE)
            .withMetrics(new LongMaxAggregatorFactory("max", "max"))
            .build()
    );

    // This test is specific to the on-heap index
    if (!(genericIndex instanceof OnheapIncrementalIndex)) {
      return;
    }

    final OnheapIncrementalIndex index = (OnheapIncrementalIndex) genericIndex;

    index.add(new MapBasedInputRow(
            0,
            Collections.singletonList("billy"),
            ImmutableMap.of("billy", 1, "max", 1)
    ));

    // override the aggregators with the mocks
    index.concurrentGet(0)[0] = mockedAggregator;

    // close the indexer and validate the expectations
    EasyMock.replay(mockedAggregator);
    index.close();
    EasyMock.verify(mockedAggregator);
  }
}
