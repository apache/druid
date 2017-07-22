/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.query;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.metamx.emitter.service.ServiceEmitter;
import io.druid.java.util.common.guava.Sequences;
import io.druid.query.Druids.TimeseriesQueryBuilder;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.CountAggregatorFactory;
import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.concurrent.ExecutorService;

public class IntervalChunkingQueryRunnerTest
{
  private IntervalChunkingQueryRunnerDecorator decorator;
  private ExecutorService executors;
  private QueryRunner baseRunner;
  private QueryToolChest toolChest;

  private final TimeseriesQueryBuilder queryBuilder;

  public IntervalChunkingQueryRunnerTest()
  {
    queryBuilder = Druids.newTimeseriesQueryBuilder()
              .dataSource("test")
              .aggregators(Lists.<AggregatorFactory>newArrayList(new CountAggregatorFactory("count")));
  }

  @Before
  public void setup()
  {
    executors = EasyMock.createMock(ExecutorService.class);
    ServiceEmitter emitter = EasyMock.createNiceMock(ServiceEmitter.class);
    decorator = new IntervalChunkingQueryRunnerDecorator(executors,
        QueryRunnerTestHelper.NOOP_QUERYWATCHER, emitter);
    baseRunner = EasyMock.createMock(QueryRunner.class);
    toolChest = EasyMock.createNiceMock(QueryToolChest.class);
  }

  @Test
  public void testDefaultNoChunking()
  {
    QueryPlus queryPlus = QueryPlus.wrap(queryBuilder.intervals("2014/2016").build());

    EasyMock.expect(baseRunner.run(queryPlus, Collections.EMPTY_MAP)).andReturn(Sequences.empty());
    EasyMock.replay(baseRunner);

    QueryRunner runner = decorator.decorate(baseRunner, toolChest);
    runner.run(queryPlus, Collections.EMPTY_MAP);

    EasyMock.verify(baseRunner);
  }

  @Test
  public void testChunking()
  {
    Query query = queryBuilder.intervals("2015-01-01T00:00:00.000/2015-01-11T00:00:00.000").context(ImmutableMap.<String, Object>of("chunkPeriod", "P1D")).build();

    executors.execute(EasyMock.anyObject(Runnable.class));
    EasyMock.expectLastCall().times(10);

    EasyMock.replay(executors);
    EasyMock.replay(toolChest);

    QueryRunner runner = decorator.decorate(baseRunner, toolChest);
    runner.run(query, Collections.EMPTY_MAP);

    EasyMock.verify(executors);
  }

  @Test
  public void testChunkingOnMonths()
  {
    Query query = queryBuilder.intervals("2015-01-01T00:00:00.000/2015-02-11T00:00:00.000").context(ImmutableMap.<String, Object>of("chunkPeriod", "P1M")).build();

    executors.execute(EasyMock.anyObject(Runnable.class));
    EasyMock.expectLastCall().times(2);

    EasyMock.replay(executors);
    EasyMock.replay(toolChest);

    QueryRunner runner = decorator.decorate(baseRunner, toolChest);
    runner.run(query, Collections.EMPTY_MAP);

    EasyMock.verify(executors);
  }
}
