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

import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.metamx.emitter.service.ServiceEmitter;
import com.metamx.emitter.service.ServiceMetricEvent;
import io.druid.granularity.PeriodGranularity;
import io.druid.java.util.common.guava.FunctionalIterable;
import io.druid.java.util.common.guava.Sequence;
import io.druid.java.util.common.guava.Sequences;
import io.druid.query.spec.MultipleIntervalSegmentSpec;
import org.joda.time.Interval;
import org.joda.time.Period;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

/**
 */
public class IntervalChunkingQueryRunner<T> implements QueryRunner<T>
{
  private final QueryRunner<T> baseRunner;

  private final QueryToolChest<T, Query<T>> toolChest;
  private final ExecutorService executor;
  private final QueryWatcher queryWatcher;
  private final ServiceEmitter emitter;

  public IntervalChunkingQueryRunner(
      QueryRunner<T> baseRunner, QueryToolChest<T, Query<T>> toolChest,
      ExecutorService executor, QueryWatcher queryWatcher, ServiceEmitter emitter
  )
  {
    this.baseRunner = baseRunner;
    this.toolChest = toolChest;
    this.executor = executor;
    this.queryWatcher = queryWatcher;
    this.emitter = emitter;
  }

  @Override
  public Sequence<T> run(final Query<T> query, final Map<String, Object> responseContext)
  {
    final Period chunkPeriod = getChunkPeriod(query);
    if (chunkPeriod.toStandardDuration().getMillis() == 0) {
      return baseRunner.run(query, responseContext);
    }

    List<Interval> chunkIntervals = Lists.newArrayList(
        FunctionalIterable
            .create(query.getIntervals())
            .transformCat(
                new Function<Interval, Iterable<Interval>>()
                {
                  @Override
                  public Iterable<Interval> apply(Interval input)
                  {
                    return splitInterval(input, chunkPeriod);
                  }
                }
            )
    );

    if (chunkIntervals.size() <= 1) {
      return baseRunner.run(query, responseContext);
    }

    return Sequences.concat(
        Lists.newArrayList(
            FunctionalIterable.create(chunkIntervals).transform(
                new Function<Interval, Sequence<T>>()
                {
                  @Override
                  public Sequence<T> apply(Interval singleInterval)
                  {
                    return new AsyncQueryRunner<T>(
                        //Note: it is assumed that toolChest.mergeResults(..) gives a query runner that is
                        //not lazy i.e. it does most of its work on call to run() method
                        toolChest.mergeResults(
                            new MetricsEmittingQueryRunner<T>(
                                emitter,
                                new Function<Query<T>, ServiceMetricEvent.Builder>()
                                {
                                  @Override
                                  public ServiceMetricEvent.Builder apply(Query<T> input)
                                  {
                                    return toolChest.makeMetricBuilder(input);
                                  }
                                },
                                baseRunner,
                                "query/intervalChunk/time",
                                ImmutableMap.of("chunkInterval", singleInterval.toString())
                            ).withWaitMeasuredFromNow()
                        ),
                        executor, queryWatcher
                    ).run(
                        query.withQuerySegmentSpec(new MultipleIntervalSegmentSpec(Arrays.asList(singleInterval))),
                        responseContext
                    );
                  }
                }
            )
        )
    );
  }

  private Iterable<Interval> splitInterval(Interval interval, Period period)
  {
    if (interval.getEndMillis() == interval.getStartMillis()) {
      return Lists.newArrayList(interval);
    }

    List<Interval> intervals = Lists.newArrayList();
    Iterator<Long> timestamps = new PeriodGranularity(period, null, null).iterable(
        interval.getStartMillis(),
        interval.getEndMillis()
    ).iterator();

    long start = Math.max(timestamps.next(), interval.getStartMillis());
    while (timestamps.hasNext()) {
      long end = timestamps.next();
      intervals.add(new Interval(start, end));
      start = end;
    }

    if (start < interval.getEndMillis()) {
      intervals.add(new Interval(start, interval.getEndMillis()));
    }

    return intervals;
  }

  private Period getChunkPeriod(Query<T> query)
  {
    String p = query.getContextValue(QueryContextKeys.CHUNK_PERIOD, "P0D");
    return Period.parse(p);
  }
}
