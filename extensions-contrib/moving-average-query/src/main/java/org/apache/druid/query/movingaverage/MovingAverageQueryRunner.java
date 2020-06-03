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

package org.apache.druid.query.movingaverage;

import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.data.input.MapBasedRow;
import org.apache.druid.data.input.Row;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.granularity.PeriodGranularity;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.QueryDataSource;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QuerySegmentWalker;
import org.apache.druid.query.Result;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.UnionDataSource;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.ResultRow;
import org.apache.druid.query.movingaverage.averagers.AveragerFactory;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.query.timeseries.TimeseriesQuery;
import org.apache.druid.query.timeseries.TimeseriesResultValue;
import org.apache.druid.server.QueryStats;
import org.apache.druid.server.RequestLogLine;
import org.apache.druid.server.log.RequestLogger;
import org.joda.time.Interval;
import org.joda.time.Period;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * The QueryRunner for MovingAverage query.
 * High level flow:
 * 1. Invokes an inner groupBy query (Or timeseries for no dimensions scenario) to get Aggregations/PostAggregtions.
 * 2. Result is passed to {@link RowBucketIterable}, which groups rows of all dimension combinations into period-based (e.g. daily) buckets of rows ({@link RowBucket}).
 * 3. The sequence is passed to {@link MovingAverageIterable}, which performs the main part of the query of adding Averagers computation into the records.
 * 4. Finishes up by applying post averagers, removing redundant dates, and applying post phases (having, sorting, limits).
 */
public class MovingAverageQueryRunner implements QueryRunner<Row>
{
  private final QuerySegmentWalker walker;
  private final RequestLogger requestLogger;

  public MovingAverageQueryRunner(
      @Nullable QuerySegmentWalker walker,
      RequestLogger requestLogger
  )
  {
    this.walker = walker;
    this.requestLogger = requestLogger;
  }

  @Override
  public Sequence<Row> run(QueryPlus<Row> query, ResponseContext responseContext)
  {

    MovingAverageQuery maq = (MovingAverageQuery) query.getQuery();
    List<Interval> intervals;
    final Period period;

    // Get the largest bucket from the list of averagers
    Optional<Integer> opt =
        maq.getAveragerSpecs().stream().map(AveragerFactory::getNumBuckets).max(Integer::compare);
    int buckets = opt.orElse(0);

    //Extend the interval beginning by specified bucket - 1
    if (maq.getGranularity() instanceof PeriodGranularity) {
      period = ((PeriodGranularity) maq.getGranularity()).getPeriod();
      int offset = buckets <= 0 ? 0 : (1 - buckets);
      intervals = maq.getIntervals()
                     .stream()
                     .map(i -> new Interval(i.getStart().withPeriodAdded(period, offset), i.getEnd()))
                     .collect(Collectors.toList());
    } else {
      throw new ISE("Only PeriodGranulaity is supported for movingAverage queries");
    }

    Sequence<Row> resultsSeq;
    DataSource dataSource = maq.getDataSource();
    if (maq.getDimensions() != null && !maq.getDimensions().isEmpty() &&
        (dataSource instanceof TableDataSource || dataSource instanceof UnionDataSource ||
         dataSource instanceof QueryDataSource)) {
      // build groupBy query from movingAverage query
      GroupByQuery.Builder builder = GroupByQuery.builder()
                                                 .setDataSource(dataSource)
                                                 .setInterval(intervals)
                                                 .setDimFilter(maq.getFilter())
                                                 .setGranularity(maq.getGranularity())
                                                 .setDimensions(maq.getDimensions())
                                                 .setAggregatorSpecs(maq.getAggregatorSpecs())
                                                 .setPostAggregatorSpecs(maq.getPostAggregatorSpecs())
                                                 .setContext(maq.getContext());
      GroupByQuery gbq = builder.build();

      ResponseContext gbqResponseContext = ResponseContext.createEmpty();
      gbqResponseContext.put(
          ResponseContext.Key.QUERY_FAIL_DEADLINE_MILLIS,
          System.currentTimeMillis() + QueryContexts.getTimeout(gbq)
      );
      gbqResponseContext.put(ResponseContext.Key.QUERY_TOTAL_BYTES_GATHERED, new AtomicLong());

      Sequence<ResultRow> results = gbq.getRunner(walker).run(QueryPlus.wrap(gbq), gbqResponseContext);
      try {
        // use localhost for remote address
        requestLogger.logNativeQuery(RequestLogLine.forNative(
            gbq,
            DateTimes.nowUtc(),
            "127.0.0.1",
            new QueryStats(
                ImmutableMap.of(
                    "query/time", 0,
                    "query/bytes", 0,
                    "success", true
                ))
        ));
      }
      catch (Exception e) {
        throw Throwables.propagate(e);
      }

      resultsSeq = results.map(row -> row.toMapBasedRow(gbq));
    } else {
      // no dimensions, so optimize this as a TimeSeries
      TimeseriesQuery tsq = new TimeseriesQuery(
          dataSource,
          new MultipleIntervalSegmentSpec(intervals),
          false,
          null,
          maq.getFilter(),
          maq.getGranularity(),
          maq.getAggregatorSpecs(),
          maq.getPostAggregatorSpecs(),
          0,
          maq.getContext()
      );
      ResponseContext tsqResponseContext = ResponseContext.createEmpty();
      tsqResponseContext.put(
          ResponseContext.Key.QUERY_FAIL_DEADLINE_MILLIS,
          System.currentTimeMillis() + QueryContexts.getTimeout(tsq)
      );
      tsqResponseContext.put(ResponseContext.Key.QUERY_TOTAL_BYTES_GATHERED, new AtomicLong());

      Sequence<Result<TimeseriesResultValue>> results = tsq.getRunner(walker).run(QueryPlus.wrap(tsq), tsqResponseContext);
      try {
        // use localhost for remote address
        requestLogger.logNativeQuery(RequestLogLine.forNative(
            tsq,
            DateTimes.nowUtc(),
            "127.0.0.1",
            new QueryStats(
                ImmutableMap.of(
                    "query/time", 0,
                    "query/bytes", 0,
                    "success", true
                ))
        ));
      }
      catch (Exception e) {
        throw Throwables.propagate(e);
      }

      resultsSeq = Sequences.map(results, new TimeseriesResultToRow());
    }

    // Process into period buckets
    Sequence<RowBucket> bucketedMovingAvgResults =
        Sequences.simple(new RowBucketIterable(resultsSeq, intervals, period));

    // Apply the windows analysis functions
    Sequence<Row> movingAvgResults = Sequences.simple(
        new MovingAverageIterable(
            bucketedMovingAvgResults,
            maq.getDimensions(),
            maq.getAveragerSpecs(),
            maq.getPostAggregatorSpecs(),
            maq.getAggregatorSpecs()
        )
    );

    // Apply any postAveragers
    Sequence<Row> movingAvgResultsWithPostAveragers =
        Sequences.map(movingAvgResults, new PostAveragerAggregatorCalculator(maq));

    // remove rows outside the reporting window
    List<Interval> reportingIntervals = maq.getIntervals();
    movingAvgResults =
        Sequences.filter(
            movingAvgResultsWithPostAveragers,
            row -> reportingIntervals.stream().anyMatch(i -> i.contains(row.getTimestamp()))
        );

    // Apply any having, sorting, and limits
    movingAvgResults = maq.applyLimit(movingAvgResults);

    return movingAvgResults;

  }

  static class TimeseriesResultToRow implements Function<Result<TimeseriesResultValue>, Row>
  {
    @Override
    public Row apply(Result<TimeseriesResultValue> lookbackResult)
    {
      Map<String, Object> event = lookbackResult.getValue().getBaseObject();
      MapBasedRow row = new MapBasedRow(lookbackResult.getTimestamp(), event);
      return row;
    }
  }
}
