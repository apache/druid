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
package org.apache.druid.query.lookbackquery;

import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.UnionDataSource;
import org.apache.druid.query.rollingavgquery.RollingAverageQuery;
import org.apache.druid.query.rollingavgquery.averagers.AveragerFactory;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.Period;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Predicates;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.data.input.MapBasedRow;
import org.apache.druid.data.input.Row;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryDataSource;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QuerySegmentWalker;
import org.apache.druid.query.QueryToolChest;
import org.apache.druid.query.QueryToolChestWarehouse;
import org.apache.druid.query.Result;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.MetricManipulatorFns;
import org.apache.druid.query.aggregation.PostAggregator;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.query.timeseries.TimeseriesQuery;
import org.apache.druid.query.timeseries.TimeseriesResultValue;
import org.apache.druid.server.log.RequestLogger;

/**
 * The QueryRunner for LookbackQuery
 */
public class LookbackQueryRunner implements QueryRunner<Result<LookbackResultValue>>
{
  private static final ExecutorService EXECUTOR = Executors.newFixedThreadPool(30);
  private static final String MEASUREMENT_QUERY_ID = "measurement_query";
  private static final String COHORT_QUERY_ID = "cohort_query";
  private static final Logger LOGGER = new Logger(LookbackQueryRunner.class);
  public static final String QUERY_FAIL_TIME = "queryFailTime";
  public static final String QUERY_TOTAL_BYTES_GATHERED = "queryTotalBytesGathered";

  private final QueryToolChestWarehouse warehouse;
  private final QuerySegmentWalker walker;
  private final RequestLogger requestLogger;

  public LookbackQueryRunner(
      HttpClient httpClient,
      ServiceEmitter emitter,
      ObjectMapper mapper,
      ObjectMapper smileMapper,
      QueryToolChestWarehouse warehouse,
      QuerySegmentWalker walker,
      RequestLogger requestLogger
  )
  {
    this.warehouse = warehouse;
    this.walker = walker;
    this.requestLogger = requestLogger;
  }

  // Used for unit testing
  LookbackQueryRunner(
      QuerySegmentWalker walker,
      QueryToolChestWarehouse warehouse,
      RequestLogger logger
  )
  {
    this.warehouse = warehouse;
    this.walker = walker;
    this.requestLogger = logger;
  }

  /**
   * Static class implementing a Function to convert Result&lt;LookbackResultValue&gt; to Row
   */
  static class LookbackResultValueToRow implements Function<Result<LookbackResultValue>, Row>
  {
    public Row apply(Result<LookbackResultValue> lookbackResult)
    {
      Result<?> l = (Result<?>) lookbackResult;
      Object val = l.getValue();
      Map<String, Object> m = null;
      if (val instanceof TimeseriesResultValue) {
        m = ((TimeseriesResultValue) val).getBaseObject();
      } else if (val instanceof LookbackResultValue) {
        m = ((LookbackResultValue) val).getMeasurementValues();
      }
      MapBasedRow row = new MapBasedRow(l.getTimestamp(), m);
      return row;
    }
  }

  /**
   * Static class implementing a function to convert a Row to Result&lt;LookbackResultValue&gt;
   */
  static class RowToLookbackResultValue implements Function<Row, Result<LookbackResultValue>>
  {
    public Result<LookbackResultValue> apply(Row row)
    {
      LookbackResultValue lrv = new LookbackResultValue(((MapBasedRow) row).getEvent());
      Result<LookbackResultValue> rlvr = new Result<>(row.getTimestamp(), lrv);
      return rlvr;
    }
  }

  static class QueryResultToLookbackResultValue implements Function<Object, Result<LookbackResultValue>>
  {
    public Result<LookbackResultValue> apply(Object row)
    {
      if (row instanceof MapBasedRow) {
        LookbackResultValue lrv = new LookbackResultValue(((MapBasedRow) row).getEvent());
        Result<LookbackResultValue> rlrv = new Result<>(((MapBasedRow) row).getTimestamp(), lrv);
        return rlrv;
      } else if (row instanceof Result) {
        @SuppressWarnings("unchecked")
        LookbackResultValue lrv = new LookbackResultValue(((Result<TimeseriesResultValue>) row).getValue()
                                                                                               .getBaseObject());
        Result<LookbackResultValue> rlrv = new Result<>(((Result<?>) row).getTimestamp(), lrv);
        return rlrv;
      } else {
        throw new ISE("Only Groupby, Timeseries, and RollingAverage results are supported");
      }
    }
  }

  /**
   * Joins the result of a Timeseries measurement query with the cohort query
   */
  static class JoinTimeSeriesResults extends JoinResults<DateTime, Result<TimeseriesResultValue>>
  {
    public JoinTimeSeriesResults(Future<Sequence<Result<TimeseriesResultValue>>> cohortResult, Period lookbackOffset)
    {
      super(cohortResult, lookbackOffset);
    }

    @Override
    public DateTime extractTimestamp(Result<TimeseriesResultValue> val)
    {
      if (val == null) {
        return null;
      }
      return val.getTimestamp();
    }

    @Override
    public DateTime extractKey(Result<TimeseriesResultValue> val)
    {
      return val.getTimestamp();
    }

    @Override
    public DateTime extractOffsetKey(Result<LookbackResultValue> val, Period offset)
    {
      DateTime t = val.getTimestamp();
      return t.plus(offset);
    }

    @Override
    public Map<String, Object> extractResult(Result<TimeseriesResultValue> cVal)
    {
      return cVal == null ? null : cVal.getValue().getBaseObject();
    }

  }

  /**
   * Joins the result of a GroupBy measurement query and GroupBy cohort query
   */
  static class JoinGroupByResults extends JoinResults<Map<String, Object>, Row>
  {
    private final List<DimensionSpec> dimensionSpecs;

    public JoinGroupByResults(
        Future<Sequence<Row>> cohortResult,
        Period lookbackOffset,
        List<DimensionSpec> dimensionSpecs
    )
    {
      super(cohortResult, lookbackOffset);

      this.dimensionSpecs = dimensionSpecs;
    }

    @Override
    public DateTime extractTimestamp(Row val)
    {
      if (val == null) {
        return null;
      }
      return val.getTimestamp();
    }

    @Override
    public Map<String, Object> extractKey(Row val)
    {
      return LookbackQueryHelper.getGroupByCohortMapKey(
          dimensionSpecs,
          ((MapBasedRow) val).getEvent(),
          val.getTimestamp()
      );
    }

    @Override
    public Map<String, Object> extractOffsetKey(Result<LookbackResultValue> val, Period offset)
    {
      return LookbackQueryHelper.getGroupByCohortMapKey(
          dimensionSpecs,
          val.getValue().getMeasurementValues(),
          val.getTimestamp().plus(offset)
      );
    }

    @Override
    public Map<String, Object> extractResult(Row cVal)
    {
      return cVal == null ? null : ((MapBasedRow) cVal).getEvent();
    }
  }

  /**
   * Given the joined results from the measurement query and cohort query, generate the result of applying PostAggregation
   */
  static class GeneratePostAggResults implements Function<Result<LookbackResultValue>, Result<LookbackResultValue>>
  {

    private final List<PostAggregator> lookBackQueryPostAggs;
    private final List<String> prefixes;
    private final List<Period> offsets;
    private final List<AggregatorFactory> metrics;
    private final List<AveragerFactory<?, ?>> averagers;
    private final List<PostAggregator> innerPostAggs;

    public GeneratePostAggResults(LookbackQuery query)
    {
      this.lookBackQueryPostAggs = query.getPostAggregatorSpecs();
      this.prefixes = query.getLookbackPrefixes();
      this.offsets = query.getLookbackOffsets();

      Query<?> innerQuery = ((QueryDataSource) query.getDataSource()).getQuery();
      switch (innerQuery.getType()) {
        case Query.TIMESERIES:
          this.metrics = ((TimeseriesQuery) innerQuery).getAggregatorSpecs();
          this.innerPostAggs = ((TimeseriesQuery) innerQuery).getPostAggregatorSpecs();
          this.averagers = null;
          break;
        case Query.GROUP_BY:
          this.metrics = ((GroupByQuery) innerQuery).getAggregatorSpecs();
          this.innerPostAggs = ((GroupByQuery) innerQuery).getPostAggregatorSpecs();
          this.averagers = null;
          break;
        case RollingAverageQuery.ROLLING_AVG_QUERY_TYPE:
          this.metrics = ((RollingAverageQuery) innerQuery).getAggregatorSpecs();
          this.innerPostAggs = ((RollingAverageQuery) innerQuery).getPostAggregatorSpecs();
          this.averagers = ((RollingAverageQuery) innerQuery).getAveragerSpecs();
          break;
        default:
          throw new ISE("Query type [%s]is not supported", innerQuery.getType());
      }
    }

    @Override
    public Result<LookbackResultValue> apply(Result<LookbackResultValue> input)
    {

      Map<String, Object> measurementValues = input.getValue().getMeasurementValues();
      Map<Period, Map<String, Object>> cohortValuesSet = input.getValue().getLookbackValues();

      // Add metrics to the result. the value maps have pre-finalized values due to passing finalize=false
      // to the inner queries.
      for (AggregatorFactory metric : metrics) {
        for (int ii = 0; ii < offsets.size(); ++ii) {
          Map<String, Object> cohortValues = cohortValuesSet.get(offsets.get(ii));
          measurementValues.put(
              prefixes.get(ii) + metric.getName(),
              cohortValues == null ? null : cohortValues.get(metric.getName())
          );
        }
      }

      // Add inner postaggs to the result
      for (PostAggregator postAgg : innerPostAggs) {
        for (int ii = 0; ii < offsets.size(); ++ii) {
          Map<String, Object> cohortValues = cohortValuesSet.get(offsets.get(ii));
          measurementValues.put(
              prefixes.get(ii) + postAgg.getName(),
              cohortValues == null ? null : cohortValues.get(postAgg.getName())
          );
        }
      }

      if (averagers != null) {
        for (AveragerFactory<?, ?> averager : averagers) {
          for (int ii = 0; ii < offsets.size(); ++ii) {
            Map<String, Object> cohortValues = cohortValuesSet.get(offsets.get(ii));
            measurementValues.put(
                prefixes.get(ii) + averager.getName(),
                cohortValues == null ? null : cohortValues.get(averager.getName())
            );
          }
        }
      }

      // calculate the outer post aggs
      for (PostAggregator postAggregator : lookBackQueryPostAggs) {
        // Check for all fields being present.
        boolean allColsPresent = true;
        for (String col : postAggregator.getDependentFields()) {
          if (measurementValues.get(col) == null) {
            allColsPresent = false;
            break;
          }
        }
        // only calculate the post agg if all its dependent fields are present.
        measurementValues.put(
            postAggregator.getName(),
            allColsPresent ? postAggregator.compute(measurementValues) : null
        );
      }

      // resultMap may have unfinalized values in it from the inner query. Finalize them
      for (AggregatorFactory metric : metrics) {

        measurementValues.put(metric.getName(), (measurementValues.get(metric.getName()) == null) ? null :
                                                metric.finalizeComputation(measurementValues.get(metric.getName())));
        for (int ii = 0; ii < offsets.size(); ++ii) {
          Map<String, Object> cohortValues = cohortValuesSet.get(offsets.get(ii));
          measurementValues.put(
              prefixes.get(ii) + metric.getName(),
              (cohortValues == null || cohortValues.get(metric.getName()) == null)
              ? null
              : metric.finalizeComputation(cohortValues.get(metric.getName()))
          );
        }
      }

      // drop cohort values now that they are no longer needed
      cohortValuesSet.clear();

      return input;
    }
  }

  /**
   * This method processes a Lookback query and generates the result.
   * * Take the datasource of the lookback query and run it for the specified interval to obtain the measurement result
   * * Calculate the cohort interval using the lookbackOffsets
   * * Generate the cohort query using the result of the lookback query and cohort interval. Obtain the cohort result
   * * Join the measurement result with the cohort result based on the timestamp, groupby dimensions and lookbackOffsets
   * * Generate the postAggregations using the joined results
   *
   * @param query           query to be processed
   * @param responseContext Map containg the responseContext
   *
   * @return The result of the lookback query
   */
  @SuppressWarnings("unchecked")
  @Override
  public Sequence<Result<LookbackResultValue>> run(
      QueryPlus<Result<LookbackResultValue>> query,
      Map<String, Object> responseContext
  )
  {
    LookbackQuery lookbackQuery = (LookbackQuery) query.getQuery();
    Map<String, Object> finalizeContextMap = new HashMap<>();
    Map<String, Object> queryContextMap = lookbackQuery.getContext();
    finalizeContextMap.putAll(queryContextMap);
    finalizeContextMap.put("finalize", false);
    String queryId = lookbackQuery.getId();

    //Calculate the cohortInterval
    List<Period> lookbackOffsets = ((LookbackQuery) query.getQuery()).getLookbackOffsets();
    Sequence<Result<LookbackResultValue>> postAggResults;
    @SuppressWarnings("rawtypes")
    List<Future> futures = new ArrayList<>();

    Query<Object> measurementQuery = ((QueryDataSource) lookbackQuery.getDataSource()).getQuery();
    QueryToolChest<Object, Query<Object>> toolChest = warehouse.getToolChest(measurementQuery);
    measurementQuery = measurementQuery.withOverriddenContext(finalizeContextMap)
                                       .withId(queryId.concat(MEASUREMENT_QUERY_ID));

    for (Period lookbackOffset : lookbackOffsets) {
      Query<Object> cohortQuery = getCohortQuery(finalizeContextMap, measurementQuery, lookbackOffset, queryId);
      futures.add(EXECUTOR.submit(new Callable<Sequence<?>>()
      {
        public Sequence<?> call() throws Exception
        {

          HashMap<String, Object> cohortResponse = new HashMap<>();
          cohortResponse.put(QUERY_FAIL_TIME, System.currentTimeMillis() + QueryContexts.getTimeout(cohortQuery));
          cohortResponse.put(QUERY_TOTAL_BYTES_GATHERED, new AtomicLong());

          //Generate the cohortQueryResult by running the cohortQuery
          Sequence<Object> cohortQueryResultSeq = cohortQuery.getRunner(walker)
                                                             .run(QueryPlus.wrap(cohortQuery), cohortResponse);

          // The following ensures that post-aggregators have been calcualted on the nested query. Some query types
          // drop the postagg results and calling makePostComputeManipulatorFn puts them back
          cohortQueryResultSeq = Sequences.<Object, Object>map(
              (Sequence<Object>) cohortQueryResultSeq,
              toolChest.makePostComputeManipulatorFn(cohortQuery, MetricManipulatorFns.identity())
          );
          List<Object> cohortResultList = Sequences.toList(cohortQueryResultSeq, new ArrayList<>());
          Sequence<Object> cohortQueryResult = Sequences.simple(cohortResultList);

          return cohortQueryResult;
        }
      }));
    }

    HashMap<String, Object> measurementResponse = new HashMap<>();
    measurementResponse.put(QUERY_FAIL_TIME, System.currentTimeMillis() + QueryContexts.getTimeout(measurementQuery));
    measurementResponse.put(QUERY_TOTAL_BYTES_GATHERED, new AtomicLong());

    //Generate the measurementResult by running  datasource of the lookbackQuery
    Sequence<Object> measurementQueryResult = measurementQuery.getRunner(walker)
                                                              .run(
                                                                  QueryPlus.wrap(measurementQuery),
                                                                  measurementResponse
                                                              );
    measurementQueryResult = Sequences.<Object, Object>map(
        (Sequence<Object>) measurementQueryResult,
        toolChest.makePostComputeManipulatorFn(measurementQuery, MetricManipulatorFns.identity())
    );
    Sequence<Result<LookbackResultValue>> results =
        Sequences.map(measurementQueryResult, new QueryResultToLookbackResultValue());

    String queryType = measurementQuery.getType();
    for (int index = 0; index < lookbackOffsets.size(); index++) {
      //Join the measurementResults with the cohortResults
      if (queryType.equals(Query.TIMESERIES)) {
        results = Sequences.map(
            results,
            new JoinTimeSeriesResults(
                (Future<Sequence<Result<TimeseriesResultValue>>>) futures.get(index),
                lookbackOffsets.get(index)
            )
        );
      } else if (queryType.equals(Query.GROUP_BY) || queryType.equals(RollingAverageQuery.ROLLING_AVG_QUERY_TYPE)) {
        results = Sequences.map(
            results,
            new JoinGroupByResults(futures.get(index), lookbackOffsets.get(index), lookbackQuery.getDimensions())
        );
      } else {
        throw new ISE("Query type [%s]is not supported", measurementQuery.getType());
      }
    }

    postAggResults = Sequences.map(
        Sequences.filter(results, Predicates.<Result<LookbackResultValue>>notNull()),
        new GeneratePostAggResults(lookbackQuery)
    );

    // Convert queryResult Type to Row since applyLimit expects Type Row.
    Sequence<Row> seqRowResult = Sequences.map(postAggResults, new LookbackResultValueToRow());
    seqRowResult = ((LookbackQuery) query.getQuery()).applyLimit(seqRowResult);
    // Reinstate result Type to LookbackResultValue before returning
    return Sequences.map(seqRowResult, new RowToLookbackResultValue());
  }

  /**
   * Get the cohort query by calculating and applying the correct interval
   *
   * @param finalizeContextMap Map to store query context
   * @param query              The query to be updated
   * @param lookbackOffset     The offset to be used for calculating the Cohort interval
   * @param queryId            The id of the updated query
   *
   * @return Cohort query with lookback interval
   */
  public Query<Object> getCohortQuery(
      Map<String, Object> finalizeContextMap,
      Query<Object> query,
      Period lookbackOffset,
      String queryId
  )
  {

    List<Interval> cohortInterval = LookbackQueryHelper.getCohortInterval(query.getIntervals(), lookbackOffset);

    Query<Object> updatedQuery = query
        .withQuerySegmentSpec(new MultipleIntervalSegmentSpec(cohortInterval))
        .withOverriddenContext(finalizeContextMap)
        .withId(queryId.concat(COHORT_QUERY_ID).concat(lookbackOffset.toString()));

    if (query.getDataSource() instanceof TableDataSource || query.getDataSource() instanceof UnionDataSource) {
      return updatedQuery;
    }

    @SuppressWarnings("unchecked")
    Query<Object> innerQuery = ((QueryDataSource) (query.getDataSource())).getQuery();
    return updatedQuery.withDataSource(new QueryDataSource(getCohortQuery(
        finalizeContextMap,
        innerQuery,
        lookbackOffset,
        queryId
    )));
  }
}
