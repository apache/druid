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

package org.apache.druid.query.context;

import com.google.common.base.Suppliers;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.BadQueryContextException;
import org.apache.druid.query.CloneQueryMode;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.context.constraint.Range;
import org.apache.druid.query.context.docs.ParameterDocumentation.Engine;
import org.apache.druid.query.context.docs.ParameterDocumentation.Query;
import org.apache.druid.query.context.docs.ParameterDocumentation.QueryType;
import org.apache.druid.query.topn.TopNQueryConfig;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.math.BigDecimal;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Supplier;

/// Central catalog of query context parameter descriptors.
///
/// Define user-facing metadata here, including descriptions, default values, constraints,
/// applicability, and introduction versions. The
/// {@link org.apache.druid.query.context.docs.ParameterDocumentationGenerator} generates the
/// corresponding tables in `query-context-reference.md`, `scan-query.md`, and
/// `sql-query-context.md`.
///
/// When adding or changing a descriptor, including introducing a new parameter, run this command
/// from the repository root to regenerate the checked-in documentation. The default Maven mode
/// verifies that the generated documentation is up to date.
///
/// ```shell
/// mvn -ntp -pl processing -am -Pskip-static-checks -Dweb.console.skip=true -DskipTests -Dquery.context.docs.mode=generate -T1C process-classes
/// ```
public final class QueryContextParameters
{
  public static final QueryContextParameter<String> QUERY_ID = stringParameter("queryId")
      .description(
          """
          Unique identifier given to this query.
          If a query ID is set or known, this can be used to cancel the query.
          """
      )
      .since("0.6.62")
      .defaultDescription("auto-generated")
      .query(Query.JSON, Query.SQL)
      .engine(Engine.NATIVE, Engine.MSQ, Engine.DART)
      .build();

  public static final QueryContextParameter<Boolean> FINALIZE = booleanParameter("finalize")
      .description(
          """
          Flag indicating whether to "finalize" aggregation results.
          Primarily used for debugging.
          For instance, the `hyperUnique` aggregator returns the full HyperLogLog sketch instead of the estimated
          cardinality when this flag is set to `false`.
          """
      )
      .since("0.1.0")
      .query(Query.JSON, Query.SQL)
      .engine(Engine.NATIVE)
      .build();

  public static final QueryContextParameter<Integer> PRIORITY = integerParameter("priority")
      .defaultValue(QueryContexts.DEFAULT_PRIORITY)
      .description(
          """
          Query priority.
          Queries with higher priority get precedence for computational resources.
          """
      )
      .since("0.5.0")
      .defaultDescription(
          """
          The default priority is one of the following: <ul><li>Value of `priority` in the query context, if
          set</li><li>The value of the runtime property `druid.query.default.context.priority`, if set and
          not null</li><li>`0` if the priority is not set in the query context or runtime properties</li></ul>
          """
      )
      .query(Query.JSON, Query.SQL)
      .engine(Engine.NATIVE)
      .build();

  public static final QueryContextParameter<String> LANE = stringParameter("lane")
      .description(
          """
          Query lane, used to control usage limits on classes of queries.
          See [Broker configuration](../configuration/index.md#broker) for more details.
          """
      )
      .since("0.18.0")
      .defaultDescription("`null`")
      .query(Query.JSON, Query.SQL)
      .engine(Engine.NATIVE)
      .build();

  public static final QueryContextParameter<Long> TIMEOUT = longParameter("timeout")
      .description(
          """
          Query timeout in millis, beyond which unfinished queries will be cancelled.
          0 timeout means `no timeout` (up to the server-side maximum query timeout, `druid.server.http.maxQueryTimeout`).
          To set the default timeout and maximum timeout, see [Broker configuration](../configuration/index.md#broker).
          """
      )
      .since("0.6.122")
      .defaultDescription("`druid.server.http.defaultQueryTimeout`")
      .query(Query.JSON, Query.SQL)
      .engine(Engine.NATIVE)
      .build();

  public static final QueryContextParameter<Long> PER_SEGMENT_TIMEOUT =
      longParameter("perSegmentTimeout")
          .description(
              """
              Per-segment processing timeout in millis, beyond which unfinished queries will be cancelled.
              Should be ≤ `timeout`.
              0 `perSegmentTimeout` means `no per-segment timeout`.
              Generally, a standard default should be O(X seconds).
              A cluster-wide default value for this query context can be specified via `druid.query.default.context.perSegmentTimeout`.
              """
          )
          .since("35.0.0")
          .defaultDescription("`null`")
          .query(Query.JSON, Query.SQL)
          .engine(Engine.NATIVE)
          .build();

  public static final QueryContextParameter<Long> MAX_SCATTER_GATHER_BYTES =
      longParameter("maxScatterGatherBytes")
          .description(
              """
              Maximum number of bytes gathered from data processes such as Historicals and realtime processes to execute a query.
              This parameter can be used to further reduce `maxScatterGatherBytes` limit at query time.
              See [Broker configuration](../configuration/index.md#broker) for more details.
              """
          )
          .since("0.10.1")
          .defaultDescription("`druid.server.http.maxScatterGatherBytes`")
          .query(Query.JSON, Query.SQL)
          .engine(Engine.NATIVE)
          .build();

  public static final QueryContextParameter<Long> MAX_QUEUED_BYTES =
      longParameter("maxQueuedBytes")
          .description(
              """
              Maximum number of bytes queued per query before exerting backpressure on the channel to the data server.
              Similar to `maxScatterGatherBytes`, except unlike that configuration, this one will trigger backpressure rather than query failure.
              Zero means disabled.
              """
          )
          .since("0.13.0-incubating")
          .defaultDescription("`druid.broker.http.maxQueuedBytes`")
          .query(Query.JSON, Query.SQL)
          .engine(Engine.NATIVE)
          .build();

  public static final QueryContextParameter<Boolean> ENABLE_PARALLEL_MERGE =
      booleanParameter("enableParallelMerge")
          .defaultValue(QueryContexts.DEFAULT_ENABLE_PARALLEL_MERGE)
          .description(
              """
              Enable parallel result merging on the Broker.
              Note that `druid.processing.merge.useParallelMergePool` must be enabled for this setting to be set to `true`.
              See [Broker configuration](../configuration/index.md#broker) for more details.
              """
          )
          .since("0.17.0")
          .query(Query.JSON, Query.SQL)
          .engine(Engine.NATIVE)
          .build();

  public static final QueryContextParameter<Integer> PARALLEL_MERGE_INITIAL_YIELD_ROWS =
      integerParameter("parallelMergeInitialYieldRows")
          .description(
              """
              Number of rows to yield per ForkJoinPool merge task for parallel result merging on the Broker, before forking off a new task to continue merging sequences.
              See [Broker configuration](../configuration/index.md#broker) for more details.
              """
          )
          .since("0.17.0")
          .defaultDescription("`druid.processing.merge.initialYieldNumRows`")
          .query(Query.JSON, Query.SQL)
          .engine(Engine.NATIVE)
          .build();

  public static final QueryContextParameter<Integer> PARALLEL_MERGE_SMALL_BATCH_ROWS =
      integerParameter("parallelMergeSmallBatchRows")
          .description(
              """
              Size of result batches to operate on in ForkJoinPool merge tasks for parallel result merging on the Broker.
              See [Broker configuration](../configuration/index.md#broker) for more details.
              """
          )
          .since("0.17.0")
          .defaultDescription("`druid.processing.merge.smallBatchNumRows`")
          .query(Query.JSON, Query.SQL)
          .engine(Engine.NATIVE)
          .build();

  public static final QueryContextParameter<Integer> PARALLEL_MERGE_PARALLELISM =
      integerParameter("parallelMergeParallelism")
          .description(
              """
              Maximum number of parallel threads to use for parallel result merging on the Broker.
              See [Broker configuration](../configuration/index.md#broker) for more details.
              """
          )
          .since("0.17.0")
          .defaultDescription("`druid.processing.merge.parallelism`")
          .query(Query.JSON, Query.SQL)
          .engine(Engine.NATIVE)
          .build();

  public static final QueryContextParameter<QueryContexts.Vectorize> VECTORIZE =
      enumParameter("vectorize", QueryContexts.Vectorize.class)
          .defaultValue(QueryContexts.DEFAULT_VECTORIZE)
          .description(
              """
              Enables or disables vectorized query execution.
              Possible values are `false` (disabled), `true` (enabled if possible, disabled otherwise, on a per-segment basis), and `force` (enabled, and query types that support vectorization will fail if they cannot be vectorized).
              The `"force"` setting is meant to aid in testing, and is not generally useful in production (since real-time segments can never be processed with vectorized execution, any queries on real-time data will fail).
              This will override `druid.query.default.context.vectorize` if it's set.
              """
          )
          .since("0.16.0-incubating")
          .query(Query.JSON, Query.SQL)
          .engine(Engine.NATIVE)
          .build();

  public static final QueryContextParameter<QueryContexts.Vectorize> VECTORIZE_VIRTUAL_COLUMNS =
      enumParameter("vectorizeVirtualColumns", QueryContexts.Vectorize.class)
          .defaultValue(QueryContexts.DEFAULT_VECTORIZE_VIRTUAL_COLUMN)
          .description(
              """
              Enables or disables vectorized query processing of queries with virtual columns, layered on top of `vectorize` (`vectorize` must also be set to true for a query to utilize vectorization).
              Possible values are `false` (disabled), `true` (enabled if possible, disabled otherwise, on a per-segment basis), and `force` (enabled, and groupBy or timeseries queries with virtual columns that cannot be vectorized will fail).
              The `"force"` setting is meant to aid in testing, and is not generally useful in production.
              This will override `druid.query.default.context.vectorizeVirtualColumns` if it's set.
              """
          )
          .since("0.20.0")
          .query(Query.JSON, Query.SQL)
          .engine(Engine.NATIVE)
          .build();

  public static final QueryContextParameter<Integer> VECTOR_SIZE = integerParameter("vectorSize")
      .defaultValue(QueryContexts.DEFAULT_VECTOR_SIZE)
      .description(
          """
          Sets the row batching size for a particular query.
          This will override `druid.query.default.context.vectorSize` if it's set.
          """
      )
      .since("0.16.0-incubating")
      .query(Query.JSON, Query.SQL)
      .engine(Engine.NATIVE)
      .build();

  public static final QueryContextParameter<Integer> MAX_SUBQUERY_ROWS =
      integerParameter("maxSubqueryRows")
          .description(
              """
              Upper limit on the number of rows a subquery can generate.
              See [Broker configuration](../configuration/index.md#broker) and [subquery guardrails](../configuration/index.md#Guardrails for materialization of subqueries) for more details.
              """
          )
          .since("0.18.0")
          .defaultDescription("`druid.server.http.maxSubqueryRows`")
          .query(Query.JSON, Query.SQL)
          .engine(Engine.NATIVE)
          .build();

  public static final QueryContextParameter<Object> MAX_SUBQUERY_BYTES =
      objectParameter("maxSubqueryBytes")
          .description(
              """
              Upper limit on the number of bytes a subquery can generate.
              See [Broker configuration](../configuration/index.md#broker) and [subquery guardrails](../configuration/index.md#Guardrails for materialization of subqueries) for more details.
              """
          )
          .since("27.0.0")
          .defaultDescription("`druid.server.http.maxSubqueryBytes`")
          .query(Query.JSON, Query.SQL)
          .engine(Engine.NATIVE)
          .build();

  public static final QueryContextParameter<Boolean> ENABLE_JOIN_FILTER_PUSH_DOWN =
      booleanParameter("enableJoinFilterPushDown")
          .defaultValue(QueryContexts.DEFAULT_ENABLE_JOIN_FILTER_PUSH_DOWN)
          .description(
              """
              Controls whether a join query will attempt filter push down, which reduces the number of rows that have to be compared in a join operation.
              """
          )
          .since("0.18.0")
          .query(Query.JSON, Query.SQL)
          .engine(Engine.NATIVE)
          .build();

  public static final QueryContextParameter<Boolean> ENABLE_JOIN_FILTER_REWRITE =
      booleanParameter("enableJoinFilterRewrite")
          .defaultValue(QueryContexts.DEFAULT_ENABLE_JOIN_FILTER_REWRITE)
          .description(
              """
              Controls whether filter clauses that reference non-base table columns will be rewritten into filters on base table columns.
              """
          )
          .since("0.18.0")
          .query(Query.JSON, Query.SQL)
          .engine(Engine.NATIVE)
          .build();

  public static final QueryContextParameter<Boolean> ENABLE_JOIN_FILTER_REWRITE_VALUE_COLUMN_FILTERS =
      booleanParameter("enableJoinFilterRewriteValueColumnFilters")
          .defaultValue(QueryContexts.DEFAULT_ENABLE_JOIN_FILTER_REWRITE_VALUE_COLUMN_FILTERS)
          .description(
              """
              Controls whether Druid rewrites non-base table filters on non-key columns in the non-base table.
              Requires a scan of the non-base table.
              """
          )
          .since("0.18.0")
          .query(Query.JSON, Query.SQL)
          .engine(Engine.NATIVE)
          .build();

  public static final QueryContextParameter<Boolean> ENABLE_REWRITE_JOIN_TO_FILTER =
      booleanParameter("enableRewriteJoinToFilter")
          .defaultValue(QueryContexts.DEFAULT_ENABLE_REWRITE_JOIN_TO_FILTER)
          .description(
              """
              Controls whether a join can be pushed partial or fully to the base table as a filter at runtime.
              """
          )
          .since("0.22.0")
          .query(Query.JSON, Query.SQL)
          .engine(Engine.NATIVE)
          .build();

  public static final QueryContextParameter<Long> JOIN_FILTER_REWRITE_MAX_SIZE =
      longParameter("joinFilterRewriteMaxSize")
          .defaultValue(QueryContexts.DEFAULT_ENABLE_JOIN_FILTER_REWRITE_MAX_SIZE)
          .description(
              """
              The maximum size of the correlated value set used for filter rewrites.
              Set this limit to prevent excessive memory use.
              """
          )
          .since("0.18.0")
          .query(Query.JSON, Query.SQL)
          .engine(Engine.NATIVE)
          .build();

  public static final QueryContextParameter<CloneQueryMode> CLONE_QUERY_MODE =
      enumParameter("cloneQueryMode", CloneQueryMode.class)
          .defaultValue(QueryContexts.DEFAULT_CLONE_QUERY_MODE)
          .description(
              """
              Indicates whether clone Historicals should be queried by brokers.
              Clone servers are created by the `cloneServers` Coordinator dynamic configuration.
              Possible values are `excludeClones`, `includeClones` and `preferClones`.
              `excludeClones` means that clone Historicals are not queried by the broker.
              `preferClones` indicates that when given a choice between the clone Historical and the original Historical which is being cloned, the broker chooses the clones.
              Historicals which are not involved in the cloning process will still be queried.
              `includeClones` means that broker queries any Historical without regarding clone status.
              This parameter only affects native queries.
              MSQ does not query Historicals directly.
              """
          )
          .since("34.0.0")
          .query(Query.JSON)
          .engine(Engine.NATIVE)
          .build();

  public static final QueryContextParameter<Boolean> USE_FILTER_CNF = booleanParameter("useFilterCNF")
      .defaultValue(QueryContexts.DEFAULT_USE_FILTER_CNF)
      .description(
          """
          If true, Druid will attempt to convert the query filter to Conjunctive Normal Form (CNF).
          During query processing, columns can be pre-filtered by intersecting the bitmap indexes of all values that match the eligible filters, often greatly reducing the raw number of rows which need to be scanned.
          But this effect only happens for the top level filter, or individual clauses of a top level 'and' filter.
          As such, filters in CNF potentially have a higher chance to utilize a large amount of bitmap indexes on string columns during pre-filtering.
          However, this setting should be used with great caution, as it can sometimes have a negative effect on performance, and in some cases, the act of computing CNF of a filter can be expensive.
          We recommend hand tuning your filters to produce an optimal form if possible, or at least verifying through experimentation that using this parameter actually improves your query performance with no ill-effects.
          """
      )
      .since("0.19.0")
      .query(Query.JSON, Query.SQL)
      .engine(Engine.NATIVE)
      .build();

  public static final QueryContextParameter<Boolean> USE_CACHE = booleanParameter("useCache")
      .defaultValue(QueryContexts.DEFAULT_USE_CACHE)
      .description(
          """
          Flag indicating whether to leverage the query cache for this query.
          When set to false, it disables reading from the query cache for this query.
          When set to true, Apache Druid uses `druid.broker.cache.useCache` or `druid.historical.cache.useCache` to determine whether or not to read from the query cache.
          """
      )
      .since("0.1.0")
      .query(Query.JSON, Query.SQL)
      .engine(Engine.NATIVE)
      .build();

  public static final QueryContextParameter<Boolean> SECONDARY_PARTITION_PRUNING =
      booleanParameter("secondaryPartitionPruning")
          .defaultValue(QueryContexts.DEFAULT_SECONDARY_PARTITION_PRUNING)
          .description(
              """
              Enable secondary partition pruning on the Broker.
              The Broker will always prune unnecessary segments from the input scan based on a filter on time intervals, but if the data is further partitioned with hash or range partitioning, this option will enable additional pruning based on a filter on secondary partition dimensions.
              """
          )
          .since("0.20.0")
          .query(Query.JSON, Query.SQL)
          .engine(Engine.NATIVE)
          .build();

  public static final QueryContextParameter<Boolean> DEBUG = booleanParameter("debug")
      .defaultValue(QueryContexts.DEFAULT_ENABLE_DEBUG)
      .description(
          """
          Flag indicating whether to enable debugging outputs for the query.
          When set to false, no additional logs will be produced (logs produced will be entirely dependent on your logging level).
          When set to true, the following addition logs will be produced:<br />- Log the stack trace of the exception (if any) produced by the query
          """
      )
      .since("0.22.0")
      .query(Query.JSON, Query.SQL)
      .engine(Engine.NATIVE)
      .build();

  public static final QueryContextParameter<Boolean> SET_PROCESSING_THREAD_NAMES =
      booleanParameter("setProcessingThreadNames")
          .defaultValue(false)
          .description(
              """
              Flag indicating whether processing thread names will be set to `processing_<queryId>` while processing a query.
              Thread renaming aids in interpreting thread dumps, but has measurable thread renaming overhead when segment scans are very quick.
              """
          )
          .since("24.0.0")
          .query(Query.JSON)
          .engine(Engine.NATIVE)
          .build();

  public static final QueryContextParameter<Boolean> BY_SEGMENT = booleanParameter("bySegment")
      .defaultValue(QueryContexts.DEFAULT_BY_SEGMENT)
      .description(
          """
          Native queries only.
          Return "by segment" results.
          Primarily used for debugging, setting it to `true` returns results associated with the data segment they came from.
          """
      )
      .since("0.1.0")
      .query(Query.JSON)
      .engine(Engine.NATIVE)
      .build();

  public static final QueryContextParameter<String> BROKER_SERVICE =
      stringParameter("brokerService")
          .description(
              """
              Broker service to which this query should be routed.
              This parameter is honored only by a broker selector strategy of type *manual*.
              See [Router strategies](../design/router.md#router-strategies) for more details.
              """
          )
          .since("0.22.0")
          .defaultDescription("`null`")
          .query(Query.JSON)
          .engine(Engine.NATIVE)
          .build();


  public static final QueryContextParameter<Boolean> POPULATE_CACHE =
      booleanParameter("populateCache")
          .defaultValue(QueryContexts.DEFAULT_POPULATE_CACHE)
          .description(
              """
              Flag indicating whether to save the results of the query to the query cache.
              Primarily used for debugging.
              When set to false, it disables saving the results of this query to the query cache.
              When set to true, Druid uses `druid.broker.cache.populateCache` or `druid.historical.cache.populateCache` to determine whether or not to save the results of this query to the query cache.
              """
          )
          .since("0.1.0")
          .query(Query.JSON, Query.SQL)
          .engine(Engine.NATIVE)
          .build();

  public static final QueryContextParameter<Boolean> POPULATE_RESULT_LEVEL_CACHE =
      booleanParameter("populateResultLevelCache")
          .defaultValue(QueryContexts.DEFAULT_POPULATE_RESULTLEVEL_CACHE)
          .description(
              """
              Flag indicating whether to save the results of the query to the result level cache.
              Primarily used for debugging.
              When set to false, it disables saving the results of this query to the query cache.
              When set to true, Druid uses `druid.broker.cache.populateResultLevelCache` to determine whether or not to save the results of this query to the result-level query cache.
              """
          )
          .since("0.13.0-incubating")
          .query(Query.JSON, Query.SQL)
          .engine(Engine.NATIVE)
          .build();


  public static final QueryContextParameter<Boolean> SERIALIZE_DATE_TIME_AS_LONG =
      booleanParameter("serializeDateTimeAsLong")
          .description("If true, DateTime is serialized as long in the result returned by Broker and the data transportation between Broker and compute process")
          .since("0.10.1")
          .defaultDescription("`false`")
          .query(Query.JSON, Query.SQL)
          .engine(Engine.NATIVE)
          .build();

  public static final QueryContextParameter<Boolean> SERIALIZE_DATE_TIME_AS_LONG_INNER =
      booleanParameter("serializeDateTimeAsLongInner")
          .description("If true, DateTime is serialized as long in the data transportation between Broker and compute process")
          .since("0.10.1")
          .defaultDescription("`false`")
          .query(Query.JSON, Query.SQL)
          .engine(Engine.NATIVE)
          .build();

  public static final QueryContextParameter<Integer> MIN_TOP_N_THRESHOLD =
      integerParameter("minTopNThreshold")
          .defaultValue(TopNQueryConfig.DEFAULT_MIN_TOPN_THRESHOLD)
          .description("The top minTopNThreshold local results from each segment are returned for merging to determine the global topN.")
          .since("0.9.0")
          .query(Query.JSON, Query.SQL)
          .engine(Engine.NATIVE)
          .queryType(QueryType.TOP_N)
          .build();

  public static final QueryContextParameter<Boolean> SKIP_EMPTY_BUCKETS =
      booleanParameter("skipEmptyBuckets")
          .defaultValue(false)
          .description("Disable timeseries zero-filling behavior, so only buckets with results will be returned.")
          .since("0.7.0")
          .query(Query.JSON)
          .engine(Engine.NATIVE)
          .queryType(QueryType.TIMESERIES)
          .build();

  public static final QueryContextParameter<Boolean> USE_RESULT_LEVEL_CACHE = booleanParameter("useResultLevelCache")
      .defaultValue(true)
      .description(
          """
          Flag indicating whether to leverage the result level cache for this query.
          When set to false, it disables reading from the query cache for this query.
          When set to true, Druid uses `druid.broker.cache.useResultLevelCache` to determine whether or not to read from the result-level query cache.
          """
      )
      .since("0.13.0-incubating")
      .query(Query.JSON, Query.SQL)
      .engine(Engine.NATIVE)
      .build();

  public static final QueryContextParameter<Boolean> REALTIME_SEGMENTS_ONLY =
      booleanParameter("realtimeSegmentsOnly")
          .defaultValue(false)
          .deprecated("Use realtimeSegmentsMode instead.")
          .description(
              """
              **Deprecated.** Use `realtimeSegmentsMode=exclusive` instead.
              When set to `true`, this is equivalent to `realtimeSegmentsMode=exclusive`.
              When set to `false`, this is equivalent to `realtimeSegmentsMode=include`.
              """
          )
          .since("35.0.0")
          .query(Query.JSON, Query.SQL)
          .engine(Engine.NATIVE)
          .build();

  public static final QueryContextParameter<QueryContexts.RealtimeSegmentsMode> REALTIME_SEGMENTS_MODE =
      enumParameter("realtimeSegmentsMode", QueryContexts.RealtimeSegmentsMode.class)
          .defaultValue(QueryContexts.DEFAULT_REALTIME_SEGMENTS_MODE)
          .description(
              """
              Controls whether realtime segments are queried.
              `include` queries all segments, including realtime.
              `exclude` skips realtime segments.
              `exclusive` queries only realtime segments.
              """
          )
          .since("38.0.0")
          .query(Query.JSON, Query.SQL)
          .engine(Engine.NATIVE)
          .build();

  public static final QueryContextParameter<Integer> MAX_ROWS_QUEUED_FOR_ORDERING =
      integerParameter("maxRowsQueuedForOrdering")
          .constraint(Range.closedRange(1, Integer.MAX_VALUE))
          .description(
              """
              The maximum number of rows returned when time ordering is used.
              Overrides the identically named config.
              """
          )
          .since("0.15.0-incubating")
          .defaultDescription("`druid.query.scan.maxRowsQueuedForOrdering`")
          .query(Query.JSON)
          .engine(Engine.NATIVE)
          .queryType(QueryType.SCAN)
          .build();

  private QueryContextParameters()
  {
  }

  /**
   * Immutable query context parameter descriptors indexed by parameter name. Built lazily on first access so that
   * every parameter field is initialized regardless of where it is declared in this class.
   */
  public static final Supplier<Map<String, QueryContextParameter<?>>> ALL =
      Suppliers.memoize(() -> {
        final Map<String, QueryContextParameter<?>> byName = new LinkedHashMap<>();
        for (final Field field : QueryContextParameters.class.getDeclaredFields()) {
          if (!Modifier.isPublic(field.getModifiers())
              || !Modifier.isStatic(field.getModifiers())
              || !QueryContextParameter.class.equals(field.getType())) {
            continue;
          }

          final QueryContextParameter<?> parameter;
          try {
            parameter = (QueryContextParameter<?>) field.get(null);
          }
          catch (final IllegalAccessException e) {
            throw new ISE(e, "Unable to read query context parameter field [%s]", field.getName());
          }

          final QueryContextParameter<?> existing = byName.putIfAbsent(parameter.getName(), parameter);
          if (existing != null) {
            throw new ISE(
                "Duplicate query context parameter [%s] declared by field [%s]",
                parameter.getName(),
                field.getName()
            );
          }
        }
        return Collections.unmodifiableMap(byName);
      });

  static QueryContextParameter.Builder<Boolean> booleanParameter(final String name)
  {
    return QueryContextParameter.builder(
        name,
        Boolean.class,
        value -> {
          if (value instanceof String) {
            // Matches the established QueryContexts coercion: any string other than "true" (ignoring case) is false.
            return Boolean.parseBoolean((String) value);
          }
          throw invalidValueException(name, "a boolean", value);
        }
    );
  }

  static QueryContextParameter.Builder<Integer> integerParameter(final String name)
  {
    return QueryContextParameter.builder(
        name,
        Integer.class,
        value -> QueryContextParameter.exactNumber(
            name,
            value,
            "integer",
            Integer.MIN_VALUE,
            Integer.MAX_VALUE,
            BigDecimal::intValueExact
        )
    );
  }

  static QueryContextParameter.Builder<Long> longParameter(final String name)
  {
    return QueryContextParameter.builder(
        name,
        Long.class,
        value -> QueryContextParameter.exactNumber(
            name,
            value,
            "long",
            Long.MIN_VALUE,
            Long.MAX_VALUE,
            BigDecimal::longValueExact
        )
    );
  }

  static QueryContextParameter.Builder<String> stringParameter(final String name)
  {
    return QueryContextParameter.builder(
        name,
        String.class,
        value -> {
          throw invalidValueException(name, "a string", value);
        }
    );
  }

  static <T extends Enum<T>> QueryContextParameter.Builder<T> enumParameter(
      final String name,
      final Class<T> valueType
  )
  {
    return QueryContextParameter.builder(name, valueType, value -> QueryContexts.getAsEnum(name, value, valueType));
  }

  static QueryContextParameter.Builder<Object> objectParameter(final String name)
  {
    return QueryContextParameter.builder(name, Object.class, value -> value);
  }

  /**
   * Creates the exception that parsers throw for a value that cannot be converted to the parameter type.
   *
   * @param expected description of the expected value that completes "should be ...", such as "in integer format"
   */
  public static BadQueryContextException invalidValueException(
      final String name,
      final String expected,
      final Object actual
  )
  {
    return new BadQueryContextException(
        StringUtils.format("Query context parameter [%s] should be %s, but got [%s]", name, expected, actual)
    );
  }
}
