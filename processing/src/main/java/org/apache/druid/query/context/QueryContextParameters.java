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
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.BadQueryContextException;
import org.apache.druid.query.CloneQueryMode;
import org.apache.druid.query.ExecutionMode;
import org.apache.druid.query.JoinAlgorithm;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.context.QueryContextParameter.Visibility;
import org.apache.druid.query.context.constraint.Range;
import org.apache.druid.query.context.docs.ParameterDocumentation.Engine;
import org.apache.druid.query.context.docs.ParameterDocumentation.Query;
import org.apache.druid.query.context.docs.ParameterDocumentation.QueryType;
import org.apache.druid.query.context.docs.ParameterDocumentation.StatementType;
import org.apache.druid.query.topn.TopNQueryConfig;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;

/// Central catalog of query context parameter descriptors.
///
/// Define user-facing metadata here, including descriptions, default values, constraints,
/// applicability, and introduction versions. The
/// {@link org.apache.druid.query.context.docs.ParameterDocumentationGenerator} generates a table row
/// for every public parameter wherever a managed document has a marker for it.
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
  public static final QueryContextParameter<Boolean> ALLOW_NATIVE_FUNCTIONS_FOR_SYSTEM_TABLES =
      booleanParameter("allowNativeFunctionsForSystemTables")
          .defaultValue(false)
          .description(
              """
              Experimental. Allows supported [system tables](sql-metadata-tables.md#native-query-execution) to use native query
              functions and operators instead of Calcite `Bindable` execution when the resolved SQL engine is `native`.
              System tables without native query support continue to use their existing execution path.<br /><br />
              During a rolling upgrade, leave this parameter disabled until the Broker and the components that serve
              the native system tables have been upgraded. You can set a cluster-wide default on Brokers with
              `druid.query.default.context.allowNativeFunctionsForSystemTables`.
              """
          )
          .since("39.0.0")
          .query(Query.SQL)
          .engine(Engine.NATIVE)
          .build();

  public static final QueryContextParameter<String> QUERY_ID = stringParameter("queryId")
      .since("0.6.62").query(Query.JSON, Query.SQL).engine(Engine.NATIVE, Engine.MSQ, Engine.DART)
      .description(
          """
          Unique identifier given to this query.
          If a query ID is set or known, this can be used to cancel the query.
          """
      )
      .defaultDescription("auto-generated")
      .build();

  public static final QueryContextParameter<Boolean> FINALIZE = booleanParameter("finalize")
      .since("0.1.0").query(Query.JSON, Query.SQL).engine(Engine.NATIVE)
      .defaultDescription("`N/A`")
      .description(
          """
          Flag indicating whether to "finalize" aggregation results.
          Primarily used for debugging.
          For instance, the `hyperUnique` aggregator returns the full HyperLogLog sketch instead of the estimated
          cardinality when this flag is set to `false`.
          """
      )
      .build();

  public static final QueryContextParameter<Integer> PRIORITY = integerParameter("priority")
      .since("0.5.0").query(Query.JSON, Query.SQL).engine(Engine.NATIVE)
      .defaultValue(QueryContexts.DEFAULT_PRIORITY)
      .description(
          """
          Query priority.
          Queries with higher priority get precedence for computational resources.
          """
      )
      .defaultDescription(
          """
          The default priority is one of the following: <ul><li>Value of `priority` in the query context, if
          set</li><li>The value of the runtime property `druid.query.default.context.priority`, if set and
          not null</li><li>`0` if the priority is not set in the query context or runtime properties</li></ul>
          """
      )
      .build();

  public static final QueryContextParameter<String> LANE = stringParameter("lane")
      .since("0.18.0").query(Query.JSON, Query.SQL).engine(Engine.NATIVE)
      .description(
          """
          Query lane, used to control usage limits on classes of queries.
          See [Broker configuration](../configuration/index.md#broker) for more details.
          """
      )
      .defaultDescription("`null`")
      .build();

  public static final QueryContextParameter<Long> TIMEOUT = longParameter("timeout")
      .since("0.6.122").query(Query.JSON, Query.SQL).engine(Engine.NATIVE)
      .description(
          """
          Query timeout in millis, beyond which unfinished queries will be cancelled.
          0 timeout means `no timeout` (up to the server-side maximum query timeout, `druid.server.http.maxQueryTimeout`).
          To set the default timeout and maximum timeout, see [Broker configuration](../configuration/index.md#broker).
          """
      )
      .defaultDescription("`druid.server.http.defaultQueryTimeout`")
      .build();

  public static final QueryContextParameter<Long> PER_SEGMENT_TIMEOUT =
      longParameter("perSegmentTimeout")
          .since("35.0.0").query(Query.JSON, Query.SQL).engine(Engine.NATIVE)
          .description(
              """
              Per-segment processing timeout in millis, beyond which unfinished queries will be cancelled.
              Should be ≤ `timeout`.
              0 `perSegmentTimeout` means `no per-segment timeout`.
              Generally, a standard default should be O(X seconds).
              A cluster-wide default value for this query context can be specified via `druid.query.default.context.perSegmentTimeout`.
              """
          )
          .defaultDescription("`null`")
          .build();

  public static final QueryContextParameter<Long> MAX_SCATTER_GATHER_BYTES =
      longParameter("maxScatterGatherBytes")
          .since("0.10.1").query(Query.JSON, Query.SQL).engine(Engine.NATIVE)
          .description(
              """
              Maximum number of bytes gathered from data processes such as Historicals and realtime processes to execute a query.
              This parameter can be used to further reduce `maxScatterGatherBytes` limit at query time.
              See [Broker configuration](../configuration/index.md#broker) for more details.
              """
          )
          .defaultDescription("`druid.server.http.maxScatterGatherBytes`")
          .build();

  public static final QueryContextParameter<Long> MAX_QUEUED_BYTES =
      longParameter("maxQueuedBytes")
          .since("0.13.0-incubating").query(Query.JSON, Query.SQL).engine(Engine.NATIVE)
          .description(
              """
              Maximum number of bytes queued per query before exerting backpressure on the channel to the data server.
              Similar to `maxScatterGatherBytes`, except unlike that configuration, this one will trigger backpressure rather than query failure.
              Zero means disabled.
              """
          )
          .defaultDescription("`druid.broker.http.maxQueuedBytes`")
          .build();

  public static final QueryContextParameter<Long> DEFAULT_TIMEOUT = longParameter("defaultTimeout")
      .since("0.10.1").query(Query.JSON, Query.SQL).engine(Engine.NATIVE)
      .defaultValue(TimeUnit.MINUTES.toMillis(5))
      .description(
          """
          Timeout in milliseconds that applies when the query doesn't set `timeout`.
          Druid sets it to the lower of `druid.server.http.defaultQueryTimeout` and `druid.server.http.maxQueryTimeout`.
          """
      )
      .visibility(Visibility.SYSTEM)
      .build();

  public static final QueryContextParameter<Boolean> ENABLE_PARALLEL_MERGE =
      booleanParameter("enableParallelMerge")
          .since("0.17.0").query(Query.JSON, Query.SQL).engine(Engine.NATIVE)
          .defaultValue(QueryContexts.DEFAULT_ENABLE_PARALLEL_MERGE)
          .description(
              """
              Enable parallel result merging on the Broker.
              Note that `druid.processing.merge.useParallelMergePool` must be enabled for this setting to be set to `true`.
              See [Broker configuration](../configuration/index.md#broker) for more details.
              """
          )
          .build();

  public static final QueryContextParameter<Integer> PARALLEL_MERGE_INITIAL_YIELD_ROWS =
      integerParameter("parallelMergeInitialYieldRows")
          .since("0.17.0").query(Query.JSON, Query.SQL).engine(Engine.NATIVE)
          .description(
              """
              Number of rows to yield per ForkJoinPool merge task for parallel result merging on the Broker, before forking off a new task to continue merging sequences.
              See [Broker configuration](../configuration/index.md#broker) for more details.
              """
          )
          .defaultDescription("`druid.processing.merge.initialYieldNumRows`")
          .build();

  public static final QueryContextParameter<Integer> PARALLEL_MERGE_SMALL_BATCH_ROWS =
      integerParameter("parallelMergeSmallBatchRows")
          .since("0.17.0").query(Query.JSON, Query.SQL).engine(Engine.NATIVE)
          .description(
              """
              Size of result batches to operate on in ForkJoinPool merge tasks for parallel result merging on the Broker.
              See [Broker configuration](../configuration/index.md#broker) for more details.
              """
          )
          .defaultDescription("`druid.processing.merge.smallBatchNumRows`")
          .build();

  public static final QueryContextParameter<Integer> PARALLEL_MERGE_PARALLELISM =
      integerParameter("parallelMergeParallelism")
          .since("0.17.0").query(Query.JSON, Query.SQL).engine(Engine.NATIVE)
          .description(
              """
              Maximum number of parallel threads to use for parallel result merging on the Broker.
              See [Broker configuration](../configuration/index.md#broker) for more details.
              """
          )
          .defaultDescription("`druid.processing.merge.parallelism`")
          .build();

  public static final QueryContextParameter<QueryContexts.Vectorize> VECTORIZE =
      enumParameter("vectorize", QueryContexts.Vectorize.class)
          .since("0.16.0-incubating").query(Query.JSON, Query.SQL).engine(Engine.NATIVE)
          .defaultValue(QueryContexts.DEFAULT_VECTORIZE)
          .description(
              """
              Enables or disables vectorized query execution.
              Possible values are `false` (disabled), `true` (enabled if possible, disabled otherwise, on a per-segment basis), and `force` (enabled, and query types that support vectorization will fail if they cannot be vectorized).
              The `"force"` setting is meant to aid in testing, and is not generally useful in production (since real-time segments can never be processed with vectorized execution, any queries on real-time data will fail).
              This will override `druid.query.default.context.vectorize` if it's set.
              """
          )
          .build();

  public static final QueryContextParameter<QueryContexts.Vectorize> VECTORIZE_VIRTUAL_COLUMNS =
      enumParameter("vectorizeVirtualColumns", QueryContexts.Vectorize.class)
          .since("0.20.0").query(Query.JSON, Query.SQL).engine(Engine.NATIVE)
          .defaultValue(QueryContexts.DEFAULT_VECTORIZE_VIRTUAL_COLUMN)
          .description(
              """
              Enables or disables vectorized query processing of queries with virtual columns, layered on top of `vectorize` (`vectorize` must also be set to true for a query to utilize vectorization).
              Possible values are `false` (disabled), `true` (enabled if possible, disabled otherwise, on a per-segment basis), and `force` (enabled, and groupBy or timeseries queries with virtual columns that cannot be vectorized will fail).
              The `"force"` setting is meant to aid in testing, and is not generally useful in production.
              This will override `druid.query.default.context.vectorizeVirtualColumns` if it's set.
              """
          )
          .build();

  public static final QueryContextParameter<Integer> VECTOR_SIZE = integerParameter("vectorSize")
      .since("0.16.0-incubating").query(Query.JSON, Query.SQL).engine(Engine.NATIVE)
      .defaultValue(QueryContexts.DEFAULT_VECTOR_SIZE)
      .description(
          """
          Sets the row batching size for a particular query.
          This will override `druid.query.default.context.vectorSize` if it's set.
          """
      )
      .build();

  public static final QueryContextParameter<Integer> MAX_SUBQUERY_ROWS =
      integerParameter("maxSubqueryRows")
          .since("0.18.0").query(Query.JSON, Query.SQL).engine(Engine.NATIVE)
          .description(
              """
              Upper limit on the number of rows a subquery can generate.
              See [Broker configuration](../configuration/index.md#broker) and [subquery guardrails](../configuration/index.md#Guardrails for materialization of subqueries) for more details.
              """
          )
          .defaultDescription("`druid.server.http.maxSubqueryRows`")
          .build();

  /**
   * The Broker's subquery guardrail resolves the value, computing the limit for {@code auto} from its heap size and
   * lookups. This descriptor only checks the form of the value: {@code auto}, {@code disabled} or a non-negative number
   * of bytes. Numbers are converted to the string form that the guardrail reads.
   */
  public static final QueryContextParameter<String> MAX_SUBQUERY_BYTES =
      QueryContextParameter
          .builder("maxSubqueryBytes", String.class, QueryContextParameters::parseMaxSubqueryBytes)
          .constraint(QueryContextParameters::validateMaxSubqueryBytes)
          .since("27.0.0").query(Query.JSON, Query.SQL).engine(Engine.NATIVE)
          .description(
              """
              Upper limit on the number of bytes a subquery can generate.
              Accepts `auto`, `disabled`, or a non-negative number of bytes.
              See [Broker configuration](../configuration/index.md#broker) and [subquery guardrails](../configuration/index.md#Guardrails for materialization of subqueries) for more details.
              """
          )
          .defaultDescription("`druid.server.http.maxSubqueryBytes`")
          .build();

  public static final QueryContextParameter<Boolean> USE_NESTED_FOR_UNKNOWN_TYPE_IN_SUBQUERY =
      booleanParameter("useNestedForUnknownTypeInSubquery")
          .since("27.0.0").query(Query.JSON, Query.SQL).engine(Engine.NATIVE)
          .defaultValue(QueryContexts.DEFAULT_USE_NESTED_FOR_UNKNOWN_TYPE_IN_SUBQUERY)
          .description(
              """
              If true, when the Broker materializes subquery results as frames to enforce `maxSubqueryBytes`,
              it serializes columns of unknown type as nested JSON instead of failing to materialize them.
              """
          )
          .defaultDescription("`druid.server.http.useNestedForUnknownTypeInSubquery`")
          .build();

  public static final QueryContextParameter<Boolean> ENABLE_JOIN_FILTER_PUSH_DOWN =
      booleanParameter("enableJoinFilterPushDown")
          .since("0.18.0").query(Query.JSON, Query.SQL).engine(Engine.NATIVE)
          .defaultValue(QueryContexts.DEFAULT_ENABLE_JOIN_FILTER_PUSH_DOWN)
          .description(
              """
              Controls whether a join query will attempt filter push down, which reduces the number of rows that have to be compared in a join operation.
              """
          )
          .build();

  public static final QueryContextParameter<Boolean> ENABLE_JOIN_FILTER_REWRITE =
      booleanParameter("enableJoinFilterRewrite")
          .since("0.18.0").query(Query.JSON, Query.SQL).engine(Engine.NATIVE)
          .defaultValue(QueryContexts.DEFAULT_ENABLE_JOIN_FILTER_REWRITE)
          .description(
              """
              Controls whether filter clauses that reference non-base table columns will be rewritten into filters on base table columns.
              """
          )
          .build();

  public static final QueryContextParameter<Boolean> ENABLE_JOIN_FILTER_REWRITE_VALUE_COLUMN_FILTERS =
      booleanParameter("enableJoinFilterRewriteValueColumnFilters")
          .since("0.18.0").query(Query.JSON, Query.SQL).engine(Engine.NATIVE)
          .defaultValue(QueryContexts.DEFAULT_ENABLE_JOIN_FILTER_REWRITE_VALUE_COLUMN_FILTERS)
          .description(
              """
              Controls whether Druid rewrites non-base table filters on non-key columns in the non-base table.
              Requires a scan of the non-base table.
              """
          )
          .build();

  public static final QueryContextParameter<Boolean> ENABLE_REWRITE_JOIN_TO_FILTER =
      booleanParameter("enableRewriteJoinToFilter")
          .since("0.22.0").query(Query.JSON, Query.SQL).engine(Engine.NATIVE)
          .defaultValue(QueryContexts.DEFAULT_ENABLE_REWRITE_JOIN_TO_FILTER)
          .description(
              """
              Controls whether a join can be pushed partial or fully to the base table as a filter at runtime.
              """
          )
          .build();

  public static final QueryContextParameter<Long> JOIN_FILTER_REWRITE_MAX_SIZE =
      longParameter("joinFilterRewriteMaxSize")
          .since("0.18.0").query(Query.JSON, Query.SQL).engine(Engine.NATIVE)
          .defaultValue(QueryContexts.DEFAULT_ENABLE_JOIN_FILTER_REWRITE_MAX_SIZE)
          .description(
              """
              The maximum size of the correlated value set used for filter rewrites.
              Set this limit to prevent excessive memory use.
              """
          )
          .build();

  public static final QueryContextParameter<Boolean> CURSOR_AUTO_ARRANGE_FILTERS =
      booleanParameter("cursorAutoArrangeFilters")
          .since("32.0.0").query(Query.JSON, Query.SQL).engine(Engine.NATIVE, Engine.MSQ, Engine.DART)
          .defaultValue(true)
          .description(
              """
              If `true`, Druid orders the children of `AND` and `OR` filters by their estimated index computation cost
              before building filter bundles for a segment cursor.
              """
          )
          .visibility(Visibility.HIDDEN)
          .build();

  public static final QueryContextParameter<CloneQueryMode> CLONE_QUERY_MODE =
      enumParameter("cloneQueryMode", CloneQueryMode.class)
          .since("34.0.0").query(Query.JSON, Query.SQL).engine(Engine.NATIVE)
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
          .build();

  public static final QueryContextParameter<Boolean> OPTIMIZE_AGGREGATORS =
      booleanParameter("optimizeAggregators")
          .since("38.0.0").query(Query.JSON, Query.SQL).engine(Engine.NATIVE, Engine.MSQ, Engine.DART)
          .defaultValue(QueryContexts.DEFAULT_OPTIMIZE_AGGREGATORS)
          .description(
              """
              Whether Druid applies per-segment aggregator optimizations.
              Set to `false` to switch off the optimizations, for example when debugging them.
              """
          )
          .visibility(Visibility.HIDDEN)
          .build();

  public static final QueryContextParameter<Boolean> USE_FILTER_CNF = booleanParameter("useFilterCNF")
      .since("0.19.0").query(Query.JSON, Query.SQL).engine(Engine.NATIVE)
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
      .build();

  public static final QueryContextParameter<Integer> NUM_RETRIES_ON_MISSING_SEGMENTS =
      integerParameter("numRetriesOnMissingSegments")
          .since("0.20.0").query(Query.JSON, Query.SQL).engine(Engine.NATIVE, Engine.MSQ)
          .description(
              """
              Maximum number of times Druid retries a query for segments that a data server reports as missing.
              For native queries, this overrides `druid.broker.retryPolicy.numTries`.
              """
          )
          .defaultDescription("`druid.broker.retryPolicy.numTries` for native queries, `3` for MSQ tasks")
          .build();

  public static final QueryContextParameter<Boolean> RETURN_PARTIAL_RESULTS =
      booleanParameter("returnPartialResults")
          .since("0.20.0").query(Query.JSON, Query.SQL).engine(Engine.NATIVE)
          .description(
              """
              If true, the Broker returns partial results when segments are still missing after all retries.
              If false, the query fails instead.
              """
          )
          .defaultDescription("`false`")
          .build();

  public static final QueryContextParameter<Boolean> USE_CACHE = booleanParameter("useCache")
      .since("0.1.0").query(Query.JSON, Query.SQL).engine(Engine.NATIVE)
      .defaultValue(QueryContexts.DEFAULT_USE_CACHE)
      .description(
          """
          Flag indicating whether to leverage the query cache for this query.
          When set to false, it disables reading from the query cache for this query.
          When set to true, Apache Druid uses `druid.broker.cache.useCache` or `druid.historical.cache.useCache` to determine whether or not to read from the query cache.
          """
      )
      .build();

  public static final QueryContextParameter<Boolean> SECONDARY_PARTITION_PRUNING =
      booleanParameter("secondaryPartitionPruning")
          .since("0.20.0").query(Query.JSON, Query.SQL).engine(Engine.NATIVE)
          .defaultValue(QueryContexts.DEFAULT_SECONDARY_PARTITION_PRUNING)
          .description(
              """
              Enable secondary partition pruning on the Broker.
              The Broker will always prune unnecessary segments from the input scan based on a filter on time intervals, but if the data is further partitioned with hash or range partitioning, this option will enable additional pruning based on a filter on secondary partition dimensions.
              """
          )
          .build();

  public static final QueryContextParameter<Boolean> DEBUG = booleanParameter("debug")
      .since("0.22.0").query(Query.JSON, Query.SQL).engine(Engine.NATIVE)
      .defaultValue(QueryContexts.DEFAULT_ENABLE_DEBUG)
      .description(
          """
          Flag indicating whether to enable debugging outputs for the query.
          When set to false, no additional logs will be produced (logs produced will be entirely dependent on your logging level).
          When set to true, the following addition logs will be produced:<br />- Log the stack trace of the exception (if any) produced by the query
          """
      )
      .build();

  public static final QueryContextParameter<Boolean> SET_PROCESSING_THREAD_NAMES =
      booleanParameter("setProcessingThreadNames")
          .since("24.0.0").query(Query.JSON).engine(Engine.NATIVE)
          .defaultValue(false)
          .description(
              """
              Flag indicating whether processing thread names will be set to `processing_<queryId>` while processing a query.
              Thread renaming aids in interpreting thread dumps, but has measurable thread renaming overhead when segment scans are very quick.
              """
          )
          .build();

  public static final QueryContextParameter<Boolean> BY_SEGMENT = booleanParameter("bySegment")
      .since("0.1.0").query(Query.JSON).engine(Engine.NATIVE)
      .defaultValue(QueryContexts.DEFAULT_BY_SEGMENT)
      .description(
          """
          Native queries only.
          Return "by segment" results.
          Primarily used for debugging, setting it to `true` returns results associated with the data segment they came from.
          """
      )
      .build();

  public static final QueryContextParameter<String> BROKER_SERVICE =
      stringParameter("brokerService")
          .since("0.22.0").query(Query.JSON, Query.SQL).engine(Engine.NATIVE)
          .description(
              """
              Broker service to which this query should be routed.
              This parameter is honored only by a broker selector strategy of type *manual*.
              See [Router strategies](../design/router.md#router-strategies) for more details.
              """
          )
          .defaultDescription("`null`")
          .build();

  public static final QueryContextParameter<Boolean> POPULATE_CACHE =
      booleanParameter("populateCache")
          .since("0.1.0").query(Query.JSON, Query.SQL).engine(Engine.NATIVE)
          .defaultValue(QueryContexts.DEFAULT_POPULATE_CACHE)
          .description(
              """
              Flag indicating whether to save the results of the query to the query cache.
              Primarily used for debugging.
              When set to false, it disables saving the results of this query to the query cache.
              When set to true, Druid uses `druid.broker.cache.populateCache` or `druid.historical.cache.populateCache` to determine whether or not to save the results of this query to the query cache.
              """
          )
          .build();

  public static final QueryContextParameter<Boolean> POPULATE_RESULT_LEVEL_CACHE =
      booleanParameter("populateResultLevelCache")
          .since("0.13.0-incubating").query(Query.JSON, Query.SQL).engine(Engine.NATIVE)
          .defaultValue(QueryContexts.DEFAULT_POPULATE_RESULTLEVEL_CACHE)
          .description(
              """
              Flag indicating whether to save the results of the query to the result level cache.
              Primarily used for debugging.
              When set to false, it disables saving the results of this query to the query cache.
              When set to true, Druid uses `druid.broker.cache.populateResultLevelCache` to determine whether or not to save the results of this query to the result-level query cache.
              """
          )
          .build();

  public static final QueryContextParameter<Boolean> SERIALIZE_DATE_TIME_AS_LONG =
      booleanParameter("serializeDateTimeAsLong")
          .since("0.10.1").query(Query.JSON, Query.SQL).engine(Engine.NATIVE)
          .description("If true, DateTime is serialized as long in the result returned by Broker and the data transportation between Broker and compute process")
          .defaultDescription("`false`")
          .build();

  public static final QueryContextParameter<Boolean> SERIALIZE_DATE_TIME_AS_LONG_INNER =
      booleanParameter("serializeDateTimeAsLongInner")
          .since("0.10.1").query(Query.JSON, Query.SQL).engine(Engine.NATIVE)
          .description("If true, DateTime is serialized as long in the data transportation between Broker and compute process")
          .defaultDescription("`false`")
          .build();

  public static final QueryContextParameter<Integer> UNCOVERED_INTERVALS_LIMIT =
      integerParameter("uncoveredIntervalsLimit")
          .since("0.9.0").query(Query.JSON, Query.SQL).engine(Engine.NATIVE)
          .defaultValue(QueryContexts.DEFAULT_UNCOVERED_INTERVALS_LIMIT)
          .description(
              """
              Maximum number of query intervals not covered by any segment that the Broker reports in the
              `uncoveredIntervals` entry of the response context.
              If more intervals are uncovered, the Broker also sets `uncoveredIntervalsOverflowed` to true.
              A value of 0 disables the check.
              """
          )
          .build();

  public static final QueryContextParameter<Integer> MIN_TOP_N_THRESHOLD =
      integerParameter("minTopNThreshold")
          .since("0.9.0").query(Query.JSON, Query.SQL).engine(Engine.NATIVE)
          .defaultValue(TopNQueryConfig.DEFAULT_MIN_TOPN_THRESHOLD)
          .description("The top minTopNThreshold local results from each segment are returned for merging to determine the global topN.")
          .queryType(QueryType.TOP_N)
          .build();

  public static final QueryContextParameter<Boolean> SKIP_EMPTY_BUCKETS =
      booleanParameter("skipEmptyBuckets")
          .since("0.7.0").query(Query.JSON).engine(Engine.NATIVE)
          .defaultValue(false)
          .description("Disable timeseries zero-filling behavior, so only buckets with results will be returned.")
          .queryType(QueryType.TIMESERIES)
          .build();

  public static final QueryContextParameter<String> ENGINE = stringParameter("engine")
      .since("34.0.0").query(Query.SQL).engine(Engine.NATIVE, Engine.DART)
      .defaultValue(QueryContexts.DEFAULT_ENGINE)
      .description(
          """
          SQL engine that runs the query through the `/druid/v2/sql` endpoint.
          Set to `msq-dart` to run the query with [Dart](dart.md).
          Druid ignores this parameter for JDBC connections.
          """
      )
      .build();

  public static final QueryContextParameter<Boolean> USE_TOPN_MULTI_PASS_POOLED_QUERY_GRANULARITY =
      booleanParameter("useTopNMultiPassPooledQueryGranularity")
          .since("32.0.0").query(Query.JSON, Query.SQL).engine(Engine.NATIVE)
          .defaultValue(false)
          .description(
              """
              Whether the TopN engine may use the pooled algorithm with multiple cursor passes when the query granularity
              isn't `all` and the result buffer is too small for a single pass.
              This is likely slower than the default fallback to heap memory, but is less likely to run out of heap.
              """
          )
          .queryType(QueryType.TOP_N)
          .visibility(Visibility.HIDDEN)
          .build();

  public static final QueryContextParameter<Boolean> USE_RESULT_LEVEL_CACHE = booleanParameter("useResultLevelCache")
      .since("0.13.0-incubating").query(Query.JSON, Query.SQL).engine(Engine.NATIVE)
      .defaultValue(true)
      .description(
          """
          Flag indicating whether to leverage the result level cache for this query.
          When set to false, it disables reading from the query cache for this query.
          When set to true, Druid uses `druid.broker.cache.useResultLevelCache` to determine whether or not to read from the result-level query cache.
          """
      )
      .build();

  public static final QueryContextParameter<Boolean> NO_PROJECTIONS =
      booleanParameter("noProjections")
          .since("32.0.0").query(Query.JSON, Query.SQL).engine(Engine.NATIVE, Engine.MSQ, Engine.DART)
          .defaultValue(false)
          .description("If `true`, the query engine doesn't use any [projections](projections.md).")
          .build();

  public static final QueryContextParameter<Boolean> FORCE_PROJECTIONS =
      booleanParameter("forceProjections")
          .since("32.0.0").query(Query.JSON, Query.SQL).engine(Engine.NATIVE, Engine.MSQ, Engine.DART)
          .defaultValue(false)
          .description(
              """
              If `true`, the query engine must use a [projection](projections.md), and the query fails if no projection matches it.
              If `false`, Druid uses a matching projection when one exists and otherwise processes the query as usual.
              """
          )
          .build();

  public static final QueryContextParameter<String> USE_PROJECTION =
      stringParameter("useProjection")
          .since("32.0.0").query(Query.JSON, Query.SQL).engine(Engine.NATIVE, Engine.MSQ, Engine.DART)
          .description(
              """
              Name of a [projection](projections.md) that the query engine must use.
              If the projection doesn't match the query, the query fails.
              """
          )
          .build();

  public static final QueryContextParameter<String> QUERY_RESOURCE_ID =
      stringParameter("queryResourceId")
          .since("30.0.0").query(Query.JSON, Query.SQL).engine(Engine.NATIVE)
          .description(
              """
              Unique identifier that maps a query to the shared resources, such as merge buffers, reserved for it at runtime.
              The Broker assigns a random value to every query.
              """
          )
          .visibility(Visibility.SYSTEM)
          .build();

  //
  // SQL planner parameters.
  //
  // The descriptors below are read by the SQL planner: PlannerContext, PlannerConfig, the planner rules and the SQL
  // validator. Descriptors that apply only to SQL are documented in sql-query-context.md.
  public static final QueryContextParameter<String> SQL_QUERY_ID =
      stringParameter("sqlQueryId")
          .since("0.14.0-incubating").query(Query.SQL).engine(Engine.NATIVE, Engine.MSQ, Engine.DART)
          .description(
              """
              SQL query ID.
              For HTTP client, Druid returns it in the `X-Druid-SQL-Query-Id` header.<br/><br/>To specify a SQL query ID, use `sqlQueryId` instead of [`queryId`](query-context-reference.md).
              Setting `queryId` for a SQL request has no effect.
              All native queries underlying SQL use an auto-generated `queryId`.
              """
          )
          .defaultDescription("auto-generated")
          .build();

  public static final QueryContextParameter<Boolean> SQL_STRINGIFY_ARRAYS =
      booleanParameter("sqlStringifyArrays")
          .since("0.22.0").query(Query.SQL).engine(Engine.NATIVE, Engine.MSQ, Engine.DART)
          .description("If `true`, Druid serializes result columns with array values as JSON strings in the response instead of arrays.")
          .defaultDescription("`true`, except for JDBC connections, where it's always `false`")
          .build();

  public static final QueryContextParameter<Integer> SQL_PLANNER_BLOAT =
      integerParameter("sqlPlannerBloat")
          .since("31.0.1").query(Query.SQL).engine(Engine.NATIVE, Engine.MSQ, Engine.DART)
          .defaultValue(1000)
          .description(
              """
              Calcite parameter which controls whether to merge two Project operators when inlining expressions causes complexity to increase.
              Implemented as a workaround to exception `There are not enough rules to produce a node with desired properties: convention=DRUID, sort=[]` thrown after rejecting the merge of two projects.
              """
          )
          .build();

  public static final QueryContextParameter<String> SQL_TIME_ZONE =
      // Unlike stringParameter, non-string values such as a JSON number are converted with String.valueOf rather than rejected.
      // This keeps the behavior from before sqlTimeZone had a descriptor, when PlannerContext stringified any value it found.
      // The string is then resolved by DateTimes.inferTzFromString.
      QueryContextParameter.builder("sqlTimeZone", String.class, String::valueOf)
          .since("0.10.0").query(Query.SQL).engine(Engine.NATIVE, Engine.MSQ, Engine.DART)
          .description(
              """
              Time zone for a connection.
              For example, `America/Los_Angeles` or an offset like `-08:00`.
              This parameter affects how time functions and timestamp literals behave.
              """
          )
          .defaultDescription("UTC")
          .build();

  public static final QueryContextParameter<Boolean> USE_APPROXIMATE_COUNT_DISTINCT =
      booleanParameter("useApproximateCountDistinct")
          .since("0.10.0").query(Query.SQL).engine(Engine.NATIVE, Engine.MSQ, Engine.DART)
          .description("Whether to use an approximate cardinality algorithm for `COUNT(DISTINCT foo)`.")
          .defaultDescription("`true`")
          .build();

  public static final QueryContextParameter<Boolean> USE_GROUPING_SET_FOR_EXACT_DISTINCT =
      booleanParameter("useGroupingSetForExactDistinct")
          .since("0.22.0").query(Query.SQL).engine(Engine.NATIVE)
          .description("Whether to use grouping sets to execute queries with multiple exact distinct aggregations.")
          .defaultDescription("`false`")
          .build();

  public static final QueryContextParameter<Boolean> USE_APPROXIMATE_TOP_N =
      booleanParameter("useApproximateTopN")
          .since("0.10.0").query(Query.SQL).engine(Engine.NATIVE)
          .description(
              """
              If `true`, Druid converts SQL queries to approximate [TopN queries](topnquery.md) wherever possible.
              If `false`, Druid uses exact [GroupBy queries](groupbyquery.md) instead.
              """
          )
          .defaultDescription("`true`")
          .build();

  public static final QueryContextParameter<Boolean> USE_LEXICOGRAPHIC_TOP_N =
      booleanParameter("useLexicographicTopN")
          .since("35.0.1").query(Query.SQL).engine(Engine.NATIVE)
          .description(
              """
              If `true`, Druid can use [TopN queries](topnquery.md) with lexicographic dimension ordering.
              If `false`, Druid uses [GroupBy queries](groupbyquery.md) instead for lexicographic ordering.
              When both `useLexicographicTopN` and `useApproximateTopN` are `false`, TopN queries are never used.
              """
          )
          .defaultDescription("`false`")
          .build();

  public static final QueryContextParameter<Boolean> USE_NATIVE_QUERY_EXPLAIN =
      booleanParameter("useNativeQueryExplain")
          .since("0.23.0").query(Query.SQL).engine(Engine.NATIVE, Engine.MSQ, Engine.DART)
          .description(
              """
              If `true`, `EXPLAIN PLAN FOR` returns the explain plan as a JSON representation of equivalent native query,
              else it returns the original version of explain plan generated by Calcite.\
              <br /><br />This property is provided for backwards compatibility.
              We don't recommend setting this parameter unless your application depends on the older behavior.
              """
          )
          .defaultDescription("`true`")
          .build();

  public static final QueryContextParameter<Boolean> SQL_FINALIZE_OUTER_SKETCHES =
      booleanParameter("sqlFinalizeOuterSketches")
          .since("25.0.0").query(Query.SQL).engine(Engine.NATIVE, Engine.MSQ, Engine.DART)
          .defaultValue(false)
          .description(
              """
              If `false` (default behavior in Druid 25.0.0 and later), `DS_HLL`, `DS_THETA`, and `DS_QUANTILES_SKETCH` return sketches in query results.
              If `true` (default behavior in Druid 24.0.1 and earlier), Druid finalizes sketches from these functions when they appear in query results.\
              <br /><br />This property is provided for backwards compatibility with behavior in Druid 24.0.1 and earlier.
              We don't recommend setting this parameter unless your application uses Druid 24.0.1 or earlier.
              Instead, use a function that doesn't return a sketch, such as `APPROX_COUNT_DISTINCT_DS_HLL`, `APPROX_COUNT_DISTINCT_DS_THETA`, `APPROX_QUANTILE_DS`, `DS_THETA_ESTIMATE`, or `DS_GET_QUANTILE`.
              """
          )
          .build();

  public static final QueryContextParameter<Boolean> SQL_USE_BOUND_AND_SELECTORS =
      booleanParameter("sqlUseBoundAndSelectors")
          .since("30.0.1").query(Query.SQL).engine(Engine.NATIVE, Engine.MSQ, Engine.DART)
          .defaultValue(false)
          .description(
              """
              If `false` (default behavior in Druid 27.0.0 and later), the SQL planner uses [equality](./filters.md#equality-filter),
              [null](./filters.md#null-filter), and [range](./filters.md#range-filter) filters instead of [selector](./filters.md#selector-filter)
              and [bounds](./filters.md#bound-filter).
              For filtering `ARRAY` typed values, `sqlUseBoundAndSelectors` must be `false`.
              """
          )
          .build();

  public static final QueryContextParameter<Boolean> SQL_USE_EXTRACTION_FNS =
      booleanParameter("sqlUseExtractionFns")
          .since("35.0.1").query(Query.SQL).engine(Engine.NATIVE, Engine.MSQ, Engine.DART)
          .defaultValue(false)
          .description(
              """
              If false, the SQL planner avoids using [`extractionFn`](dimensionspecs.md#extraction-functions) in favor of using other constructs such as [virtual columns](virtual-columns.md).
              This parameter is provided for compatibility with prior behavior, and may be removed in a future release.
              """
          )
          .build();

  public static final QueryContextParameter<Boolean> SQL_REVERSE_LOOKUP =
      booleanParameter("sqlReverseLookup")
          .since("30.0.1").query(Query.SQL).engine(Engine.NATIVE, Engine.MSQ, Engine.DART)
          .defaultValue(true)
          .description(
              """
              Whether to consider the [reverse-lookup rewrite](lookups.md#reverse-lookup) of the `LOOKUP` function during SQL planning.\
              <br /><br />Druid reverses calls to `LOOKUP` only when the number of matching keys is lower than both `inSubQueryThreshold` and `sqlReverseLookupThreshold`.
              """
          )
          .build();

  public static final QueryContextParameter<Integer> SQL_REVERSE_LOOKUP_THRESHOLD =
      integerParameter("sqlReverseLookupThreshold")
          .since("30.0.1").query(Query.SQL).engine(Engine.NATIVE, Engine.MSQ, Engine.DART)
          .defaultValue(10000)
          .description(
              """
              Maximum size of `IN` filter to create when applying a [reverse-lookup rewrite](lookups.md#reverse-lookup).
              If a `LOOKUP` call matches more keys than the specified threshold, it remains unchanged.\
              <br /><br />If `inSubQueryThreshold` is lower than `sqlReverseLookupThreshold`, Druid uses `inSubQueryThreshold` threshold instead.
              """
          )
          .build();

  public static final QueryContextParameter<Boolean> SQL_PULL_UP_LOOKUP =
      booleanParameter("sqlPullUpLookup")
          .since("30.0.1").query(Query.SQL).engine(Engine.NATIVE, Engine.MSQ, Engine.DART)
          .defaultValue(true)
          .description("Whether to consider the [pull-up rewrite](lookups.md#pull-up) of the `LOOKUP` function during SQL planning.")
          .build();

  public static final QueryContextParameter<JoinAlgorithm> SQL_JOIN_ALGORITHM =
      enumParameter("sqlJoinAlgorithm", JoinAlgorithm.class, JoinAlgorithm::fromString)
          .since("26.0.0").query(Query.SQL).engine(Engine.MSQ, Engine.DART)
          .defaultValue(JoinAlgorithm.BROADCAST)
          .description(
              """
              Algorithm to use for `JOIN`: `broadcast` for broadcast hash join or `sortMerge` for sort-merge join.
              Affects all joins in the query.
              This is a hint to the MSQ engine, and the actual joins may proceed differently.
              The native engine only supports `broadcast`.
              See [Joins](../multi-stage-query/reference.md#joins) for more details.
              """
          )
          .statement(StatementType.SELECT, StatementType.INSERT, StatementType.REPLACE)
          .build();

  public static final QueryContextParameter<String> SQL_CURRENT_TIMESTAMP =
      QueryContextParameter
          .builder("sqlCurrentTimestamp", String.class, QueryContextParameters::parseSqlCurrentTimestamp)
          .constraint(QueryContextParameters::validateSqlCurrentTimestamp)
          .since("0.10.0").query(Query.SQL).engine(Engine.NATIVE, Engine.MSQ, Engine.DART)
          .description(
              """
              Overrides the current time that SQL functions such as `CURRENT_TIMESTAMP` use, as an ISO 8601 string or
              milliseconds since the epoch. Intended for testing.
              """
          )
          .visibility(Visibility.HIDDEN)
          .build();

  public static final QueryContextParameter<Long> SQL_OUTER_LIMIT =
      longParameter("sqlOuterLimit")
          .since("0.17.0").query(Query.SQL).engine(Engine.NATIVE, Engine.MSQ, Engine.DART)
          .description(
              """
              Maximum number of rows to return, applied as an outer `LIMIT` around the SQL query so that clients such as
              the web console can limit results without rewriting the query. Not allowed for `INSERT` and `REPLACE`.
              """
          )
          .visibility(Visibility.HIDDEN)
          .build();

  public static final QueryContextParameter<Boolean> ENABLE_RAC_OVER_WIRE =
      booleanParameter("enableRACOverWire")
          .since("32.0.0").query(Query.SQL).engine(Engine.NATIVE)
          .defaultValue(false)
          .description(
              """
              If true, and the engine doesn't support window leaf operators natively, the SQL planner pushes the leaf window
              operator to data servers, which then transfer rows and columns (RAC) results over the wire.
              """
          )
          .visibility(Visibility.HIDDEN)
          .build();

  public static final QueryContextParameter<Boolean> SQL_USE_GRANULARITY =
      booleanParameter("sqlUseGranularity")
          .since("31.0.0").query(Query.SQL).engine(Engine.NATIVE, Engine.MSQ, Engine.DART)
          .defaultValue(true)
          .description(
              """
              Whether the SQL planner may use query granularities other than `all`.
              Set to false to query tables whose segments aren't sorted by time first, which only support `all`.
              """
          )
          .visibility(Visibility.HIDDEN)
          .build();

  public static final QueryContextParameter<Integer> MAX_NUMERIC_IN_FILTERS =
      integerParameter("maxNumericInFilters")
          .since("0.23.0").query(Query.SQL).engine(Engine.NATIVE, Engine.MSQ, Engine.DART)
          .description(
              """
              Max limit for the amount of numeric values that Druid can compare for a string type dimension when the entire SQL WHERE clause of a query translates only to an [OR](../querying/filters.md#or) of [bound filter](../querying/filters.md#bound-filter).
              By default, Druid doesn't restrict the amount of numeric bound filters on string columns, although this situation may block other queries from running.
              Set this parameter to a smaller value to prevent Druid from running queries that have prohibitively long segment processing times.
              The optimal limit requires some trial and error.
              We recommend starting with 100.
              Users who submit a query that exceeds the limit of `maxNumericInFilters` should rewrite their queries to use strings in the `WHERE` clause instead of numbers.
              For example, `WHERE someString IN (‘123’, ‘456’)`.
              This value can't exceed the set system configuration `druid.sql.planner.maxNumericInFilters`.
              If `druid.sql.planner.maxNumericInFilters` isn't set explicitly, Druid ignores this value.
              """
          )
          .defaultDescription("`-1`")
          .build();

  public static final QueryContextParameter<Boolean> ENABLE_JOIN_LEFT_TABLE_SCAN_DIRECT =
      booleanParameter("enableJoinLeftTableScanDirect")
          .since("0.22.0").query(Query.SQL).engine(Engine.NATIVE, Engine.MSQ, Engine.DART)
          .defaultValue(QueryContexts.DEFAULT_ENABLE_SQL_JOIN_LEFT_SCAN_DIRECT)
          .description(
              """
              This parameter applies to queries with joins.
              By default, when the left child is a simple scan with a filter, Druid runs the scan as a query, then joins it with the right child on the Broker.
              Setting this parameter to `true` overrides that behavior and pushes the join to the data servers instead.
              Even if a query doesn't explicitly include a join, this parameter may still apply since the SQL planner can translate the query into a join internally.
              """
          )
          .build();

  public static final QueryContextParameter<Integer> IN_SUB_QUERY_THRESHOLD =
      integerParameter("inSubQueryThreshold")
          .since("0.23.0").query(Query.SQL).engine(Engine.NATIVE, Engine.MSQ, Engine.DART)
          .defaultValue(QueryContexts.DEFAULT_IN_SUB_QUERY_THRESHOLD)
          .description(
              """
              At or beyond this threshold number of values, Druid converts SQL `IN` to `JOIN` on an inline table.
              `inFunctionThreshold` takes priority over this setting.
              A threshold of 0 forces usage of an inline table in all cases where the size of a SQL `IN` is larger than `inFunctionThreshold`.
              A threshold of `2147483647` disables the rewrite of SQL `IN` to `JOIN`.
              """
          )
          .build();

  public static final QueryContextParameter<Long> MAX_PLANNING_TIME_MS =
      longParameter("maxPlanningTimeMs")
          .since("39.0.0").query(Query.SQL).engine(Engine.NATIVE, Engine.MSQ, Engine.DART)
          .defaultValue(0L)
          .description(
              """
              Maximum wall-clock time, in milliseconds, allowed for planning this query on the Broker.
              When planning exceeds this budget, planning is aborted and the query fails with an HTTP 504 `Query timeout` error.
              Use this to guard against pathological queries, such as one with a very large `IN` filter, whose planning time can spike.
              A non-positive value disables the timeout.
              To apply a cluster-wide default, set the `druid.query.default.context.maxPlanningTimeMs` runtime property (or the Broker's dynamic `queryContext` config); see [Overriding default query context values](../configuration/index.md#overriding-default-query-context-values).
              """
          )
          .defaultDescription("`0` (disabled)")
          .build();

  public static final QueryContextParameter<Integer> IN_FUNCTION_THRESHOLD =
      integerParameter("inFunctionThreshold")
          .since("31.0.0").query(Query.SQL).engine(Engine.NATIVE, Engine.MSQ, Engine.DART)
          .defaultValue(QueryContexts.DEFAULT_IN_FUNCTION_THRESHOLD)
          .description(
              """
              At or beyond this threshold number of values, Druid converts SQL `IN` to [`SCALAR_IN_ARRAY`](sql-functions.md#scalar_in_array).
              A threshold of 0 forces this conversion in all cases.
              A threshold of `Integer.MAX_VALUE` disables this conversion.
              The converted function is eligible for fewer planning-time optimizations, which speeds up planning, but may prevent certain planning-time optimizations.
              """
          )
          .build();

  public static final QueryContextParameter<Integer> IN_FUNCTION_EXPR_THRESHOLD =
      integerParameter("inFunctionExprThreshold")
          .since("31.0.0").query(Query.SQL).engine(Engine.NATIVE, Engine.MSQ, Engine.DART)
          .defaultValue(QueryContexts.DEFAULT_IN_FUNCTION_EXPR_THRESHOLD)
          .description(
              """
              At or beyond this threshold number of values, SQL `IN` is eligible for execution using the native function `scalar_in_array` rather than an <code>&#124;&#124;</code> of `==`, even if the number of values is below `inFunctionThreshold`.
              This property only affects translation of SQL `IN` to a [native expression](math-expr.md).
              It doesn't affect translation of SQL `IN` to a [native filter](filters.md).
              This property is provided for backwards compatibility purposes, and may be removed in a future release.
              """
          )
          .build();

  public static final QueryContextParameter<Boolean> ENABLE_TIME_BOUNDARY_PLANNING =
      booleanParameter("enableTimeBoundaryPlanning")
          .since("0.24.0").query(Query.SQL).engine(Engine.NATIVE)
          .defaultValue(QueryContexts.DEFAULT_ENABLE_TIME_BOUNDARY_PLANNING)
          .description(
              """
              If `true`, Druid converts SQL queries to [time boundary queries](timeboundaryquery.md) wherever possible.
              Time boundary queries are very efficient for min-max calculation on the `__time` column in a datasource.
              """
          )
          .build();

  public static final QueryContextParameter<Boolean> CATALOG_VALIDATION_ENABLED =
      booleanParameter("catalogValidationEnabled")
          .since("31.0.0").query(Query.SQL).engine(Engine.MSQ)
          .defaultValue(QueryContexts.DEFAULT_CATALOG_VALIDATION_ENABLED)
          .description(
              """
              If false, Druid skips the catalog-specific validation of `INSERT` and `REPLACE` statements that write to
              tables defined in the Druid catalog.
              """
          )
          .build();

  public static final QueryContextParameter<Boolean> EXTENDED_FILTERED_SUM_REWRITE =
      booleanParameter("extendedFilteredSumRewrite")
          .since("32.0.0").query(Query.SQL).engine(Engine.NATIVE, Engine.MSQ, Engine.DART)
          .defaultValue(QueryContexts.DEFAULT_EXTENDED_FILTERED_SUM_REWRITE_ENABLED)
          .description(
              """
              Whether the SQL planner rewrites `SUM(CASE WHEN cond THEN col ELSE 0 END)` to `SUM(col) FILTER (WHERE cond)`.
              The rewrite is faster but may return incorrect results when the condition never matches.
              Intended for testing until a correct and fast rewrite is available.
              """
          )
          .visibility(Visibility.HIDDEN)
          .build();

  public static final QueryContextParameter<QueryContexts.PlannerStrategy> PLANNER_STRATEGY =
      enumParameter("plannerStrategy", QueryContexts.PlannerStrategy.class)
          .since("27.0.0").query(Query.SQL).engine(Engine.NATIVE, Engine.MSQ, Engine.DART)
          .defaultValue(QueryContexts.PlannerStrategy.COUPLED)
          .description(
              """
              SQL planning strategy: `COUPLED` plans the logical query and generates the native query together,
              while `DECOUPLED` separates logical planning from native query generation.
              """
          )
          .visibility(Visibility.HIDDEN)
          .build();

  //
  // GroupBy query parameters.
  //
  // These descriptors are read by the groupBy engine. Public ones are documented in groupbyquery.md.

  //
  // Scan, search and timeseries query parameters.
  //
  // Public ones are documented in scan-query.md and searchquery.md.

  //
  // MSQ and Dart parameters.
  //
  // These descriptors are read by the MSQ task engine and Dart. Public ones are documented in
  // multi-stage-query/reference.md and dart.md. Values whose types live in the multi-stage-query module are declared as
  // strings and converted to those types where they are read.

  //
  // Other query context parameters.
  //
  // Parameters specific to an engine, an API or a query type, such as Dart, the SQL statements API, realtime segment
  // selection and scan ordering, along with parameters Druid uses for internal coordination. Each descriptor's
  // visibility says whether it is documented or can be set by users.
  public static final QueryContextParameter<String> DART_QUERY_ID =
      stringParameter("dartQueryId")
          .since("32.0.0").query(Query.SQL).engine(Engine.DART)
          .description(
              """
              Globally unique execution ID that Dart generates for each query and uses as the controller query ID.
              Unlike `sqlQueryId`, users can't choose it.
              """
          )
          .visibility(Visibility.SYSTEM)
          .build();

  public static final QueryContextParameter<String> SUB_QUERY_ID =
      stringParameter("subQueryId")
          .since("0.18.0").query(Query.JSON, Query.SQL).engine(Engine.NATIVE)
          .description("ID that the Broker assigns to each subquery so that it can be correlated with its parent query.")
          .visibility(Visibility.SYSTEM)
          .build();

  public static final QueryContextParameter<Boolean> FULL_REPORT = booleanParameter("fullReport")
      .since("32.0.0").query(Query.SQL).engine(Engine.DART)
      .defaultValue(QueryContexts.DEFAULT_CTX_FULL_REPORT)
      .description(
          """
          If true, a [Dart](dart.md) query returns a single `fullReport` column that contains the full query report,
          including the results, instead of the query results.
          The report size is limited by `druid.msq.dart.controller.maxQueryReportSize`.
          """
      )
      .build();

  public static final QueryContextParameter<ExecutionMode> EXECUTION_MODE =
      enumParameter("executionMode", ExecutionMode.class)
          .since("27.0.0").query(Query.SQL).engine(Engine.MSQ)
          .description(
              """
              Determines how the [SQL statements API](../api-reference/sql-api.md#query-from-deep-storage) fetches query results.
              Druid currently only supports `ASYNC`, which requires you to retrieve the results after the query completes.
              """
          )
          .build();

  public static final QueryContextParameter<Boolean> REALTIME_SEGMENTS_ONLY =
      booleanParameter("realtimeSegmentsOnly")
          .since("35.0.0").query(Query.JSON, Query.SQL).engine(Engine.NATIVE)
          .defaultValue(false)
          .deprecated("Use realtimeSegmentsMode instead.")
          .description(
              """
              **Deprecated.** Use `realtimeSegmentsMode=exclusive` instead.
              When set to `true`, this is equivalent to `realtimeSegmentsMode=exclusive`.
              When set to `false`, this is equivalent to `realtimeSegmentsMode=include`.
              """
          )
          .build();

  public static final QueryContextParameter<QueryContexts.RealtimeSegmentsMode> REALTIME_SEGMENTS_MODE =
      enumParameter("realtimeSegmentsMode", QueryContexts.RealtimeSegmentsMode.class)
          .since("38.0.0").query(Query.JSON, Query.SQL).engine(Engine.NATIVE)
          .defaultValue(QueryContexts.DEFAULT_REALTIME_SEGMENTS_MODE)
          .description(
              """
              Controls whether realtime segments are queried.
              `include` queries all segments, including realtime.
              `exclude` skips realtime segments.
              `exclusive` queries only realtime segments.
              """
          )
          .build();

  public static final QueryContextParameter<Boolean> PREPLANNED = booleanParameter("prePlanned")
      .since("34.0.0").query(Query.SQL).engine(Engine.DART)
      .defaultValue(QueryContexts.DEFAULT_PREPLANNED)
      .description(
          """
          Whether Dart plans the query definition directly from the logical plan, instead of first translating the query
          to a native query.
          """
      )
      .visibility(Visibility.HIDDEN)
      .build();

  public static final QueryContextParameter<Integer> MAX_ROWS_QUEUED_FOR_ORDERING =
      integerParameter("maxRowsQueuedForOrdering")
          .since("0.15.0-incubating").query(Query.JSON).engine(Engine.NATIVE)
          .constraint(Range.closedRange(1, Integer.MAX_VALUE))
          .description(
              """
              The maximum number of rows returned when time ordering is used.
              Overrides the identically named config.
              """
          )
          .defaultDescription("`druid.query.scan.maxRowsQueuedForOrdering`")
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

  /**
   * Creates an enum parameter whose string values are converted with the enum's own lookup, for enums whose accepted
   * values differ from their constant names. The lookup must throw {@link IllegalArgumentException} for unknown values.
   */
  static <T extends Enum<T>> QueryContextParameter.Builder<T> enumParameter(
      final String name,
      final Class<T> valueType,
      final Function<String, T> fromString
  )
  {
    return QueryContextParameter.builder(
        name,
        valueType,
        value -> {
          if (value instanceof String) {
            try {
              return fromString.apply((String) value);
            }
            catch (IllegalArgumentException _) {
              // Fall through to the invalid-value error below.
            }
          }
          throw invalidValueException(name, "one of " + Arrays.toString(valueType.getEnumConstants()), value);
        }
    );
  }

  private static final String MAX_SUBQUERY_BYTES_FORMAT = "'auto', 'disabled' or a non-negative number of bytes";

  private static String parseMaxSubqueryBytes(final Object value)
  {
    try {
      return String.valueOf(
          QueryContextParameter.exactNumber(
              "maxSubqueryBytes",
              value,
              "long",
              0,
              Long.MAX_VALUE,
              BigDecimal::longValueExact
          )
      );
    }
    catch (BadQueryContextException e) {
      throw invalidValueException("maxSubqueryBytes", MAX_SUBQUERY_BYTES_FORMAT, value);
    }
  }

  /**
   * Accepts the same strings as the Broker's subquery guardrail.
   */
  private static final String SQL_CURRENT_TIMESTAMP_FORMAT = "an ISO 8601 timestamp or milliseconds since the epoch";

  /**
   * Accepts milliseconds since the epoch, for compatibility, as the equivalent ISO 8601 timestamp
   */
  private static String parseSqlCurrentTimestamp(final Object value)
  {
    try {
      final long millis = QueryContextParameter.exactNumber(
          "sqlCurrentTimestamp",
          value,
          "long",
          Long.MIN_VALUE,
          Long.MAX_VALUE,
          BigDecimal::longValueExact
      );
      return DateTimes.utc(millis).toString();
    }
    catch (BadQueryContextException e) {
      throw invalidValueException("sqlCurrentTimestamp", SQL_CURRENT_TIMESTAMP_FORMAT, value);
    }
  }

  private static void validateSqlCurrentTimestamp(final String name, final String value)
  {
    try {
      DateTimes.of(value);
    }
    catch (IllegalArgumentException e) {
      throw invalidValueException(name, SQL_CURRENT_TIMESTAMP_FORMAT, value);
    }
  }

  private static void validateMaxSubqueryBytes(final String name, final String value)
  {
    if ("auto".equalsIgnoreCase(value) || "disabled".equalsIgnoreCase(value)) {
      return;
    }
    try {
      if (Long.parseLong(value) >= 0) {
        return;
      }
    }
    catch (NumberFormatException e) {
      // Reported below.
    }
    throw invalidValueException(name, MAX_SUBQUERY_BYTES_FORMAT, value);
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
