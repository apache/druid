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
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.frame.FrameType;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.HumanReadableBytes;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.BadQueryContextException;
import org.apache.druid.query.CloneQueryMode;
import org.apache.druid.query.ExecutionMode;
import org.apache.druid.query.JoinAlgorithm;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.ResultSerializationMode;
import org.apache.druid.query.context.QueryContextParameter.Visibility;
import org.apache.druid.query.context.constraint.Range;
import org.apache.druid.query.context.docs.ParameterDocumentation.Engine;
import org.apache.druid.query.context.docs.ParameterDocumentation.Query;
import org.apache.druid.query.context.docs.ParameterDocumentation.QueryType;
import org.apache.druid.query.context.docs.ParameterDocumentation.StatementType;
import org.apache.druid.query.groupby.DeferExpressionDimensions;
import org.apache.druid.query.topn.TopNQueryConfig;
import org.apache.druid.segment.loading.AcquireMode;

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
/// Descriptors are grouped into the sections below, each wrapped in `// region` and `// endregion` comments.
/// IDEs can fold the sections and list them for navigation, such as IntelliJ's Navigate > Custom Folding.
///
/// - General query parameters
/// - SQL planner parameters
/// - GroupBy query parameters
/// - Scan, search and timeseries query parameters
/// - MSQ and Dart parameters
/// - Other query context parameters
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
  // region General query parameters
  //
  // Parameters that apply across query types. Public ones are documented in query-context-reference.md.
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
      .defaultValue(0)
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
          .defaultValue(true)
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
          .defaultValue(QueryContexts.Vectorize.TRUE)
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
          .defaultValue(QueryContexts.Vectorize.TRUE)
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
      .defaultValue(512)
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
          .defaultValue(false)
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
          .defaultValue(true)
          .description(
              """
              Controls whether a join query will attempt filter push down, which reduces the number of rows that have to be compared in a join operation.
              """
          )
          .build();

  public static final QueryContextParameter<Boolean> ENABLE_JOIN_FILTER_REWRITE =
      booleanParameter("enableJoinFilterRewrite")
          .since("0.18.0").query(Query.JSON, Query.SQL).engine(Engine.NATIVE)
          .defaultValue(true)
          .description(
              """
              Controls whether filter clauses that reference non-base table columns will be rewritten into filters on base table columns.
              """
          )
          .build();

  public static final QueryContextParameter<Boolean> ENABLE_JOIN_FILTER_REWRITE_VALUE_COLUMN_FILTERS =
      booleanParameter("enableJoinFilterRewriteValueColumnFilters")
          .since("0.18.0").query(Query.JSON, Query.SQL).engine(Engine.NATIVE)
          .defaultValue(false)
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
          .defaultValue(true)
          .description(
              """
              Controls whether a join can be pushed partial or fully to the base table as a filter at runtime.
              """
          )
          .build();

  public static final QueryContextParameter<Long> JOIN_FILTER_REWRITE_MAX_SIZE =
      longParameter("joinFilterRewriteMaxSize")
          .since("0.18.0").query(Query.JSON, Query.SQL).engine(Engine.NATIVE)
          .defaultValue(10000L)
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
          .defaultValue(CloneQueryMode.EXCLUDECLONES)
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
          .defaultValue(true)
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
      .defaultValue(false)
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
      .defaultValue(true)
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
          .defaultValue(true)
          .description(
              """
              Enable secondary partition pruning on the Broker.
              The Broker will always prune unnecessary segments from the input scan based on a filter on time intervals, but if the data is further partitioned with hash or range partitioning, this option will enable additional pruning based on a filter on secondary partition dimensions.
              """
          )
          .build();

  public static final QueryContextParameter<Boolean> DEBUG = booleanParameter("debug")
      .since("0.22.0").query(Query.JSON, Query.SQL).engine(Engine.NATIVE)
      .defaultValue(false)
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
      .defaultValue(false)
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
          .defaultValue(true)
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
          .defaultValue(true)
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
          .defaultValue(0)
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
      .defaultValue("native")
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

  // endregion

  // region SQL planner parameters
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
          .defaultValue(false)
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
          .defaultValue(Integer.MAX_VALUE)
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
          .defaultValue(100)
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
          .defaultValue(2)
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
          .defaultValue(false)
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
          .defaultValue(true)
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
          .defaultValue(true)
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

  public static final QueryContextParameter<Boolean> COMPUTE_INNER_JOIN_COST_AS_FILTER =
      booleanParameter("computeInnerJoinCostAsFilter")
          .since("0.22.0").query(Query.SQL).engine(Engine.NATIVE, Engine.MSQ, Engine.DART)
          .description(
              """
              Whether the SQL planner computes the cost of an inner join as if it were a filter on the left input.
              Overrides `druid.sql.planner.computeInnerJoinCostAsFilter`.
              """
          )
          .defaultDescription("`true`")
          .visibility(Visibility.HIDDEN)
          .build();

  public static final QueryContextParameter<Boolean> FORCE_EXPRESSION_VIRTUAL_COLUMNS =
      booleanParameter("forceExpressionVirtualColumns")
          .since("24.0.0").query(Query.SQL).engine(Engine.NATIVE, Engine.MSQ, Engine.DART)
          .description(
              """
              If true, the SQL planner always generates virtual columns of type `expression` instead of optimized
              special-purpose virtual columns.
              Overrides `druid.sql.planner.forceExpressionVirtualColumns`.
              """
          )
          .defaultDescription("`false`")
          .visibility(Visibility.HIDDEN)
          .build();

  public static final QueryContextParameter<Boolean> REQUIRE_TIME_CONDITION =
      booleanParameter("requireTimeCondition")
          .since("34.0.0").query(Query.SQL).engine(Engine.NATIVE, Engine.MSQ, Engine.DART)
          .description(
              """
              If true, SQL queries without a filter condition on the `__time` column fail, so that every generated
              native query has user-specified intervals.
              Overrides `druid.sql.planner.requireTimeCondition`.
              """
          )
          .defaultDescription("`false`")
          .visibility(Visibility.HIDDEN)
          .build();

  public static final QueryContextParameter<Integer> MAX_OPTIMIZE_COUNT_FOR_DRUID_REVERSE_LOOKUP_RULE =
      integerParameter("maxOptimizeCountForDruidReverseLookupRule")
          .since("29.0.0").query(Query.SQL).engine(Engine.NATIVE, Engine.MSQ, Engine.DART)
          .defaultValue(Integer.MAX_VALUE)
          .description(
              """
              Maximum number of times the reverse-lookup rule optimizes `IN` filters while planning a query.
              Intended for testing that the rule doesn't iterate lookups more often than necessary.
              """
          )
          .visibility(Visibility.HIDDEN)
          .build();

  public static final QueryContextParameter<String> SQL_INSERT_SEGMENT_GRANULARITY =
      stringParameter("sqlInsertSegmentGranularity")
          .since("0.23.0").query(Query.SQL).engine(Engine.MSQ)
          .description(
              """
              JSON-serialized segment granularity of an `INSERT` or `REPLACE` statement.
              The SQL planner sets it from the `PARTITIONED BY` clause.
              """
          )
          .visibility(Visibility.SYSTEM)
          .build();

  public static final QueryContextParameter<String> SQL_INSERT_QUERY_GRANULARITY =
      stringParameter("sqlInsertQueryGranularity")
          .since("31.0.0").query(Query.SQL).engine(Engine.MSQ)
          .description(
              """
              JSON-serialized query granularity that the MSQ engine applies to the segments it generates.
              Compaction with the MSQ engine sets it from the granularity spec.
              """
          )
          .visibility(Visibility.HIDDEN)
          .build();

  public static final QueryContextParameter<String> SQL_REPLACE_TIME_CHUNKS =
      stringParameter("sqlReplaceTimeChunks")
          .since("24.0.0").query(Query.SQL).engine(Engine.MSQ)
          .description(
              """
              Comma-separated list of the intervals, or `all`, that a `REPLACE` statement overwrites.
              The SQL planner sets it from the `OVERWRITE` clause.
              """
          )
          .visibility(Visibility.SYSTEM)
          .build();

  public static final QueryContextParameter<Long> APPROX_QUANTILE_DS_MAX_STREAM_LENGTH =
      longParameter("approxQuantileDsMaxStreamLength")
          .since("0.22.0").query(Query.SQL).engine(Engine.NATIVE, Engine.MSQ, Engine.DART)
          .description(
              """
              Maximum stream length of the sketches that the `APPROX_QUANTILE_DS` and `DS_QUANTILES_SKETCH` SQL functions build.
              Increase it if a query fails because a sketch reaches its maximum stream length.
              """
          )
          .defaultDescription("`1000000000`")
          .visibility(Visibility.HIDDEN)
          .build();

  // endregion

  // region GroupBy query parameters
  //
  // These descriptors are read by the groupBy engine. Public ones are documented in groupbyquery.md.
  public static final QueryContextParameter<HumanReadableBytes> MAX_ON_DISK_STORAGE =
      humanReadableBytesParameter("maxOnDiskStorage")
          .since("0.9.2").query(Query.JSON, Query.SQL).engine(Engine.NATIVE)
          .description("Can be used to lower the value of `druid.query.groupBy.maxOnDiskStorage` for this query.")
          .defaultDescription("`druid.query.groupBy.defaultOnDiskStorage`")
          .queryType(QueryType.GROUP_BY)
          .build();

  public static final QueryContextParameter<Integer> MAX_SPILL_FILE_COUNT =
      integerParameter("maxSpillFileCount")
          .since("37.0.0").query(Query.JSON, Query.SQL).engine(Engine.NATIVE)
          .description("Can be used to override the value of `druid.query.groupBy.maxSpillFileCount` for this query.")
          .defaultDescription("`druid.query.groupBy.maxSpillFileCount`")
          .queryType(QueryType.GROUP_BY)
          .build();

  public static final QueryContextParameter<HumanReadableBytes> MIN_SPILL_FILE_SIZE =
      humanReadableBytesParameter("minSpillFileSize")
          .since("38.0.0").query(Query.JSON, Query.SQL).engine(Engine.NATIVE)
          .description("Can be used to override the value of `druid.query.groupBy.minSpillFileSize` for this query.")
          .defaultDescription("`druid.query.groupBy.minSpillFileSize`")
          .queryType(QueryType.GROUP_BY)
          .build();

  public static final QueryContextParameter<Boolean> GROUP_BY_IS_SINGLE_THREADED =
      booleanParameter("groupByIsSingleThreaded")
          .since("0.9.1").query(Query.JSON, Query.SQL).engine(Engine.NATIVE)
          .description("Overrides the value of `druid.query.groupBy.singleThreaded` for this query.")
          .defaultDescription("`druid.query.groupBy.singleThreaded`")
          .queryType(QueryType.GROUP_BY)
          .build();

  public static final QueryContextParameter<Integer> BUFFER_GROUPER_INITIAL_BUCKETS =
      integerParameter("bufferGrouperInitialBuckets")
          .since("0.9.2").query(Query.JSON, Query.SQL).engine(Engine.NATIVE, Engine.MSQ, Engine.DART)
          .description("Overrides the value of `druid.query.groupBy.bufferGrouperInitialBuckets` for this query.")
          .defaultDescription("`druid.query.groupBy.bufferGrouperInitialBuckets`")
          .queryType(QueryType.GROUP_BY)
          .build();

  public static final QueryContextParameter<Float> BUFFER_GROUPER_MAX_LOAD_FACTOR =
      floatParameter("bufferGrouperMaxLoadFactor")
          .since("0.9.2").query(Query.JSON, Query.SQL).engine(Engine.NATIVE, Engine.MSQ, Engine.DART)
          .description("Overrides the value of `druid.query.groupBy.bufferGrouperMaxLoadFactor` for this query.")
          .defaultDescription("`druid.query.groupBy.bufferGrouperMaxLoadFactor`")
          .queryType(QueryType.GROUP_BY)
          .build();

  public static final QueryContextParameter<Boolean> FORCE_HASH_AGGREGATION =
      booleanParameter("forceHashAggregation")
          .since("0.11.0").query(Query.JSON, Query.SQL).engine(Engine.NATIVE, Engine.MSQ, Engine.DART)
          .description("Overrides the value of `druid.query.groupBy.forceHashAggregation`.")
          .defaultDescription("`druid.query.groupBy.forceHashAggregation`")
          .queryType(QueryType.GROUP_BY)
          .build();

  public static final QueryContextParameter<Integer> INTERMEDIATE_COMBINE_DEGREE =
      integerParameter("intermediateCombineDegree")
          .since("0.12.0").query(Query.JSON, Query.SQL).engine(Engine.NATIVE)
          .description("Overrides the value of `druid.query.groupBy.intermediateCombineDegree`.")
          .defaultDescription("`druid.query.groupBy.intermediateCombineDegree`")
          .queryType(QueryType.GROUP_BY)
          .build();

  public static final QueryContextParameter<Integer> NUM_PARALLEL_COMBINE_THREADS =
      integerParameter("numParallelCombineThreads")
          .since("0.12.0").query(Query.JSON, Query.SQL).engine(Engine.NATIVE)
          .description("Overrides the value of `druid.query.groupBy.numParallelCombineThreads`.")
          .defaultDescription("`druid.query.groupBy.numParallelCombineThreads`")
          .queryType(QueryType.GROUP_BY)
          .build();

  public static final QueryContextParameter<HumanReadableBytes> MAX_SELECTOR_DICTIONARY_SIZE =
      humanReadableBytesParameter("maxSelectorDictionarySize")
          .since("0.23.0").query(Query.JSON, Query.SQL).engine(Engine.NATIVE, Engine.MSQ, Engine.DART)
          .description("Overrides the value of `druid.query.groupBy.maxSelectorDictionarySize`.")
          .defaultDescription("`druid.query.groupBy.maxSelectorDictionarySize`")
          .queryType(QueryType.GROUP_BY)
          .build();

  public static final QueryContextParameter<HumanReadableBytes> MAX_MERGING_DICTIONARY_SIZE =
      humanReadableBytesParameter("maxMergingDictionarySize")
          .since("0.9.2").query(Query.JSON, Query.SQL).engine(Engine.NATIVE)
          .description("Overrides the value of `druid.query.groupBy.maxMergingDictionarySize`.")
          .defaultDescription("`druid.query.groupBy.maxMergingDictionarySize`")
          .queryType(QueryType.GROUP_BY)
          .build();

  public static final QueryContextParameter<Boolean> MERGE_THREAD_LOCAL =
      booleanParameter("mergeThreadLocal")
          .since("24.0.0").query(Query.JSON, Query.SQL).engine(Engine.NATIVE)
          .description(
              """
              Whether merge buffers should always be split into thread-local buffers.
              Setting this to `true` reduces thread contention, but uses memory less efficiently.
              This tradeoff is beneficial when memory is plentiful.
              """
          )
          .defaultDescription("`druid.query.groupBy.mergeThreadLocal`")
          .queryType(QueryType.GROUP_BY)
          .build();

  public static final QueryContextParameter<Boolean> SORT_BY_DIMS_FIRST =
      booleanParameter("sortByDimsFirst")
          .since("0.10.0").query(Query.JSON, Query.SQL).engine(Engine.NATIVE)
          .defaultValue(false)
          .description("Sort the results first by dimension values and then by timestamp.")
          .queryType(QueryType.GROUP_BY)
          .build();

  public static final QueryContextParameter<Boolean> FORCE_LIMIT_PUSH_DOWN =
      booleanParameter("forceLimitPushDown")
          .since("0.10.1").query(Query.JSON, Query.SQL).engine(Engine.NATIVE)
          .description(
              """
              When all fields in the orderby are part of the grouping key, the Broker will push limit application down to the Historical processes.
              When the sorting order uses fields that are not in the grouping key, applying this optimization can result in approximate results with unknown accuracy, so this optimization is disabled by default in that case.
              Enabling this context flag turns on limit push down for limit/orderbys that contain non-grouping key columns.
              """
          )
          .defaultDescription("`druid.query.groupBy.forcePushDownLimit`")
          .queryType(QueryType.GROUP_BY)
          .build();

  public static final QueryContextParameter<Boolean> APPLY_LIMIT_PUSH_DOWN_TO_SEGMENT =
      booleanParameter("applyLimitPushDownToSegment")
          .since("0.17.0").query(Query.JSON, Query.SQL).engine(Engine.NATIVE)
          .description(
              """
              If Broker pushes limit down to queryable nodes (historicals, peons) then limit results during segment scan.
              This context value can be used to override `druid.query.groupBy.applyLimitPushDownToSegment`.
              """
          )
          .defaultDescription("`druid.query.groupBy.applyLimitPushDownToSegment`")
          .queryType(QueryType.GROUP_BY)
          .build();

  public static final QueryContextParameter<Boolean> GROUP_BY_ENABLE_MULTI_VALUE_UNNESTING =
      booleanParameter("groupByEnableMultiValueUnnesting")
          .since("0.23.0").query(Query.JSON, Query.SQL).engine(Engine.NATIVE, Engine.MSQ, Engine.DART)
          .description(
              """
              Safety flag to enable/disable the implicit unnesting on multi value column's as part of the grouping key.
              'true' indicates multi-value grouping keys are unnested.
              'false' returns an error if a multi value column is found as part of the grouping key.
              """
          )
          .defaultDescription("`druid.query.groupBy.enableMultiValueUnnesting`")
          .queryType(QueryType.GROUP_BY)
          .build();

  public static final QueryContextParameter<DeferExpressionDimensions> DEFER_EXPRESSION_DIMENSIONS =
      enumParameter("deferExpressionDimensions", DeferExpressionDimensions.class, DeferExpressionDimensions::fromString)
          .since("31.0.0").query(Query.JSON, Query.SQL).engine(Engine.NATIVE, Engine.MSQ, Engine.DART)
          .description(
              """
              When an entry in `dimensions` references an `expression` virtual column, this property influences whether expression evaluation is deferred from cursor processing to the merge step.
              Options are:<ul><li>`fixedWidth`: Defer expressions with fixed-width inputs (numeric and dictionary-encoded string).</li><li>`fixedWidthNonNumeric`: Defer expressions with fixed-width inputs (numeric and dictionary-encoded string), unless the expression output and all inputs are numeric.</li><li>`singleString`: Defer string-typed expressions with a single dictionary-encoded string input.</li><li>`always`: Defer all expressions. May require building dictionaries for expression inputs.</li></ul><br />These properties only take effect when the `groupBy` query can be vectorized.
              Non-vectorized queries only defer string-typed expressions of single string inputs.
              """
          )
          .defaultDescription("`druid.query.groupBy.deferExpressionDimensions`")
          .queryType(QueryType.GROUP_BY)
          .build();

  public static final QueryContextParameter<Boolean> APPLY_LIMIT_PUSH_DOWN =
      booleanParameter("applyLimitPushDown")
          .since("0.11.0").query(Query.JSON, Query.SQL).engine(Engine.NATIVE)
          .defaultValue(true)
          .description(
              """
              Whether a groupBy query with a limit may push the limit down to the merge buffers of data servers.
              Set to `false` to switch off limit push down.
              The Broker sets this parameter on the queries it sends to data servers to tell them whether it applied limit push down.
              """
          )
          .queryType(QueryType.GROUP_BY)
          .visibility(Visibility.HIDDEN)
          .build();

  public static final QueryContextParameter<Boolean> RESULT_AS_ARRAY =
      booleanParameter("resultAsArray")
          .since("0.17.0").query(Query.JSON, Query.SQL).engine(Engine.NATIVE)
          .defaultValue(false)
          .description(
              """
              If true, groupBy results are returned as arrays instead of maps.
              """
          )
          .queryType(QueryType.GROUP_BY)
          .build();

  public static final QueryContextParameter<Integer> BUFFER_GROUPER_MAX_SIZE =
      integerParameter("bufferGrouperMaxSize")
          .since("0.9.2").query(Query.JSON, Query.SQL).engine(Engine.NATIVE, Engine.MSQ, Engine.DART)
          .description(
              """
              Maximum number of entries in the off-heap hash table used for grouping results.
              Can only lower `druid.query.groupBy.bufferGrouperMaxSize`.
              Mainly used by tests to force spilling.
              """
          )
          .defaultDescription("`druid.query.groupBy.bufferGrouperMaxSize`")
          .queryType(QueryType.GROUP_BY)
          .visibility(Visibility.HIDDEN)
          .build();

  public static final QueryContextParameter<Boolean> FORCE_PUSH_DOWN_NESTED_QUERY =
      booleanParameter("forcePushDownNestedQuery")
          .since("0.17.0").query(Query.JSON, Query.SQL).engine(Engine.NATIVE)
          .description(
              """
              If true, the Broker pushes the outer query of a nested groupBy query down to data servers together with the inner query, and merges the results.
              Overrides `druid.query.groupBy.forcePushDownNestedQuery`.
              """
          )
          .defaultDescription("`druid.query.groupBy.forcePushDownNestedQuery`")
          .queryType(QueryType.GROUP_BY)
          .visibility(Visibility.HIDDEN)
          .build();

  public static final QueryContextParameter<Boolean> EXECUTING_NESTED_QUERY =
      booleanParameter("executingNestedQuery")
          .since("0.17.0").query(Query.JSON).engine(Engine.NATIVE)
          .defaultValue(false)
          .description(
              """
              Set on a groupBy query whose outer query was pushed down to data servers, so that data servers don't apply post-aggregators to the partial results that they return to the Broker.
              """
          )
          .queryType(QueryType.GROUP_BY)
          .visibility(Visibility.SYSTEM)
          .build();

  public static final QueryContextParameter<Boolean> GROUP_BY_OUTERMOST =
      booleanParameter("groupByOutermost")
          .since("0.10.0").query(Query.JSON).engine(Engine.NATIVE)
          .defaultValue(true)
          .description(
              """
              Whether a groupBy query is the outermost one, which applies post-aggregators, having filters and limits.
              The Broker sets it to false on the queries it sends to data servers.
              """
          )
          .queryType(QueryType.GROUP_BY)
          .visibility(Visibility.SYSTEM)
          .build();

  public static final QueryContextParameter<String> FUDGE_TIMESTAMP =
      stringParameter("fudgeTimestamp")
          .since("0.9.2").query(Query.JSON).engine(Engine.NATIVE)
          .description(
              """
              Timestamp in milliseconds since the epoch that the Broker passes to data servers so that every result row of a groupBy query with a universal timestamp, such as one with `all` granularity, has the same timestamp.
              """
          )
          .queryType(QueryType.GROUP_BY)
          .visibility(Visibility.SYSTEM)
          .build();

  public static final QueryContextParameter<Boolean> MERGE_RUNNERS_USING_CHAINED_EXECUTION =
      booleanParameter("mergeRunnersUsingChainedExecution")
          .since("0.9.2").query(Query.JSON).engine(Engine.NATIVE)
          .defaultValue(false)
          .description(
              """
              Set by the groupBy merging runner on nested merge calls, such as those on realtime servers, so that only the outermost merge uses parallel execution.
              """
          )
          .queryType(QueryType.GROUP_BY)
          .visibility(Visibility.SYSTEM)
          .build();

  public static final QueryContextParameter<Boolean> HAS_DROPPED_DIMENSIONS =
      booleanParameter("hasDroppedDimensions")
          .since("33.0.0").query(Query.JSON, Query.SQL).engine(Engine.NATIVE, Engine.MSQ, Engine.DART)
          .defaultValue(false)
          .description(
              """
              Whether the SQL planner dropped grouping dimensions from a groupBy query, such as for `GROUP BY 'constant'`.
              Used to determine whether the query returns a summary row when there is no input.
              """
          )
          .queryType(QueryType.GROUP_BY)
          .visibility(Visibility.SYSTEM)
          .build();

  public static final QueryContextParameter<String> TIMESTAMP_RESULT_FIELD =
      stringParameter("timestampResultField")
          .since("0.19.0").query(Query.JSON, Query.SQL).engine(Engine.NATIVE, Engine.MSQ, Engine.DART)
          .description(
              """
              Name of the output column that the SQL planner maps to the query timestamp, such as one produced by `TIME_FLOOR(__time, ...)`, so that the query granularity can compute it instead of a dimension.
              """
          )
          .queryType(QueryType.GROUP_BY, QueryType.TIMESERIES)
          .visibility(Visibility.SYSTEM)
          .build();

  public static final QueryContextParameter<String> TIMESTAMP_RESULT_FIELD_GRANULARITY =
      stringParameter("timestampResultFieldGranularity")
          .since("0.22.0").query(Query.JSON, Query.SQL).engine(Engine.NATIVE, Engine.MSQ, Engine.DART)
          .description("JSON-serialized granularity of the output column named by `timestampResultField`.")
          .queryType(QueryType.GROUP_BY)
          .visibility(Visibility.SYSTEM)
          .build();

  public static final QueryContextParameter<Integer> TIMESTAMP_RESULT_FIELD_IN_ORIGINAL_DIMENSIONS =
      integerParameter("timestampResultFieldInOriginalDimensions")
          .since("0.22.0").query(Query.JSON, Query.SQL).engine(Engine.NATIVE, Engine.MSQ, Engine.DART)
          .description("Position of the output column named by `timestampResultField` in the original groupBy dimensions.")
          .queryType(QueryType.GROUP_BY)
          .visibility(Visibility.SYSTEM)
          .build();

  // endregion

  // region Scan, search and timeseries query parameters
  //
  // Public ones are documented in scan-query.md and searchquery.md.
  public static final QueryContextParameter<Integer> MAX_SEGMENT_PARTITIONS_ORDERED_IN_MEMORY =
      integerParameter("maxSegmentPartitionsOrderedInMemory")
          .since("0.15.0-incubating").query(Query.JSON).engine(Engine.NATIVE)
          .constraint(Range.closedRange(1, Integer.MAX_VALUE))
          .description(
              """
              The maximum number of segments scanned per historical when time ordering is used.
              Overrides the identically named config.
              """
          )
          .defaultDescription("`druid.query.scan.maxSegmentPartitionsOrderedInMemory`")
          .queryType(QueryType.SCAN)
          .build();

  public static final QueryContextParameter<Boolean> SCAN_OUTERMOST =
      booleanParameter("scanOutermost")
          .since("0.15.0-incubating").query(Query.JSON, Query.SQL).engine(Engine.NATIVE)
          .defaultValue(true)
          .description(
              """
              Whether a scan query is running on the outermost process, that is, the process the query was sent to.
              Druid sets it to false for the inner queries so that only the outermost process applies time ordering and limits.
              """
          )
          .queryType(QueryType.SCAN)
          .visibility(Visibility.SYSTEM)
          .build();

  public static final QueryContextParameter<String> SEARCH_STRATEGY =
      stringParameter("searchStrategy")
          .since("0.10.0").query(Query.JSON).engine(Engine.NATIVE)
          .description("Overrides the value of `druid.query.search.searchStrategy` for this query.")
          .defaultDescription("`druid.query.search.searchStrategy`")
          .queryType(QueryType.SEARCH)
          .build();

  public static final QueryContextParameter<Boolean> GRAND_TOTAL =
      booleanParameter("grandTotal")
          .since("0.13.0-incubating").query(Query.JSON).engine(Engine.NATIVE)
          .defaultValue(false)
          .description(
              """
              If true, a timeseries query returns an additional row with the totals of all aggregations as its last row.
              """
          )
          .queryType(QueryType.TIMESERIES)
          .build();

  public static final QueryContextParameter<Boolean> DO_AGGREGATE_TOP_N_METRIC_FIRST =
      booleanParameter("doAggregateTopNMetricFirst")
          .since("0.6.112").query(Query.JSON, Query.SQL).engine(Engine.NATIVE)
          .defaultValue(false)
          .description(
              """
              If true, the TopN engine uses the two-pass algorithm that aggregates the ordering metric first,
              and then aggregates the remaining metrics only for the top values.
              """
          )
          .queryType(QueryType.TOP_N)
          .visibility(Visibility.HIDDEN)
          .build();

  public static final QueryContextParameter<Boolean> UNRAVEL =
      booleanParameter("unravel")
          .since("26.0.0").query(Query.JSON, Query.SQL).engine(Engine.NATIVE)
          .defaultValue(true)
          .description(
              """
              Whether a window operator query converts its rows and columns results to rows or frames.
              Druid sets it to false for the inner queries so that only the outermost query converts the results.
              """
          )
          .visibility(Visibility.SYSTEM)
          .build();

  public static final QueryContextParameter<ResultSerializationMode> SERIALIZATION =
      enumParameter("serialization", ResultSerializationMode.class)
          .since("32.0.0").query(Query.JSON, Query.SQL).engine(Engine.NATIVE)
          .description(
              """
              Format, `rows` or `frames`, in which the Broker materializes the results of a subquery.
              The Broker sets it for each subquery that it inlines.
              """
          )
          .visibility(Visibility.SYSTEM)
          .build();

  // endregion

  // region MSQ and Dart parameters
  //
  // These descriptors are read by the MSQ task engine and Dart. Public ones are documented in
  // multi-stage-query/reference.md and dart.md. Values whose types live in the multi-stage-query module are declared as
  // strings and converted to those types where they are read.
  public static final QueryContextParameter<Integer> MAX_NUM_TASKS =
      integerParameter("maxNumTasks")
          .since("24.0.0").query(Query.SQL).engine(Engine.MSQ)
          .defaultValue(2)
          .description(
              """
              The maximum total number of tasks to launch, including the controller task.
              The lowest possible value for this setting is 2: one controller and one worker.
              All tasks must be able to launch simultaneously.
              If they cannot, the query returns a `TaskStartTimeout` error code after approximately 10 minutes.
              """
          )
          .statement(StatementType.SELECT, StatementType.INSERT, StatementType.REPLACE)
          .build();

  public static final QueryContextParameter<String> TASK_ASSIGNMENT_STRATEGY =
      stringParameter("taskAssignment")
          .since("24.0.0").query(Query.SQL).engine(Engine.MSQ)
          .defaultValue("max")
          .description(
              """
              Determines how many tasks to use.
              Possible values include: <ul><li>`max`: Uses as many tasks as possible, up to `maxNumTasks`.</li><li>`auto`: When file sizes can be determined through directory listing (for example: local files, S3, GCS, HDFS) uses as few tasks as possible without exceeding 512 MiB or 10,000 files per task, unless exceeding these limits is necessary to stay within `maxNumTasks`.
              When calculating the size of files, the weighted size is used, which considers the file format and compression format used if any.
              When file sizes cannot be determined through directory listing (for example: http), behaves the same as `max`.</li></ul>
              """
          )
          .statement(StatementType.SELECT, StatementType.INSERT, StatementType.REPLACE)
          .build();

  public static final QueryContextParameter<Boolean> FINALIZE_AGGREGATIONS =
      booleanParameter("finalizeAggregations")
          .since("24.0.0").query(Query.SQL).engine(Engine.MSQ, Engine.DART)
          .defaultValue(true)
          .description(
              """
              Determines the type of aggregation to return.
              If true, Druid finalizes the results of complex aggregations that directly appear in query results.
              If false, Druid returns the aggregation's intermediate type rather than finalized type.
              This parameter is useful during ingestion, where it enables storing sketches directly in Druid tables.
              For more information about aggregations, see [SQL aggregation functions](../querying/sql-aggregations.md).
              """
          )
          .statement(StatementType.SELECT, StatementType.INSERT, StatementType.REPLACE)
          .build();

  public static final QueryContextParameter<String> ARRAY_INGEST_MODE =
      stringParameter("arrayIngestMode")
          .since("28.0.0").query(Query.SQL).engine(Engine.MSQ)
          .defaultValue("array")
          .description(
              """
              Controls how ARRAY type values are stored in Druid segments.
              When set to `array` (the default, recommended for SQL compliance), Druid will store all ARRAY typed values in [ARRAY typed columns](../querying/arrays.md), and supports storing both VARCHAR and numeric typed arrays.
              When set to `mvd` (for backwards compatibility), Druid only supports VARCHAR typed arrays, and will store them as [multi-value string columns](../querying/multi-value-dimensions.md).
              See [`arrayIngestMode`] in the [Arrays](../querying/arrays.md) page for more details.
              """
          )
          .statement(StatementType.INSERT, StatementType.REPLACE)
          .build();

  public static final QueryContextParameter<Integer> MAX_ROWS_IN_MEMORY =
      integerParameter("maxRowsInMemory")
          .since("36.0.0").query(Query.SQL).engine(Engine.MSQ)
          .defaultValue(100_000)
          .description(
              """
              Maximum number of rows to store in memory at once before flushing to disk during the segment generation process.
              Ignored for non-INSERT queries.
              In most cases, use the default value.
              You may need to override the default if you run into one of the [known issues](./known-issues.md) around memory usage.
              """
          )
          .statement(StatementType.INSERT, StatementType.REPLACE)
          .build();

  public static final QueryContextParameter<Integer> ROWS_IN_MEMORY =
      integerParameter("rowsInMemory")
          .since("24.0.0").query(Query.SQL).engine(Engine.MSQ)
          .defaultValue(100_000)
          .description(
              """
              Alternate spelling of `maxRowsInMemory`.
              Ignored if `maxRowsInMemory` is set.
              """
          )
          .statement(StatementType.INSERT, StatementType.REPLACE)
          .build();

  public static final QueryContextParameter<String> SEGMENT_SORT_ORDER =
      stringParameter("segmentSortOrder")
          .since("24.0.0").query(Query.SQL).engine(Engine.MSQ)
          .description(
              """
              Normally, Druid sorts rows in individual segments using `__time` first, followed by the [CLUSTERED BY](#clustered-by) clause.
              When you set `segmentSortOrder`, Druid uses the order from this context parameter instead.
              Provide the column list as comma-separated values or as a JSON array in string form.<br /><br />For example, consider an INSERT query that uses `CLUSTERED BY country` and has `segmentSortOrder` set to `__time,city,country`.
              Within each time chunk, Druid assigns rows to segments based on `country`, and then within each of those segments, Druid sorts those rows by `__time` first, then `city`, then `country`.
              """
          )
          .defaultDescription("empty list")
          .statement(StatementType.INSERT, StatementType.REPLACE)
          .build();

  public static final QueryContextParameter<Boolean> FORCE_SEGMENT_SORT_BY_TIME =
      booleanParameter("forceSegmentSortByTime")
          .since("31.0.0").query(Query.SQL).engine(Engine.MSQ)
          .defaultValue(DimensionsSpec.DEFAULT_FORCE_TIME_SORT)
          .description(
              """
              When set to `true` (the default), Druid prepends `__time` to [CLUSTERED BY](#clustered-by) when determining the sort order for individual segments.
              Druid also requires that `segmentSortOrder`, if provided, starts with `__time`.<br /><br />When set to `false`, Druid uses the [CLUSTERED BY](#clustered-by) alone to determine the sort order for individual segments, and does not require that `segmentSortOrder` begin with `__time`.
              Setting this parameter to `false` is an experimental feature; see [Sorting](../ingestion/partitioning.md#sorting) for details.
              """
          )
          .statement(StatementType.INSERT, StatementType.REPLACE)
          .build();

  public static final QueryContextParameter<Integer> ROWS_PER_SEGMENT =
      integerParameter("rowsPerSegment")
          .since("24.0.0").query(Query.SQL).engine(Engine.MSQ)
          .defaultValue(3_000_000)
          .description(
              """
              The number of rows per segment to target.
              The actual number of rows per segment may be somewhat higher or lower than this number.
              In most cases, use the default.
              For general information about sizing rows per segment, see [Segment Size Optimization](../operations/segment-optimization.md).
              """
          )
          .statement(StatementType.INSERT, StatementType.REPLACE)
          .build();

  public static final QueryContextParameter<Object> INDEX_SPEC =
      objectParameter("indexSpec")
          .since("25.0.0").query(Query.SQL).engine(Engine.MSQ)
          .description(
              """
              An [`indexSpec`](../ingestion/ingestion-spec.md#indexspec) to use when generating segments.
              May be a JSON string or object.
              See [Front coding](../ingestion/ingestion-spec.md#front-coding) for details on configuring an `indexSpec` with front coding.
              """
          )
          .defaultDescription("See [`indexSpec`](../ingestion/ingestion-spec.md#indexspec).")
          .statement(StatementType.INSERT, StatementType.REPLACE)
          .build();

  public static final QueryContextParameter<Boolean> DURABLE_SHUFFLE_STORAGE =
      booleanParameter("durableShuffleStorage")
          .since("24.0.0").query(Query.SQL).engine(Engine.MSQ)
          .defaultValue(false)
          .description(
              """
              Whether to use durable storage for shuffle mesh.
              To use this feature, configure the durable storage at the server level using `druid.msq.intermediate.storage.enable=true`).
              If these properties are not configured, any query with the context variable `durableShuffleStorage=true` fails with a configuration error.
              """
          )
          .statement(StatementType.SELECT, StatementType.INSERT, StatementType.REPLACE)
          .build();

  public static final QueryContextParameter<Boolean> FAULT_TOLERANCE =
      booleanParameter("faultTolerance")
          .since("26.0.0").query(Query.SQL).engine(Engine.MSQ)
          .defaultValue(false)
          .description(
              """
              Whether to turn on fault tolerance mode or not.
              Failed workers are retried based on [Limits](#limits).
              Cannot be used when `durableShuffleStorage` is explicitly set to false.
              """
          )
          .statement(StatementType.SELECT, StatementType.INSERT, StatementType.REPLACE)
          .build();

  public static final QueryContextParameter<String> SELECT_DESTINATION =
      stringParameter("selectDestination")
          .since("27.0.0").query(Query.SQL).engine(Engine.MSQ)
          .defaultValue("taskReport")
          .description(
              """
              Controls where the final result of the select query is written.
              <br />Use `taskReport`(the default) to write select results to the task report.
              <b> This is not scalable since task reports size explodes for large results </b> <br/>Use `durableStorage` to write results to durable storage location.
              <b>For large results sets, its recommended to use `durableStorage` </b>.
              To configure durable storage see [`this`](#durable-storage) section.
              """
          )
          .statement(StatementType.SELECT)
          .build();

  public static final QueryContextParameter<Boolean> WAIT_UNTIL_SEGMENTS_LOAD =
      booleanParameter("waitUntilSegmentsLoad")
          .since("29.0.0").query(Query.SQL).engine(Engine.MSQ)
          .defaultValue(false)
          .description(
              """
              If set, the ingest query waits for the generated segments to be loaded before exiting, else the ingest query exits without waiting.
              The task and live reports contain the information about the status of loading segments if this flag is set.
              This will ensure that any future queries made after the ingestion exits will include results from the ingestion.
              The drawback is that the controller task will stall till the segments are loaded.
              """
          )
          .statement(StatementType.INSERT, StatementType.REPLACE)
          .build();

  public static final QueryContextParameter<String> INCLUDE_SEGMENT_SOURCE =
      stringParameter("includeSegmentSource")
          .since("28.0.0").query(Query.SQL).engine(Engine.MSQ, Engine.DART)
          .description(
              """
              Controls the sources Druid queries for results in addition to the segments present on deep storage.
              Can be `NONE` or `REALTIME`.
              If set to `NONE`, only non-realtime (published and used) segments are downloaded from deep storage.
              If set to `REALTIME`, results are also included from realtime tasks.
              `REALTIME` cannot be used while writing data into the same datasource it is read from.
              """
          )
          .defaultDescription("`NONE` for the MSQ task engine, `REALTIME` for Dart")
          .statement(StatementType.SELECT, StatementType.INSERT, StatementType.REPLACE)
          .build();

  public static final QueryContextParameter<AcquireMode> SEGMENT_ACQUIRE_MODE =
      enumParameter("segmentAcquireMode", AcquireMode.class)
          .since("39.0.0").query(Query.JSON, Query.SQL).engine(Engine.MSQ, Engine.DART)
          .description(
              """
              Overrides how leaf stages acquire segments from the segment cache.
              `FULL` downloads each segment up front, while `PARTIAL` downloads only the parts a query reads.
              `PARTIAL` falls back to `FULL` where partial downloads are unavailable.
              Intended for testing, debugging and performance comparison.
              """
          )
          .defaultDescription("Chosen by the engine")
          .visibility(Visibility.HIDDEN)
          .build();

  public static final QueryContextParameter<Integer> ROWS_PER_PAGE =
      integerParameter("rowsPerPage")
          .since("29.0.0").query(Query.SQL).engine(Engine.MSQ)
          .defaultValue(100_000)
          .description(
              """
              The number of rows per page to target.
              The actual number of rows per page may be somewhat higher or lower than this number.
              In most cases, use the default.<br /> This property comes into effect only when `selectDestination` is set to `durableStorage`
              """
          )
          .statement(StatementType.SELECT)
          .build();

  public static final QueryContextParameter<String> SKIP_TYPE_VERIFICATION =
      stringParameter("skipTypeVerification")
          .since("30.0.0").query(Query.SQL).engine(Engine.MSQ)
          .description(
              """
              During query validation, Druid validates that [string arrays](../querying/arrays.md) and [multi-value dimensions](../querying/multi-value-dimensions.md) are not mixed in the same column.
              If you are intentionally migrating from one to the other, use this context parameter to disable type validation.<br /><br />Provide the column list as comma-separated values or as a JSON array in string form.
              """
          )
          .defaultDescription("empty list")
          .statement(StatementType.INSERT, StatementType.REPLACE)
          .build();

  public static final QueryContextParameter<Boolean> FAIL_ON_EMPTY_INSERT =
      booleanParameter("failOnEmptyInsert")
          .since("29.0.0").query(Query.SQL).engine(Engine.MSQ)
          .defaultValue(false)
          .description(
              """
              When set to false (the default), an INSERT query generating no output rows will be no-op, and a REPLACE query generating no output rows will delete all data that matches the OVERWRITE clause.
              When set to true, an ingest query generating no output rows will throw an `InsertCannotBeEmpty` fault.
              """
          )
          .statement(StatementType.INSERT, StatementType.REPLACE)
          .build();

  public static final QueryContextParameter<Boolean> FAIL_ON_MULTI_VALUED_RANGE_PARTITION =
      booleanParameter("failOnMultiValuedRangePartition")
          .since("39.0.0").query(Query.SQL).engine(Engine.MSQ)
          .defaultValue(false)
          .description(
              """
              Set by Druid when it compacts a datasource whose partitioning is by range.
              An ingestion query falls back to dynamic partitioning when the data turns out to hold multi-valued fields, since it stores the rows either way.
              Compaction cannot, because the segments it writes would then disagree with the partitioning its config asks for, so it fails instead.
              """
          )
          .statement(StatementType.REPLACE)
          .visibility(Visibility.SYSTEM)
          .build();

  public static final QueryContextParameter<Boolean> REMOVE_NULL_BYTES =
      booleanParameter("removeNullBytes")
          .since("31.0.0").query(Query.SQL).engine(Engine.MSQ, Engine.DART)
          .defaultValue(false)
          .description(
              """
              The MSQ engine can't process null bytes in strings and throws `InvalidNullByteFault` if it encounters them in the source data.
              If the parameter is set to true, the MSQ engine removes the null bytes in string fields when reading the data.
              """
          )
          .statement(StatementType.SELECT, StatementType.INSERT, StatementType.REPLACE)
          .build();

  public static final QueryContextParameter<Boolean> INCLUDE_ALL_COUNTERS =
      booleanParameter("includeAllCounters")
          .since("31.0.0").query(Query.SQL).engine(Engine.MSQ)
          .defaultValue(true)
          .description(
              """
              Whether to include counters that were added in Druid 31 or later.
              This is a backwards compatibility option that must be set to `false` during a rolling update from versions prior to Druid 31.
              """
          )
          .statement(StatementType.SELECT, StatementType.INSERT, StatementType.REPLACE)
          .build();

  public static final QueryContextParameter<Integer> MAX_FRAME_SIZE =
      integerParameter("maxFrameSize")
          .since("35.0.0").query(Query.SQL).engine(Engine.MSQ, Engine.DART)
          .defaultValue(1_000_000)
          .description(
              """
              Size of frames used for data transfer within the MSQ engine.
              You generally do not need to change this unless you have very large rows.
              """
          )
          .defaultDescription("`1000000` (1 MB)")
          .statement(StatementType.SELECT, StatementType.INSERT, StatementType.REPLACE)
          .build();

  public static final QueryContextParameter<Integer> MAX_INPUT_FILES_PER_WORKER =
      integerParameter("maxInputFilesPerWorker")
          .since("36.0.0").query(Query.SQL).engine(Engine.MSQ)
          .defaultValue(10_000)
          .description(
              """
              Maximum number of input files or segments per worker.
              If a single worker would need to read more than this number of files, the query fails with a `TooManyInputFiles` error.
              In this case, you should either increase this limit if your tasks have enough memory to handle more files, add more workers by increasing `maxNumTasks`, or split your query into smaller queries that process fewer files.
              """
          )
          .statement(StatementType.SELECT, StatementType.INSERT, StatementType.REPLACE)
          .build();

  public static final QueryContextParameter<Boolean> BACKGROUND_FETCH_EXTERNAL_FILES =
      booleanParameter("backgroundFetchExternalFiles")
          .since("38.0.0").query(Query.SQL).engine(Engine.MSQ)
          .defaultValue(true)
          .description(
              """
              Controls how workers read cloud storage files (e.g. `s3`, `gs`, `azure`) referenced by [`EXTERN`](#extern-function).
              When `true` (the default), each worker fetches its input files asynchronously when possible before reading them.
              This overlaps downloading with processing, which generally improves throughput when reading larger numbers of files.
              Otherwise, workers stream each file directly from cloud storage while processing it.<br /><br />This setting has no effect for input sources that use `prefix`, or input sources that request [system fields](../ingestion/input-sources.md).
              """
          )
          .statement(StatementType.SELECT, StatementType.INSERT, StatementType.REPLACE)
          .build();

  public static final QueryContextParameter<Integer> MAX_PARTITIONS =
      integerParameter("maxPartitions")
          .since("36.0.0").query(Query.SQL).engine(Engine.MSQ)
          .defaultValue(25_000)
          .description(
              """
              Maximum number of output partitions for any single stage.
              For INSERT or REPLACE queries, this controls the maximum number of segments that can be generated.
              If the query would exceed this limit, it fails with a `TooManyPartitions` error.
              You can increase this limit if needed, break your query into smaller queries, or use a larger target segment size (via `rowsPerSegment`).
              """
          )
          .statement(StatementType.SELECT, StatementType.INSERT, StatementType.REPLACE)
          .build();

  public static final QueryContextParameter<Integer> MAX_THREADS =
      integerParameter("maxThreads")
          .since("35.0.0").query(Query.SQL).engine(Engine.MSQ, Engine.DART)
          .description(
              """
              Maximum number of threads to use for processing.
              This only has an effect if it is greater than zero and less than the default thread count based on system configuration.
              Otherwise, it is ignored, and workers use the default thread count.
              """
          )
          .defaultDescription("Not set (use default thread count)")
          .statement(StatementType.SELECT, StatementType.INSERT, StatementType.REPLACE)
          .build();

  public static final QueryContextParameter<Integer> MAX_CONCURRENT_STAGES =
      integerParameter("maxConcurrentStages")
          .since("31.0.0").query(Query.SQL).engine(Engine.MSQ, Engine.DART)
          .description(
              """
              Number of stages that can run concurrently for a query.
              A higher number can potentially improve pipelining but results in less memory available for each stage.
              """
          )
          .defaultDescription("`2` for Dart, `1` for the MSQ task engine")
          .statement(StatementType.SELECT, StatementType.INSERT, StatementType.REPLACE)
          .build();

  public static final QueryContextParameter<Integer> MAX_NON_LEAF_WORKERS =
      integerParameter("maxNonLeafWorkers")
          .since("32.0.0").query(Query.SQL).engine(Engine.DART)
          .defaultValue(1)
          .description("Number of workers to use for stages beyond the leaf stage.")
          .defaultDescription("1 (Scatter-gather style)")
          .statement(StatementType.SELECT)
          .build();

  public static final QueryContextParameter<Integer> TARGET_PARTITIONS_PER_WORKER =
      integerParameter("targetPartitionsPerWorker")
          .since("32.0.0").query(Query.SQL).engine(Engine.MSQ, Engine.DART)
          .description(
              """
              Number of partitions Druid generates for each worker.
              This number controls how much parallelism can be maintained throughout a query.
              """
          )
          .defaultDescription("`1` for Dart, the number of processing threads for the MSQ task engine")
          .statement(StatementType.SELECT, StatementType.INSERT, StatementType.REPLACE)
          .build();

  public static final QueryContextParameter<String> MSQ_MODE =
      stringParameter("mode")
          .since("24.0.0").query(Query.SQL).engine(Engine.MSQ)
          .defaultValue("strict")
          .description(
              """
              MSQ mode, which sets defaults for other context parameters.
              `strict` sets `maxParseExceptions` to 0, and `nonStrict` sets it to -1.
              Values that the query context sets explicitly take precedence over the mode defaults.
              """
          )
          .statement(StatementType.SELECT, StatementType.INSERT, StatementType.REPLACE)
          .visibility(Visibility.HIDDEN)
          .build();

  public static final QueryContextParameter<Long> MAX_INPUT_BYTES_PER_WORKER =
      longParameter("maxInputBytesPerWorker")
          .since("26.0.0").query(Query.SQL).engine(Engine.MSQ)
          .defaultValue(512L * 1024 * 1024)
          .description(
              """
              Maximum number of input bytes given to a single worker when `taskAssignment` is `auto`.
              When dividing the input of a stage among the workers, Druid moves on to the next worker once a worker reaches this size.
              This is only a guideline during input slicing and doesn't guarantee that a worker's input can't be larger.
              """
          )
          .statement(StatementType.SELECT, StatementType.INSERT, StatementType.REPLACE)
          .visibility(Visibility.HIDDEN)
          .build();

  public static final QueryContextParameter<Integer> MAX_CLUSTERED_BY_COLUMNS =
      integerParameter("maxClusteredByColumns")
          .since("37.0.0").query(Query.SQL).engine(Engine.MSQ)
          .description(
              """
              Maximum number of columns that can appear in the `CLUSTERED BY` clause.
              Intended for testing.
              """
          )
          .defaultDescription("1500")
          .statement(StatementType.INSERT, StatementType.REPLACE)
          .visibility(Visibility.HIDDEN)
          .build();

  public static final QueryContextParameter<String> CLUSTER_STATISTICS_MERGE_MODE =
      stringParameter("clusterStatisticsMergeMode")
          .since("26.0.0").query(Query.SQL).engine(Engine.MSQ)
          .defaultValue("SEQUENTIAL")
          .description(
              """
              Whether the controller merges the key statistics sketches of workers in parallel or sequentially.
              Possible values are `PARALLEL`, `SEQUENTIAL` and `AUTO`.
              `AUTO` chooses between the two based on the number of workers and the size of the input.
              """
          )
          .statement(StatementType.SELECT, StatementType.INSERT, StatementType.REPLACE)
          .visibility(Visibility.HIDDEN)
          .build();

  public static final QueryContextParameter<String> SKETCH_ENCODING =
      stringParameter("sketchEncoding")
          .since("32.0.0").query(Query.SQL).engine(Engine.MSQ)
          .defaultValue("OCTET_STREAM")
          .description(
              """
              Encoding that the controller requests for the key statistics sketches it fetches from workers.
              Possible values are `OCTET_STREAM` and `JSON`.
              """
          )
          .statement(StatementType.SELECT, StatementType.INSERT, StatementType.REPLACE)
          .visibility(Visibility.HIDDEN)
          .build();

  public static final QueryContextParameter<Boolean> USE_COMBINER =
      booleanParameter("useCombiner")
          .since("37.0.0").query(Query.SQL).engine(Engine.MSQ, Engine.DART)
          .defaultValue(false)
          .description("Whether stage processors attempt to combine rows with the same key when doing sort-based aggregations.")
          .statement(StatementType.SELECT, StatementType.INSERT, StatementType.REPLACE)
          .visibility(Visibility.HIDDEN)
          .build();

  public static final QueryContextParameter<Integer> MAX_NUM_SEGMENTS =
      integerParameter("maxNumSegments")
          .since("31.0.0").query(Query.SQL).engine(Engine.MSQ)
          .description(
              """
              Maximum number of segments that an ingestion query can generate in a single time chunk.
              If the query would exceed this limit, it fails with a `TooManySegmentsInTimeChunk` error.
              """
          )
          .defaultDescription("No limit")
          .statement(StatementType.INSERT, StatementType.REPLACE)
          .build();

  public static final QueryContextParameter<Boolean> USE_AUTO_COLUMN_SCHEMAS =
      booleanParameter("useAutoColumnSchemas")
          .since("27.0.0").query(Query.SQL).engine(Engine.MSQ)
          .defaultValue(false)
          .description(
              """
              Experimental flag that makes ingestion use the `auto` column schema for all standard-typed columns during
              segment generation.
              """
          )
          .statement(StatementType.INSERT, StatementType.REPLACE)
          .visibility(Visibility.HIDDEN)
          .build();

  public static final QueryContextParameter<Boolean> LIVE_REPORT_COUNTERS =
      booleanParameter("liveReportCounters")
          .since("36.0.0").query(Query.SQL).engine(Engine.MSQ, Engine.DART)
          .description(
              """
              Whether workers periodically send counter snapshots to the controller, so that the controller has more
              up-to-date progress information.
              """
          )
          .defaultDescription("`true` for the MSQ task engine, `false` for Dart")
          .statement(StatementType.SELECT, StatementType.INSERT, StatementType.REPLACE)
          .visibility(Visibility.HIDDEN)
          .build();

  public static final QueryContextParameter<Integer> MAX_ROWS_MATERIALIZED_IN_WINDOW =
      integerParameter("maxRowsMaterializedInWindow")
          .since("30.0.0").query(Query.SQL).engine(Engine.MSQ, Engine.DART)
          .defaultValue(100_000)
          .description(
              """
              Maximum number of rows that window functions can materialize in a single window partition.
              If a partition exceeds this limit, the query fails with a `TooManyRowsInAWindow` error.
              """
          )
          .statement(StatementType.SELECT, StatementType.INSERT, StatementType.REPLACE)
          .build();

  public static final QueryContextParameter<Integer> SEGMENT_LOAD_AHEAD_COUNT =
      integerParameter("segmentLoadAheadCount")
          .since("37.0.0").query(Query.SQL).engine(Engine.MSQ, Engine.DART)
          .description(
              """
              Number of segments that a worker loads ahead of them being needed.
              When set, it takes precedence over the worker's local default.
              """
          )
          .defaultDescription("`druid.msq.dart.worker.segmentLoadAheadCount` for Dart, otherwise twice the number of processing threads")
          .statement(StatementType.SELECT, StatementType.INSERT, StatementType.REPLACE)
          .visibility(Visibility.HIDDEN)
          .build();

  public static final QueryContextParameter<Boolean> IS_REINDEX =
      booleanParameter("isReindex")
          .since("28.0.0").query(Query.SQL).engine(Engine.MSQ)
          .defaultValue(false)
          .description("Set by Druid when an ingestion query replaces data in a datasource that it also reads from.")
          .statement(StatementType.REPLACE)
          .visibility(Visibility.SYSTEM)
          .build();

  public static final QueryContextParameter<String> START_TIME =
      stringParameter("startTime")
          .since("34.0.0").query(Query.SQL).engine(Engine.MSQ, Engine.DART)
          .description(
              """
              ISO 8601 time at which the Broker started the query.
              Druid uses it to compute query deadlines and report timings.
              """
          )
          .statement(StatementType.SELECT, StatementType.INSERT, StatementType.REPLACE)
          .visibility(Visibility.SYSTEM)
          .build();

  public static final QueryContextParameter<String> QUERY_DEADLINE =
      stringParameter("queryDeadline")
          .since("37.0.0").query(Query.SQL).engine(Engine.MSQ, Engine.DART)
          .description(
              """
              ISO 8601 time at which the query times out.
              The Broker computes it from `startTime` and `timeout`.
              """
          )
          .statement(StatementType.SELECT, StatementType.INSERT, StatementType.REPLACE)
          .visibility(Visibility.SYSTEM)
          .build();

  public static final QueryContextParameter<Integer> ROW_BASED_FRAME_TYPE =
      integerParameter("rowBasedFrameType")
          .since("34.0.0").query(Query.SQL).engine(Engine.MSQ, Engine.DART)
          .defaultValue((int) FrameType.ROW_BASED_V1.version())
          .description(
              """
              Version of the row-based frame format that MSQ uses.
              The Broker sets it to the latest version, and older servers fall back to the default so that rolling updates
              from older Druid versions keep working.
              """
          )
          .statement(StatementType.SELECT, StatementType.INSERT, StatementType.REPLACE)
          .visibility(Visibility.SYSTEM)
          .build();

  public static final QueryContextParameter<Boolean> WINDOW_FUNCTION_OPERATOR_TRANSFORMATION =
      booleanParameter("windowFunctionOperatorTransformation")
          .since("32.0.0").query(Query.SQL).engine(Engine.MSQ, Engine.DART)
          .defaultValue(false)
          .description(
              """
              Whether MSQ uses the transformed operator chain for window functions.
              The Broker sets it to `true` so that rolling updates from older Druid versions keep working.
              """
          )
          .statement(StatementType.SELECT, StatementType.INSERT, StatementType.REPLACE)
          .visibility(Visibility.SYSTEM)
          .build();

  public static final QueryContextParameter<Long> MAX_PARSE_EXCEPTIONS =
      longParameter("maxParseExceptions")
          .since("24.0.0").query(Query.SQL).engine(Engine.MSQ)
          .description(
              """
              Maximum number of parse exceptions that are ignored while executing the query before it stops with `TooManyWarningsFault`.
              To ignore all the parse exceptions, set the value to -1.
              """
          )
          .defaultDescription("`0`")
          .statement(StatementType.SELECT, StatementType.INSERT, StatementType.REPLACE)
          .build();

  public static final QueryContextParameter<Boolean> STORE_COMPACTION_STATE =
      booleanParameter("storeCompactionState")
          .since("30.0.0").query(Query.SQL).engine(Engine.MSQ)
          .defaultValue(false)
          .description(
              """
              When set to true, a REPLACE query stores as part of each segment's metadata a `lastCompactionState` field that captures the various specs used to create the segment.
              Future compaction jobs skip segments whose `lastCompactionState` matches the desired compaction state.
              Works the same as [`storeCompactionState`](../ingestion/tasks.md#context-parameters) task context flag.
              """
          )
          .statement(StatementType.REPLACE)
          .build();

  public static final QueryContextParameter<String> TASK_LOCK_TYPE =
      stringParameter("taskLockType")
          .since("29.0.0").query(Query.SQL).engine(Engine.MSQ)
          .description(
              """
              Type of time chunk lock that an INSERT or REPLACE query acquires: `EXCLUSIVE`, `SHARED`, `APPEND` or `REPLACE`.
              If not set, REPLACE queries use `EXCLUSIVE` and INSERT queries use `SHARED`.
              Ignored when `useConcurrentLocks` is true.
              """
          )
          .defaultDescription("`EXCLUSIVE` for REPLACE, `SHARED` for INSERT")
          .statement(StatementType.INSERT, StatementType.REPLACE)
          .build();

  public static final QueryContextParameter<Boolean> USE_CONCURRENT_LOCKS =
      booleanParameter("useConcurrentLocks")
          .since("30.0.0").query(Query.SQL).engine(Engine.MSQ)
          .defaultValue(false)
          .description(
              """
              Whether an INSERT or REPLACE query uses concurrent locks: `APPEND` locks for INSERT and `REPLACE` locks for REPLACE.
              See [concurrent append and replace](../ingestion/concurrent-append-replace.md).
              """
          )
          .statement(StatementType.INSERT, StatementType.REPLACE)
          .build();

  public static final QueryContextParameter<String> USER =
      stringParameter("__user")
          .since("27.0.0").query(Query.SQL).engine(Engine.MSQ)
          .description("Identity of the user who submitted the query. Set by Druid so that later requests about the query can be authorized.")
          .visibility(Visibility.SYSTEM)
          .build();

  public static final QueryContextParameter<String> RESULT_FORMAT =
      stringParameter("__resultFormat")
          .since("28.0.0").query(Query.SQL).engine(Engine.MSQ)
          .description("Result format of a query submitted through the SQL statements API. Set by Druid from the request.")
          .visibility(Visibility.SYSTEM)
          .build();

  public static final QueryContextParameter<String> TIME_COLUMN =
      stringParameter("__timeColumn")
          .since("24.0.0").query(Query.SQL).engine(Engine.MSQ)
          .description("Name of the output column that holds the time of each row of an ingestion query. Set by the SQL planner.")
          .statement(StatementType.INSERT, StatementType.REPLACE)
          .visibility(Visibility.SYSTEM)
          .build();

  // endregion

  // region Other query context parameters
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
      .defaultValue(false)
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
          .defaultValue(QueryContexts.RealtimeSegmentsMode.INCLUDE)
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
      .defaultValue(true)
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

  public static final QueryContextParameter<String> POST_PROCESSING =
      stringParameter("postProcessing")
          .since("0.6.148").query(Query.JSON).engine(Engine.NATIVE)
          .description("JSON-serialized post-processing operator, such as `timewarp`, that the Broker applies to the query results.")
          .visibility(Visibility.HIDDEN)
          .build();

  public static final QueryContextParameter<Boolean> SKIP_INCREMENTAL_SEGMENT =
      booleanParameter("skipIncrementalSegment")
          .since("0.9.0").query(Query.JSON, Query.SQL).engine(Engine.NATIVE)
          .defaultValue(false)
          .description("If true, realtime tasks skip the in-memory incremental segments that haven't been persisted yet when processing a query.")
          .visibility(Visibility.HIDDEN)
          .build();

  public static final QueryContextParameter<String> EXPORT_FILE_FORMAT =
      stringParameter("__exportFileFormat")
          .since("30.0.0").query(Query.SQL).engine(Engine.MSQ)
          .description("Result format of an `INSERT INTO EXTERN` export. Set by the SQL planner from the `AS` clause.")
          .statement(StatementType.INSERT)
          .visibility(Visibility.SYSTEM)
          .build();

  public static final QueryContextParameter<Long> QUERY_FAIL_TIME =
      longParameter("queryFailTime")
          .since("0.10.1").query(Query.JSON, Query.SQL).engine(Engine.NATIVE)
          .description("Wall-clock time, in milliseconds since the epoch, after which a data server fails the query with a timeout. Set by the Broker from the query timeout.")
          .visibility(Visibility.SYSTEM)
          .build();

  public static final QueryContextParameter<String> MAX_TIME_ARRAY_OUTPUT_NAME =
      stringParameter("maxTimeArrayOutputName")
          .since("24.0.0").query(Query.SQL).engine(Engine.NATIVE)
          .description("Output column name for the maximum time of a time boundary query generated by the SQL planner.")
          .visibility(Visibility.SYSTEM)
          .build();

  public static final QueryContextParameter<String> MIN_TIME_ARRAY_OUTPUT_NAME =
      stringParameter("minTimeArrayOutputName")
          .since("24.0.0").query(Query.SQL).engine(Engine.NATIVE)
          .description("Output column name for the minimum time of a time boundary query generated by the SQL planner.")
          .visibility(Visibility.SYSTEM)
          .build();

  // endregion

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

  static QueryContextParameter.Builder<Float> floatParameter(final String name)
  {
    return QueryContextParameter.builder(
        name,
        Float.class,
        value -> {
          if (value instanceof Number) {
            return ((Number) value).floatValue();
          } else if (value instanceof String) {
            try {
              return Float.parseFloat((String) value);
            }
            catch (NumberFormatException _) {
              // Fall through to the invalid-value error below.
            }
          }
          throw invalidValueException(name, "in float format", value);
        }
    );
  }

  /**
   * Creates a parameter whose values are numbers of bytes, or strings with a unit suffix such as {@code "1GiB"}, as
   * parsed by {@link HumanReadableBytes#parse(String)}.
   */
  static QueryContextParameter.Builder<HumanReadableBytes> humanReadableBytesParameter(final String name)
  {
    return QueryContextParameter.builder(
        name,
        HumanReadableBytes.class,
        value -> {
          if (value instanceof Number) {
            return HumanReadableBytes.valueOf(((Number) value).longValue());
          } else if (value instanceof String) {
            try {
              return HumanReadableBytes.valueOf(HumanReadableBytes.parse((String) value));
            }
            catch (IAE _) {
              // Fall through to the invalid-value error below.
            }
          }
          throw invalidValueException(name, "a human readable number", value);
        }
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

  /**
   * Creates an enum parameter whose string values are the enum's constant names, matched case-insensitively, so the
   * enum's JSON form must equal its constant name ignoring case. The enum must also follow the JSON form convention
   * described on {@link QueryContextParameter}.
   */
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
   * The enum must follow the JSON form convention described on {@link QueryContextParameter}.
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

  static QueryContextParameter.Builder<Object> objectParameter(final String name)
  {
    return QueryContextParameter.builder(name, Object.class, value -> value);
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
    catch (NumberFormatException _) {
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
