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

package org.apache.druid.extensions.openlineage;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWith;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.lifecycle.LifecycleStart;
import org.apache.druid.java.util.common.lifecycle.LifecycleStop;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.BaseQuery;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryColumnUsageAnalyzer;
import org.apache.druid.query.union.UnionQuery;
import org.apache.druid.server.RequestLogLine;
import org.apache.druid.server.log.RequestLogger;
import org.apache.druid.sql.calcite.parser.DruidSqlParser;
import org.apache.druid.sql.calcite.parser.ExternalDestinationSqlIdentifier;
import org.apache.druid.utils.CloseableUtils;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * OpenLineage RunEvents for completed Druid queries.
 */
public class OpenLineageRequestLogger implements RequestLogger
{
  private static final Logger log = new Logger(OpenLineageRequestLogger.class);

  private static final String PRODUCER =
      "https://github.com/apache/druid/tree/master/extensions-contrib/openlineage-emitter";
  private static final String SCHEMA_URL =
      "https://openlineage.io/spec/2-0-2/OpenLineage.json";
  private static final String ENGINE_FACET_SCHEMA_URL =
      "https://openlineage.io/spec/facets/1-1-1/ProcessingEngineRunFacet.json";
  private static final String ERROR_FACET_SCHEMA_URL =
      "https://openlineage.io/spec/facets/1-0-0/ErrorMessageRunFacet.json";
  private static final String JOB_TYPE_FACET_SCHEMA_URL =
      "https://openlineage.io/spec/facets/2-0-2/JobTypeJobFacet.json";
  private static final String SQL_FACET_SCHEMA_URL =
      "https://openlineage.io/spec/facets/1-0-1/SQLJobFacet.json";
  // raw.githubusercontent.com so consumers can dereference the URL and receive JSON, not an HTML tree page.
  private static final String CUSTOM_SCHEMA_BASE =
      "https://raw.githubusercontent.com/apache/druid/master/extensions-contrib/openlineage-emitter"
      + "/src/main/resources/openlineage-schema/";
  private static final String CONTEXT_FACET_SCHEMA_URL = CUSTOM_SCHEMA_BASE + "DruidQueryContextRunFacet.json";
  private static final String STATS_FACET_SCHEMA_URL = CUSTOM_SCHEMA_BASE + "DruidQueryStatisticsRunFacet.json";
  // Standard OpenLineage SchemaDatasetFacet listing the input columns referenced by the query (names only).
  private static final String SCHEMA_FACET_SCHEMA_URL =
      "https://openlineage.io/spec/facets/1-1-1/SchemaDatasetFacet.json";
  // Custom dataset facet describing how each referenced column was used (filter, group-by, aggregation, ...).
  private static final String COLUMN_USAGE_FACET_SCHEMA_URL =
      CUSTOM_SCHEMA_BASE + "DruidColumnUsageDatasetFacet.json";
  static final int SQL_FACET_MAX_LENGTH = 64 * 1024;
  static final int DEFAULT_EMIT_QUEUE_CAPACITY = 1000;
  static final int DEFAULT_EMIT_THREAD_COUNT = 1;
  private static final int DISCARD_WARNING_INTERVAL = 1000;
  /**
   * Number of attempts per event (1 initial + MAX_SEND_RETRIES retries).
   * Delivery is at-most-once after all attempts are exhausted: if every attempt
   * fails the event is dropped and a warning is logged.
   */
  static final int MAX_SEND_RETRIES = 2;
  private static final long RETRY_SLEEP_MS = 500;

  static final String UNKNOWN_QUERY_ID = "unknown-query-id";

  private final ObjectMapper jsonMapper;
  private final String namespace;
  private final OpenLineageRequestLoggerProvider.TransportType transportType;
  @Nullable
  private final String transportUrl;
  private final Set<String> excludedNativeQueryTypes;
  private final boolean columnLineageEnabled;
  @Nullable
  private final HttpClient httpClient;
  @Nullable
  private final ExecutorService emitExecutor;
  private final AtomicLong discardedEventCount = new AtomicLong(0);

  public OpenLineageRequestLogger(
      ObjectMapper jsonMapper,
      String namespace,
      OpenLineageRequestLoggerProvider.TransportType transportType,
      @Nullable String transportUrl,
      Set<String> excludedNativeQueryTypes
  )
  {
    this(jsonMapper, namespace, transportType, transportUrl, excludedNativeQueryTypes,
         true, DEFAULT_EMIT_QUEUE_CAPACITY, DEFAULT_EMIT_THREAD_COUNT, null);
  }

  public OpenLineageRequestLogger(
      ObjectMapper jsonMapper,
      String namespace,
      OpenLineageRequestLoggerProvider.TransportType transportType,
      @Nullable String transportUrl,
      Set<String> excludedNativeQueryTypes,
      boolean columnLineageEnabled,
      int emitQueueCapacity,
      int emitThreadCount,
      @Nullable HttpClient httpClient
  )
  {
    this.jsonMapper = jsonMapper;
    this.namespace = namespace;
    this.transportType = transportType;
    this.transportUrl = transportUrl;
    this.excludedNativeQueryTypes = excludedNativeQueryTypes;
    this.columnLineageEnabled = columnLineageEnabled;
    if (transportType == OpenLineageRequestLoggerProvider.TransportType.HTTP && transportUrl == null) {
      throw new IllegalStateException(
          "druid.request.logging.transportUrl must be set when transportType=HTTP"
      );
    }
    if (transportType == OpenLineageRequestLoggerProvider.TransportType.HTTP) {
      this.httpClient = httpClient != null ? httpClient : HttpClientBuilder.create().build();
      // Bounded queue: if the queue is full, drop the event rather than blocking the query thread.
      // A warning is logged on the first drop and every DISCARD_WARNING_INTERVAL drops thereafter.
      this.emitExecutor = new ThreadPoolExecutor(
          emitThreadCount,
          emitThreadCount,
          60L,
          TimeUnit.SECONDS,
          new ArrayBlockingQueue<>(emitQueueCapacity),
          Execs.makeThreadFactory("OpenLineageEmitter-%d"),
          new DiscardWithWarningPolicy(discardedEventCount)
      );
    } else {
      this.httpClient = null;
      this.emitExecutor = null;
    }
  }

  // Note: ComposingRequestLogger does not delegate @LifecycleStart to sub-loggers, so this method
  // may not be called when used in a composing configuration. HTTP URL validation is therefore
  // performed in the constructor instead. This method is retained for direct lifecycle use.
  @LifecycleStart
  @Override
  public void start()
  {
    log.info(
        "Started OpenLineage %s transport%s",
        transportType,
        transportUrl != null ? " to [" + transportUrl + "]" : ""
    );
  }

  @LifecycleStop
  @Override
  public void stop()
  {
    if (emitExecutor != null) {
      emitExecutor.shutdown();
      try {
        if (!emitExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
          emitExecutor.shutdownNow();
        }
      }
      catch (InterruptedException e) {
        emitExecutor.shutdownNow();
        Thread.currentThread().interrupt();
      }
    }
    if (httpClient instanceof Closeable) {
      CloseableUtils.closeAndSuppressExceptions(
          (Closeable) httpClient,
          e -> log.warn(e, "Failed to close OpenLineage HTTP client")
      );
    }
    log.info("Stopped OpenLineage request logger");
  }

  @Override
  public void logNativeQuery(RequestLogLine requestLogLine) throws IOException
  {
    if (requestLogLine.getQuery() == null) {
      return;
    }

    String queryType = requestLogLine.getQuery().getType();

    if (excludedNativeQueryTypes.contains(queryType)) {
      return;
    }

    List<String> inputs = new ArrayList<>(extractInputTables(requestLogLine.getQuery()));
    String queryId = requestLogLine.getQuery().getId();
    if (queryId == null) {
      log.debug("Native query reached OpenLineage logger without a query ID");
      queryId = UNKNOWN_QUERY_ID;
    }

    Map<String, Map<String, EnumSet<QueryColumnUsageAnalyzer.ColumnUsage>>> columnsByTable =
        columnLineageEnabled ? extractColumnsByTable(requestLogLine.getQuery()) : null;
    emit(buildRunEvent(queryId, queryType, requestLogLine, inputs, columnsByTable, null));
  }

  /**
   * Collects the input table names of a native query. A top-level {@link UnionQuery} has no single
   * datasource ({@link UnionQuery#getDataSource()} throws by design), so its branches are unioned;
   * every other query exposes its tables through {@code getDataSource().getTableNames()}.
   */
  private static Set<String> extractInputTables(Query<?> query)
  {
    Set<String> tables = new LinkedHashSet<>();
    if (query instanceof UnionQuery) {
      for (DataSource dataSource : ((UnionQuery) query).getDataSources()) {
        tables.addAll(dataSource.getTableNames());
      }
    } else {
      tables.addAll(query.getDataSource().getTableNames());
    }
    return tables;
  }

  /**
   * Emits lineage for MSQ DML statements (INSERT INTO / REPLACE INTO). For native-engine
   * SQL SELECT queries, lineage is emitted from {@link #logNativeQuery} instead, which has
   * structured access to datasource references without requiring SQL parsing.
   *
   * <p>MSQ INSERT/REPLACE queries submit an MSQControllerTask and never produce a native
   * request-log event, so their output lineage must be captured here. The output datasource
   * is extracted from the SQL AST via Druid's SQL parser; inputs are not emitted because
   * reliably extracting FROM/JOIN tables in the logger layer would duplicate planner work.
   */
  @Override
  public void logSqlQuery(RequestLogLine requestLogLine) throws IOException
  {
    String sql = requestLogLine.getSql();
    if (sql == null) {
      return;
    }
    String outputTable = extractMsqOutputDatasource(sql);
    if (outputTable == null) {
      return;
    }

    Map<String, Object> sqlContext = requestLogLine.getSqlQueryContext();
    String queryId = sqlContext != null ? (String) sqlContext.get("sqlQueryId") : null;
    if (queryId == null) {
      log.debug("MSQ SQL query reached OpenLineage logger without a sqlQueryId");
      queryId = UNKNOWN_QUERY_ID;
    }

    emit(buildRunEvent(queryId, "msq", requestLogLine, List.of(), null, outputTable));
  }

  /**
   * Extracts the output datasource name from an MSQ DML statement (INSERT INTO / REPLACE INTO)
   * using Druid's SQL parser. Returns {@code null} for any input that is not a Druid
   * INSERT/REPLACE into a regular datasource — including:
   * <ul>
   *   <li>SELECT and other non-DML statements</li>
   *   <li>{@code INSERT INTO EXTERN(...) AS CSV ...} export statements (target parses as a
   *       SqlCall, not a SqlIdentifier)</li>
   *   <li>SQL that fails to parse</li>
   * </ul>
   *
   * <p>For {@code INSERT INTO druid.foo} or {@code INSERT INTO catalog.druid.foo}, returns just
   * {@code foo} — matching the planner's normalization to a bare datasource name. CTEs in the
   * Druid MSQ form {@code INSERT INTO foo WITH x AS (...) SELECT ...} are handled correctly
   * because the target table is on the outer {@code SqlInsert} node; the CTE appears as the
   * source, not as a wrapper around the statement.
   */
  @Nullable
  static String extractMsqOutputDatasource(String sql)
  {
    try {
      // allowSetStatements=true so that a Druid SET preamble (e.g. "SET sqlQueryId = '...'; INSERT
      // INTO foo ...") doesn't cause the parse to throw. We only inspect the main statement.
      SqlNode node = DruidSqlParser.parse(sql, true).getMainStatement();
      // WITH ... INSERT/REPLACE wraps the ingest with a SqlWith; unwrap it.
      if (node instanceof SqlWith) {
        node = ((SqlWith) node).body;
      }
      // DruidSqlInsert and DruidSqlReplace both extend SqlInsert, so this covers both.
      if (!(node instanceof SqlInsert)) {
        return null;
      }
      SqlNode target = ((SqlInsert) node).getTargetTable();
      // INSERT INTO EXTERN(...) AS CSV writes to a file, not a Druid datasource.
      // ExternalDestinationSqlIdentifier extends SqlIdentifier, so check it explicitly.
      if (target instanceof ExternalDestinationSqlIdentifier) {
        return null;
      }
      if (!(target instanceof SqlIdentifier)) {
        return null;
      }
      SqlIdentifier id = (SqlIdentifier) target;
      // Druid's planner normalizes any schema/catalog prefix (e.g. "druid.foo",
      // "catalog.druid.foo") to the bare datasource name. Match that here.
      return id.names.isEmpty() ? null : id.names.get(id.names.size() - 1);
    }
    catch (Exception e) {
      log.debug(e, "Failed to parse SQL for MSQ output datasource extraction; skipping output lineage");
      return null;
    }
  }

  /**
   * Resolves the per-base-table column-usage map for a native query by delegating to the core
   * {@link QueryColumnUsageAnalyzer}, then used to attach the {@code schema} and {@code druid_columnUsage}
   * dataset facets to input datasets. Returns {@code null} (yielding table-level lineage only) when no
   * base-table columns can be determined, or on any error -- lineage extraction must never break the
   * request-logging path, so it never fabricates or mis-attributes columns.
   */
  @Nullable
  private Map<String, Map<String, EnumSet<QueryColumnUsageAnalyzer.ColumnUsage>>> extractColumnsByTable(Query<?> query)
  {
    try {
      Map<String, Map<String, EnumSet<QueryColumnUsageAnalyzer.ColumnUsage>>> result =
          QueryColumnUsageAnalyzer.analyze(query);
      return result.isEmpty() ? null : result;
    }
    // StackOverflowError (an Error, not an Exception) is caught too so that a pathologically deep
    // query plan degrades to table-level lineage rather than breaking the request-logging path.
    catch (Exception | StackOverflowError e) {
      log.debug(e, "Failed to extract column lineage; falling back to table-level lineage");
      return null;
    }
  }

  private ObjectNode buildRunEvent(
      String queryId,
      String queryType,
      RequestLogLine requestLogLine,
      List<String> inputs,
      @Nullable Map<String, Map<String, EnumSet<QueryColumnUsageAnalyzer.ColumnUsage>>> columnsByTable,
      @Nullable String output
  )
  {
    Map<String, Object> stats = requestLogLine.getQueryStats().getStats();
    boolean success = Boolean.TRUE.equals(stats.get("success"));

    ObjectNode event = jsonMapper.createObjectNode();
    event.put("eventType", success ? "COMPLETE" : "FAIL");
    event.put("eventTime", requestLogLine.getTimestamp().toInstant().toString());
    event.put("producer", PRODUCER);
    event.put("schemaURL", SCHEMA_URL);
    event.set("run", buildRun(queryId, queryType, requestLogLine, stats, success));
    event.set("job", buildJob(queryId, requestLogLine.getSql()));
    event.set("inputs", buildDatasets(inputs, columnsByTable));
    event.set("outputs", buildDatasets(output != null ? List.of(output) : List.of(), null));
    return event;
  }

  private ObjectNode buildRun(
      String queryId,
      String queryType,
      RequestLogLine requestLogLine,
      Map<String, Object> stats,
      boolean success
  )
  {
    ObjectNode run = jsonMapper.createObjectNode();
    run.put("runId", UUID.nameUUIDFromBytes(queryId.getBytes(StandardCharsets.UTF_8)).toString());

    ObjectNode facets = jsonMapper.createObjectNode();

    ObjectNode engineFacet = createFacet(ENGINE_FACET_SCHEMA_URL);
    engineFacet.put("name", "druid");
    engineFacet.put("version", getDruidVersion());
    facets.set("processing_engine", engineFacet);

    ObjectNode contextFacet = createFacet(CONTEXT_FACET_SCHEMA_URL);
    contextFacet.put("queryType", queryType);
    contextFacet.put("remoteAddress", requestLogLine.getRemoteAddr());
    Object identity = stats.get("identity");
    if (identity != null) {
      contextFacet.put("identity", identity.toString());
    }
    // For native sub-queries of SQL, include the parent SQL query ID for correlation.
    Object sqlQueryId = requestLogLine.getQuery() != null
        ? requestLogLine.getQuery().getContext().get(BaseQuery.SQL_QUERY_ID) : null;
    if (sqlQueryId != null) {
      contextFacet.put("sqlQueryId", sqlQueryId.toString());
    }
    facets.set("druid_query_context", contextFacet);

    ObjectNode statsFacet = createFacet(STATS_FACET_SCHEMA_URL);
    putLongStat(statsFacet, "durationMs", stats, "sqlQuery/time", "query/time");
    putLongStat(statsFacet, "bytes", stats, "sqlQuery/bytes", "query/bytes");
    putLongStat(statsFacet, "planningTimeMs", stats, "sqlQuery/planningTimeMs");
    Object statusCode = stats.get("statusCode");
    if (statusCode != null) {
      statsFacet.put("statusCode", statusCode.toString());
    }
    facets.set("druid_query_statistics", statsFacet);

    if (!success) {
      Object exception = stats.get("exception");
      if (exception != null) {
        ObjectNode errorFacet = createFacet(ERROR_FACET_SCHEMA_URL);
        errorFacet.put("message", exception.toString());
        if (sqlQueryId != null) {
          errorFacet.put("programmingLanguage", "SQL");
        }
        facets.set("errorMessage", errorFacet);
      }
    }

    run.set("facets", facets);
    return run;
  }

  private ObjectNode buildJob(String queryId, @Nullable String sql)
  {
    ObjectNode job = jsonMapper.createObjectNode();
    job.put("namespace", namespace);
    job.put("name", queryId);

    ObjectNode facets = jsonMapper.createObjectNode();

    ObjectNode jobTypeFacet = createFacet(JOB_TYPE_FACET_SCHEMA_URL);
    jobTypeFacet.put("processingType", "BATCH");
    jobTypeFacet.put("integration", "DRUID");
    jobTypeFacet.put("jobType", "QUERY");
    facets.set("jobType", jobTypeFacet);

    if (sql != null) {
      ObjectNode sqlFacet = createFacet(SQL_FACET_SCHEMA_URL);
      if (sql.length() > SQL_FACET_MAX_LENGTH) {
        log.warn(
            "SQL text for query [%s] exceeds [%,d] bytes and will be truncated in the sql job facet",
            queryId,
            SQL_FACET_MAX_LENGTH
        );
        sqlFacet.put("query", sql.substring(0, SQL_FACET_MAX_LENGTH));
      } else {
        sqlFacet.put("query", sql);
      }
      facets.set("sql", sqlFacet);
    }

    job.set("facets", facets);
    return job;
  }

  private ObjectNode createFacet(@Nullable String schemaUrl)
  {
    ObjectNode facet = jsonMapper.createObjectNode();
    facet.put("_producer", PRODUCER);
    if (schemaUrl != null) {
      facet.put("_schemaURL", schemaUrl);
    }
    return facet;
  }

  private ArrayNode buildDatasets(
      List<String> tableNames,
      @Nullable Map<String, Map<String, EnumSet<QueryColumnUsageAnalyzer.ColumnUsage>>> columnsByTable
  )
  {
    ArrayNode array = jsonMapper.createArrayNode();
    for (String name : tableNames) {
      ObjectNode node = jsonMapper.createObjectNode();
      node.put("namespace", namespace);
      node.put("name", name);
      ObjectNode facets = jsonMapper.createObjectNode();
      Map<String, EnumSet<QueryColumnUsageAnalyzer.ColumnUsage>> columns = columnsByTable == null ? null : columnsByTable.get(name);
      if (columns != null && !columns.isEmpty()) {
        addColumnFacets(facets, columns);
      }
      node.set("facets", facets);
      array.add(node);
    }
    return array;
  }

  /**
   * Attaches the standard OpenLineage {@code schema} facet (referenced column names, sorted) and the
   * custom {@code druid_columnUsage} facet (column to usage roles) to an input dataset's facets.
   */
  private void addColumnFacets(ObjectNode facets, Map<String, EnumSet<QueryColumnUsageAnalyzer.ColumnUsage>> columns)
  {
    ObjectNode schemaFacet = createFacet(SCHEMA_FACET_SCHEMA_URL);
    ArrayNode fields = jsonMapper.createArrayNode();
    ObjectNode usageFields = jsonMapper.createObjectNode();
    // columns is a TreeMap, so iteration (and the emitted JSON) is deterministically sorted by name.
    for (Map.Entry<String, EnumSet<QueryColumnUsageAnalyzer.ColumnUsage>> entry : columns.entrySet()) {
      ObjectNode field = jsonMapper.createObjectNode();
      field.put("name", entry.getKey());
      fields.add(field);

      ArrayNode usages = jsonMapper.createArrayNode();
      // EnumSet iterates in enum declaration order, so usage lists are deterministic too.
      for (QueryColumnUsageAnalyzer.ColumnUsage role : entry.getValue()) {
        usages.add(role.name());
      }
      ObjectNode usageEntry = jsonMapper.createObjectNode();
      usageEntry.set("usages", usages);
      usageFields.set(entry.getKey(), usageEntry);
    }
    schemaFacet.set("fields", fields);
    facets.set("schema", schemaFacet);

    ObjectNode usageFacet = createFacet(COLUMN_USAGE_FACET_SCHEMA_URL);
    usageFacet.set("fields", usageFields);
    facets.set("druid_columnUsage", usageFacet);
  }

  protected void emit(ObjectNode event)
  {
    try {
      String json = jsonMapper.writeValueAsString(event);
      if (transportType == OpenLineageRequestLoggerProvider.TransportType.HTTP) {
        emitExecutor.submit(() -> emitHttp(json));
      } else {
        log.debug("OpenLineage event: %s", json);
      }
    }
    catch (IOException e) {
      log.error(e, "Failed to serialize OpenLineage event");
    }
  }

  private void emitHttp(String json)
  {
    IOException lastException = null;
    int lastStatusCode = -1;

    for (int attempt = 0; attempt <= MAX_SEND_RETRIES; attempt++) {
      if (attempt > 0) {
        try {
          Thread.sleep(RETRY_SLEEP_MS);
        }
        catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          log.warn("OpenLineage HTTP emit interrupted on retry [%d]; dropping event", attempt);
          return;
        }
      }

      HttpPost post = new HttpPost(transportUrl);
      post.setEntity(new StringEntity(json, ContentType.APPLICATION_JSON));
      try {
        org.apache.http.HttpResponse response = httpClient.execute(post);
        lastStatusCode = response.getStatusLine().getStatusCode();
        EntityUtils.consumeQuietly(response.getEntity());
        if (lastStatusCode >= 200 && lastStatusCode < 300) {
          return; // success
        }
        // Non-2xx: retry (server-side error may be transient)
        log.debug(
            "OpenLineage HTTP attempt [%d/%d] received non-2xx [%d] from [%s]",
            attempt + 1,
            MAX_SEND_RETRIES + 1,
            lastStatusCode,
            transportUrl
        );
        lastException = null;
      }
      catch (IOException e) {
        lastException = e;
        log.debug(
            e,
            "OpenLineage HTTP attempt [%d/%d] failed posting to [%s]",
            attempt + 1,
            MAX_SEND_RETRIES + 1,
            transportUrl
        );
      }
      finally {
        post.releaseConnection();
      }
    }

    // All attempts exhausted — delivery guarantee is at-most-once.
    if (lastException != null) {
      log.warn(
          lastException,
          "OpenLineage event dropped: all [%d] attempts to POST to [%s] failed with an exception",
          MAX_SEND_RETRIES + 1,
          transportUrl
      );
    } else {
      log.warn(
          "OpenLineage event dropped: all [%d] attempts to POST to [%s] returned non-2xx status [%d]",
          MAX_SEND_RETRIES + 1,
          transportUrl,
          lastStatusCode
      );
    }
  }

  private void putLongStat(ObjectNode node, String targetKey, Map<String, Object> stats, String... sourceKeys)
  {
    for (String key : sourceKeys) {
      Object val = stats.get(key);
      if (val instanceof Number) {
        node.put(targetKey, ((Number) val).longValue());
        return;
      }
    }
  }

  private static String getDruidVersion()
  {
    String v = OpenLineageRequestLogger.class.getPackage().getImplementationVersion();
    return v != null ? v : "unknown";
  }

  /**
   * Rejection handler that discards events when the emit queue is full, but logs a warning
   * on the first drop and every {@link #DISCARD_WARNING_INTERVAL} drops thereafter.
   */
  private static class DiscardWithWarningPolicy implements RejectedExecutionHandler
  {
    private final AtomicLong discardedCount;

    DiscardWithWarningPolicy(AtomicLong discardedCount)
    {
      this.discardedCount = discardedCount;
    }

    @Override
    public void rejectedExecution(Runnable r, ThreadPoolExecutor executor)
    {
      long count = discardedCount.incrementAndGet();
      if (count == 1 || count % DISCARD_WARNING_INTERVAL == 0) {
        log.warn("OpenLineage emit queue full, discarded [%,d] events total", count);
      }
    }
  }
}
