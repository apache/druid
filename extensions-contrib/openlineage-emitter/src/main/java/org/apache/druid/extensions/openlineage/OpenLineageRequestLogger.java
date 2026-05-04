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
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlWith;
import org.apache.calcite.sql.SqlWithItem;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.lifecycle.LifecycleStart;
import org.apache.druid.java.util.common.lifecycle.LifecycleStop;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.BaseQuery;
import org.apache.druid.server.RequestLogLine;
import org.apache.druid.server.log.RequestLogger;
import org.apache.druid.utils.CloseableUtils;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.util.EntityUtils;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * OpenLineage RunEvents for completed Druid queries.
 */
public class OpenLineageRequestLogger implements RequestLogger
{
  private static final Logger log = new Logger(OpenLineageRequestLogger.class);

  private static final String PRODUCER =
      "https://github.com/apache/druid/extensions-contrib/openlineage-emitter";
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

  // Standard Calcite parser (not Druid's custom parser): sufficient for table name extraction.
  // Druid-specific syntax (REPLACE, EXTERN, etc.) falls through to the SqlParseException handler.
  private static final SqlParser.Config SQL_PARSER_CONFIG = SqlParser
      .config()
      .withCaseSensitive(true)
      .withUnquotedCasing(Casing.UNCHANGED)
      .withQuotedCasing(Casing.UNCHANGED)
      .withQuoting(Quoting.DOUBLE_QUOTE);

  // Matches INSERT INTO or REPLACE INTO when the standard Calcite parser fails (e.g. EXTERN,
  // PARTITIONED BY). Captures the output table name; handles quoted and unquoted identifiers.
  private static final Pattern WRITE_INTO_PATTERN =
      Pattern.compile("(?i)^\\s*(?:INSERT|REPLACE)\\s+INTO\\s+\"?([^\"\\s]+)\"?");
  // Extracts the SELECT subquery from INSERT INTO or REPLACE INTO for input lineage.
  // Stops before PARTITIONED BY / CLUSTERED BY which are Druid-specific clauses.
  private static final Pattern SELECT_FROM_WRITE_PATTERN =
      Pattern.compile("(?is)\\b(SELECT\\s+.+?)(?:\\s+PARTITIONED\\s+BY|\\s+CLUSTERED\\s+BY|$)");

  static final int DEFAULT_EMIT_QUEUE_CAPACITY = 1000;
  static final int DEFAULT_EMIT_THREAD_COUNT = 1;
  private static final int DISCARD_WARNING_INTERVAL = 1000;

  static final String UNKNOWN_QUERY_ID = "unknown-query-id";

  private final ObjectMapper jsonMapper;
  private final String namespace;
  private final OpenLineageRequestLoggerProvider.TransportType transportType;
  @Nullable
  private final String transportUrl;
  private final Set<String> excludedNativeQueryTypes;
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
         DEFAULT_EMIT_QUEUE_CAPACITY, DEFAULT_EMIT_THREAD_COUNT, null);
  }

  public OpenLineageRequestLogger(
      ObjectMapper jsonMapper,
      String namespace,
      OpenLineageRequestLoggerProvider.TransportType transportType,
      @Nullable String transportUrl,
      Set<String> excludedNativeQueryTypes,
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
    if (transportType == OpenLineageRequestLoggerProvider.TransportType.HTTP && transportUrl == null) {
      throw new IllegalStateException(
          "druid.request.logging.transportUrl must be set when transportType=HTTP"
      );
    }
    if (transportType == OpenLineageRequestLoggerProvider.TransportType.HTTP) {
      this.httpClient = httpClient;
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

    // Skip native sub-queries of a SQL execution to avoid duplicating the SQL-level event.
    if (requestLogLine.getQuery().getContext().get(BaseQuery.SQL_QUERY_ID) != null) {
      return;
    }

    String queryType = requestLogLine.getQuery().getType();

    if (excludedNativeQueryTypes.contains(queryType)) {
      return;
    }

    List<String> inputs = new ArrayList<>(new LinkedHashSet<>(requestLogLine.getQuery().getDataSource().getTableNames()));
    String queryId = requestLogLine.getQuery().getId();
    if (queryId == null) {
      log.debug("Native query reached OpenLineage logger without a query ID");
      queryId = UNKNOWN_QUERY_ID;
    }

    emit(buildRunEvent(queryId, null, queryType, requestLogLine, inputs, null));
  }

  @Override
  public void logSqlQuery(RequestLogLine requestLogLine) throws IOException
  {
    String sql = requestLogLine.getSql();
    List<String> inputs = new ArrayList<>();
    String output = null;

    if (sql != null) {
      try {
        SqlNode parsed = SqlParser.create(sql, SQL_PARSER_CONFIG).parseQuery();
        inputs = extractInputs(parsed);
        output = extractOutput(parsed);
      }
      catch (SqlParseException e) {
        // Druid-specific SQL extensions (INSERT/REPLACE INTO with PARTITIONED BY, EXTERN, etc.)
        // may not parse with the standard Calcite parser. Attempt to extract lineage via regex;
        // for other unparseable statements, emit the event without table-level lineage.
        Matcher writeMatcher = WRITE_INTO_PATTERN.matcher(sql);
        if (writeMatcher.find()) {
          output = writeMatcher.group(1);
          // Also try to extract inputs from the SELECT subquery. EXTERN-sourced inputs will
          // fail to re-parse and fall through with empty inputs, which is correct.
          Matcher selectMatcher = SELECT_FROM_WRITE_PATTERN.matcher(sql);
          if (selectMatcher.find()) {
            try {
              SqlNode selectNode = SqlParser.create(selectMatcher.group(1), SQL_PARSER_CONFIG).parseQuery();
              inputs = extractInputs(selectNode);
            }
            catch (SqlParseException ignored) {
              // EXTERN or other non-standard sources — inputs left empty
            }
          }
        }
        log.debug(
            "OpenLineage: could not parse SQL with standard Calcite parser (query will still be emitted): %s",
            e.getMessage()
        );
      }
    }

    String queryId = extractSqlQueryId(requestLogLine);
    emit(buildRunEvent(queryId, sql, "sql", requestLogLine, new ArrayList<>(new LinkedHashSet<>(inputs)), output));
  }

  private ObjectNode buildRunEvent(
      String queryId,
      @Nullable String sql,
      String queryType,
      RequestLogLine requestLogLine,
      List<String> inputs,
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
    event.set("job", buildJob(queryId, sql));
    event.set("inputs", buildDatasets(inputs));
    event.set("outputs", buildDatasets(output != null ? List.of(output) : List.of()));
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

    ObjectNode contextFacet = createFacet(null);
    contextFacet.put("queryType", queryType);
    contextFacet.put("remoteAddress", requestLogLine.getRemoteAddr());
    Object identity = stats.get("identity");
    if (identity != null) {
      contextFacet.put("identity", identity.toString());
    }
    Map<String, Object> sqlQueryContext = requestLogLine.getSqlQueryContext();
    Object nativeQueryIds = sqlQueryContext != null ? sqlQueryContext.get("nativeQueryIds") : null;
    if (nativeQueryIds != null) {
      contextFacet.put("nativeQueryIds", nativeQueryIds.toString());
    }
    facets.set("druid_query_context", contextFacet);

    ObjectNode statsFacet = createFacet(null);
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
        if ("sql".equals(queryType)) {
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
      sqlFacet.put("query", sql);
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

  private ArrayNode buildDatasets(List<String> tableNames)
  {
    ArrayNode array = jsonMapper.createArrayNode();
    for (String name : tableNames) {
      ObjectNode node = jsonMapper.createObjectNode();
      node.put("namespace", namespace);
      node.put("name", name);
      node.set("facets", jsonMapper.createObjectNode());
      array.add(node);
    }
    return array;
  }

  private List<String> extractInputs(SqlNode root)
  {
    List<String> tables = new ArrayList<>();
    if (root instanceof SqlInsert) {
      // For INSERT/REPLACE, only walk the source query — the target table is an output, not an input.
      collectTableNames(((SqlInsert) root).getSource(), tables, Set.of());
    } else {
      collectTableNames(root, tables, Set.of());
    }
    return tables;
  }

  @Nullable
  private String extractOutput(SqlNode root)
  {
    if (root instanceof SqlInsert) {
      SqlNode target = ((SqlInsert) root).getTargetTable();
      if (target instanceof SqlIdentifier) {
        return String.join(".", ((SqlIdentifier) target).names);
      }
    }
    return null;
  }

  /**
   * Walks the SQL tree to collect table references from FROM clauses, JOINs, subqueries in
   * WHERE/HAVING/SELECT, and set operations (UNION/INTERSECT/EXCEPT). CTE alias names are
   * tracked in {@code excludeNames} to avoid reporting them as table inputs.
   *
   * <p>The walker distinguishes table-reference positions (FROM, JOIN) from column-reference
   * positions (SELECT list, WHERE expressions) by collecting {@link SqlIdentifier} nodes only
   * from known table-reference contexts, while recursing into all subqueries to find nested
   * FROM clauses.
   */
  private void collectTableNames(SqlNode node, List<String> tables, Set<String> excludeNames)
  {
    if (node == null) {
      return;
    }
    if (node instanceof SqlWith) {
      SqlWith with = (SqlWith) node;
      Set<String> innerExcludes = new HashSet<>(excludeNames);
      for (SqlNode item : with.withList) {
        if (item instanceof SqlWithItem) {
          innerExcludes.add(((SqlWithItem) item).name.getSimple());
          collectTableNames(((SqlWithItem) item).query, tables, innerExcludes);
        }
      }
      collectTableNames(with.body, tables, innerExcludes);
    } else if (node instanceof SqlNodeList) {
      // Lists (e.g., SELECT list items): recurse into each element to find subqueries.
      for (SqlNode item : (SqlNodeList) node) {
        collectTableNames(item, tables, excludeNames);
      }
    } else if (node instanceof SqlSelect) {
      SqlSelect select = (SqlSelect) node;
      // FROM clause: identifiers here are table references.
      collectFromPosition(select.getFrom(), tables, excludeNames);
      // Recurse into WHERE, HAVING, and SELECT list to find nested subqueries.
      collectTableNames(select.getWhere(), tables, excludeNames);
      collectTableNames(select.getHaving(), tables, excludeNames);
      collectTableNames(select.getSelectList(), tables, excludeNames);
    } else if (node instanceof SqlCall) {
      SqlCall call = (SqlCall) node;
      if (call.getKind() == SqlKind.UNION || call.getKind() == SqlKind.INTERSECT || call.getKind() == SqlKind.EXCEPT) {
        // Set operations: each operand is a SELECT whose FROM clause has table references.
        for (SqlNode operand : call.getOperandList()) {
          collectTableNames(operand, tables, excludeNames);
        }
      } else {
        // Other expressions (AND, OR, IN, =, function calls, etc.): recurse to find subqueries.
        for (SqlNode operand : call.getOperandList()) {
          if (operand instanceof SqlSelect || operand instanceof SqlWith || operand instanceof SqlCall) {
            collectTableNames(operand, tables, excludeNames);
          }
        }
      }
    }
  }

  /**
   * Collects table names from a FROM-clause position, where {@link SqlIdentifier} nodes
   * represent table references (not column references).
   */
  private void collectFromPosition(SqlNode node, List<String> tables, Set<String> excludeNames)
  {
    if (node == null) {
      return;
    }
    if (node instanceof SqlIdentifier) {
      String name = String.join(".", ((SqlIdentifier) node).names);
      if (!excludeNames.contains(name)) {
        tables.add(name);
      }
    } else if (node instanceof SqlJoin) {
      SqlJoin join = (SqlJoin) node;
      collectFromPosition(join.getLeft(), tables, excludeNames);
      collectFromPosition(join.getRight(), tables, excludeNames);
    } else if (node instanceof SqlSelect) {
      // Subquery in FROM: recurse into the full select.
      collectTableNames(node, tables, excludeNames);
    } else if (node instanceof SqlBasicCall && node.getKind() == SqlKind.AS) {
      // Alias: the table reference is the first operand.
      collectFromPosition(((SqlBasicCall) node).operand(0), tables, excludeNames);
    } else if (node instanceof SqlCall && node.getKind() == SqlKind.LATERAL) {
      // LATERAL (subquery): the single operand is a SELECT whose tables we should collect.
      // Other SqlCalls in FROM position (UNNEST, TABLE, etc.) are not table references —
      // their operands are column expressions, not datasource names.
      for (SqlNode operand : ((SqlCall) node).getOperandList()) {
        collectFromPosition(operand, tables, excludeNames);
      }
    }
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
    HttpPost post = new HttpPost(transportUrl);
    post.setEntity(new StringEntity(json, ContentType.APPLICATION_JSON));
    try {
      org.apache.http.HttpResponse response = httpClient.execute(post);
      int statusCode = response.getStatusLine().getStatusCode();
      if (statusCode < 200 || statusCode >= 300) {
        log.warn(
            "OpenLineage HTTP transport received non-2xx response [%d] from [%s]; event may have been dropped",
            statusCode,
            transportUrl
        );
      }
      EntityUtils.consumeQuietly(response.getEntity());
    }
    catch (IOException e) {
      log.error(e, "Failed to POST OpenLineage event to [%s]", transportUrl);
    }
    finally {
      post.releaseConnection();
    }
  }

  private String extractSqlQueryId(RequestLogLine requestLogLine)
  {
    Map<String, Object> sqlQueryContext = requestLogLine.getSqlQueryContext();
    Object id = sqlQueryContext != null ? sqlQueryContext.get(BaseQuery.SQL_QUERY_ID) : null;
    if (id != null) {
      return id.toString();
    }
    log.debug("SQL query reached OpenLineage logger without a query ID");
    return UNKNOWN_QUERY_ID;
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
