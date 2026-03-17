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
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOrderBy;
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
import org.apache.http.impl.client.HttpClientBuilder;
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
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

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

  private static final int EMIT_QUEUE_CAPACITY = 1000;
  private static final int EMIT_THREAD_COUNT = 1;

  /**
   * Internal broker query types that are automatically issued for schema discovery and metadata
   * caching. These are not user-initiated and produce noisy, low-value lineage events.
   */
  private static final Set<String> INTERNAL_QUERY_TYPES = Set.of(
      "segmentMetadata",
      "dataSourceMetadata",
      "timeBoundary"
  );

  private final ObjectMapper jsonMapper;
  private final String namespace;
  private final OpenLineageRequestLoggerProvider.TransportType transportType;
  @Nullable
  private final String transportUrl;
  @Nullable
  private final HttpClient httpClient;
  @Nullable
  private final ExecutorService emitExecutor;

  public OpenLineageRequestLogger(
      ObjectMapper jsonMapper,
      String namespace,
      OpenLineageRequestLoggerProvider.TransportType transportType,
      @Nullable String transportUrl
  )
  {
    this.jsonMapper = jsonMapper;
    this.namespace = namespace;
    this.transportType = transportType;
    this.transportUrl = transportUrl;
    if (transportType == OpenLineageRequestLoggerProvider.TransportType.HTTP) {
      this.httpClient = HttpClientBuilder.create().build();
      // Bounded queue: if the queue is full, silently drop the event rather than blocking
      // the query thread. Uses Druid's Execs for daemon thread naming conventions.
      this.emitExecutor = new ThreadPoolExecutor(
          EMIT_THREAD_COUNT,
          EMIT_THREAD_COUNT,
          60L,
          TimeUnit.SECONDS,
          new ArrayBlockingQueue<>(EMIT_QUEUE_CAPACITY),
          Execs.makeThreadFactory("OpenLineageEmitter-%d"),
          new ThreadPoolExecutor.DiscardPolicy()
      );
    } else {
      this.httpClient = null;
      this.emitExecutor = null;
    }
  }

  @LifecycleStart
  @Override
  public void start() throws Exception
  {
    if (transportType == OpenLineageRequestLoggerProvider.TransportType.HTTP && transportUrl == null) {
      throw new IllegalStateException(
          "druid.request.logging.transportUrl must be set when transportType=HTTP"
      );
    }
    if (transportType == OpenLineageRequestLoggerProvider.TransportType.HTTP) {
      log.info("Started OpenLineage HTTP transport to [%s]", transportUrl);
    } else {
      log.info("Started OpenLineage console transport");
    }
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

    if (INTERNAL_QUERY_TYPES.contains(queryType)) {
      return;
    }

    List<String> inputs = new ArrayList<>(new LinkedHashSet<>(requestLogLine.getQuery().getDataSource().getTableNames()));
    String queryId = requestLogLine.getQuery().getId();
    if (queryId == null) {
      queryId = UUID.randomUUID().toString();
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
        // Druid-specific SQL extensions (REPLACE, EXTERN, etc.) may not parse with the standard
        // Calcite parser. Emit the event without table-level lineage rather than failing.
        log.debug(
            "OpenLineage: could not parse SQL for lineage extraction (query will still be emitted): %s",
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
    Object nativeQueryIds = requestLogLine.getSqlQueryContext().get("nativeQueryIds");
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
    if (root instanceof SqlWith) {
      SqlWith with = (SqlWith) root;
      Set<String> cteNames = new HashSet<>();
      for (SqlNode item : with.withList) {
        if (item instanceof SqlWithItem) {
          cteNames.add(((SqlWithItem) item).name.getSimple());
          collectFromClause(((SqlWithItem) item).query, tables, cteNames);
        }
      }
      collectFromClause(with.body, tables, cteNames);
    } else if (root instanceof SqlInsert) {
      collectFromClause(((SqlInsert) root).getSource(), tables, Set.of());
    } else {
      collectFromClause(root, tables, Set.of());
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

  private void collectFromClause(SqlNode from, List<String> tables, Set<String> excludeNames)
  {
    if (from == null) {
      return;
    }
    if (from instanceof SqlIdentifier) {
      String name = String.join(".", ((SqlIdentifier) from).names);
      if (!excludeNames.contains(name)) {
        tables.add(name);
      }
    } else if (from instanceof SqlJoin) {
      SqlJoin join = (SqlJoin) from;
      collectFromClause(join.getLeft(), tables, excludeNames);
      collectFromClause(join.getRight(), tables, excludeNames);
    } else if (from instanceof SqlSelect) {
      collectFromClause(((SqlSelect) from).getFrom(), tables, excludeNames);
    } else if (from instanceof SqlWith) {
      SqlWith with = (SqlWith) from;
      Set<String> innerExcludes = new HashSet<>(excludeNames);
      for (SqlNode item : with.withList) {
        if (item instanceof SqlWithItem) {
          innerExcludes.add(((SqlWithItem) item).name.getSimple());
          collectFromClause(((SqlWithItem) item).query, tables, innerExcludes);
        }
      }
      collectFromClause(with.body, tables, innerExcludes);
    } else if (from instanceof SqlOrderBy) {
      collectFromClause(((SqlOrderBy) from).query, tables, excludeNames);
    } else if (from instanceof SqlBasicCall && from.getKind() == SqlKind.AS) {
      collectFromClause(((SqlBasicCall) from).operand(0), tables, excludeNames);
    }
  }

  protected void emit(ObjectNode event)
  {
    try {
      String json = jsonMapper.writeValueAsString(event);
      if (transportType == OpenLineageRequestLoggerProvider.TransportType.HTTP) {
        emitExecutor.submit(() -> emitHttp(json));
      } else {
        log.info("OpenLineage event: %s", json);
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
    Object id = requestLogLine.getSqlQueryContext().get(BaseQuery.SQL_QUERY_ID);
    if (id != null) {
      return id.toString();
    }
    return UUID.randomUUID().toString();
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

}
