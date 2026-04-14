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

import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.query.BaseQuery;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.Query;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.query.spec.QuerySegmentSpec;
import org.apache.druid.server.QueryStats;
import org.apache.druid.server.RequestLogLine;
import org.joda.time.DateTime;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;


public class OpenLineageRequestLoggerTest
{
  private static final ObjectMapper MAPPER = new DefaultObjectMapper();
  private static final String NAMESPACE = "druid://test:8082";
  private static final DateTime TIMESTAMP = DateTimes.of("2024-01-01T00:00:00Z");
  private static final String REMOTE_ADDR = "10.0.0.1";
  private static final Set<String> DEFAULT_EXCLUDED_NATIVE_QUERY_TYPES = Set.of(
      "segmentMetadata",
      "dataSourceMetadata",
      "timeBoundary"
  );

  private List<ObjectNode> capturedEvents;
  private OpenLineageRequestLogger logger;

  @BeforeEach
  public void setUp()
  {
    capturedEvents = new ArrayList<>();
    logger = createLogger(DEFAULT_EXCLUDED_NATIVE_QUERY_TYPES);
  }

  private OpenLineageRequestLogger createLogger(Set<String> excludedNativeQueryTypes)
  {
    return new OpenLineageRequestLogger(
        MAPPER,
        NAMESPACE,
        OpenLineageRequestLoggerProvider.TransportType.CONSOLE,
        null,
        excludedNativeQueryTypes
    )
    {
      @Override
      protected void emit(ObjectNode event)
      {
        capturedEvents.add(event);
      }
    };
  }

  @Test
  public void testSqlSimpleSelect() throws IOException
  {
    logger.logSqlQuery(sqlLine(
        "SELECT * FROM \"kttm\"",
        ImmutableMap.of("sqlQueryId", "qid-1"),
        ImmutableMap.of("success", true)
    ));

    Assertions.assertEquals(1, capturedEvents.size());
    ObjectNode event = capturedEvents.get(0);

    Assertions.assertEquals("COMPLETE", event.get("eventType").asText());
    Assertions.assertEquals(NAMESPACE, event.get("job").get("namespace").asText());

    JsonNode inputs = event.get("inputs");
    Assertions.assertEquals(1, inputs.size());
    Assertions.assertEquals("kttm", inputs.get(0).get("name").asText());
    Assertions.assertEquals(NAMESPACE, inputs.get(0).get("namespace").asText());

    Assertions.assertEquals(0, event.get("outputs").size());
  }

  @Test
  public void testSqlJoin() throws IOException
  {
    logger.logSqlQuery(sqlLine(
        "SELECT a.\"x\" FROM \"tableA\" AS a JOIN \"tableB\" AS b ON a.\"id\" = b.\"id\"",
        ImmutableMap.of("sqlQueryId", "qid-2"),
        ImmutableMap.of("success", true)
    ));

    Assertions.assertEquals(1, capturedEvents.size());
    Set<String> names = inputNames(capturedEvents.get(0));
    Assertions.assertEquals(2, names.size());
    Assertions.assertTrue(names.contains("tableA"));
    Assertions.assertTrue(names.contains("tableB"));
  }

  @Test
  public void testSqlCteExcludesCteNames() throws IOException
  {
    logger.logSqlQuery(sqlLine(
        "WITH \"cte\" AS (SELECT * FROM \"realTable\") SELECT * FROM \"cte\"",
        ImmutableMap.of("sqlQueryId", "qid-3"),
        ImmutableMap.of("success", true)
    ));

    Assertions.assertEquals(1, capturedEvents.size());
    Set<String> names = inputNames(capturedEvents.get(0));
    Assertions.assertTrue(names.contains("realTable"));
    Assertions.assertFalse(names.contains("cte"));
  }

  @Test
  public void testSqlInsertExtractsInputAndOutput() throws IOException
  {
    logger.logSqlQuery(sqlLine(
        "INSERT INTO \"outputTable\" SELECT * FROM \"inputTable\"",
        ImmutableMap.of("sqlQueryId", "qid-4"),
        ImmutableMap.of("success", true)
    ));

    Assertions.assertEquals(1, capturedEvents.size());
    ObjectNode event = capturedEvents.get(0);

    Assertions.assertTrue(inputNames(event).contains("inputTable"));

    JsonNode outputs = event.get("outputs");
    Assertions.assertEquals(1, outputs.size());
    Assertions.assertEquals("outputTable", outputs.get(0).get("name").asText());
  }

  @Test
  public void testSqlParseFailureStillEmitsEvent() throws IOException
  {
    // Druid-specific syntax (REPLACE INTO) doesn't parse with the standard Calcite parser;
    // the event is still emitted, just without table-level lineage.
    logger.logSqlQuery(sqlLine(
        "REPLACE INTO \"ds\" OVERWRITE ALL SELECT * FROM \"src\" PARTITIONED BY ALL",
        ImmutableMap.of("sqlQueryId", "qid-5"),
        ImmutableMap.of("success", true)
    ));

    Assertions.assertEquals(1, capturedEvents.size());
    Assertions.assertEquals(0, capturedEvents.get(0).get("inputs").size());
    Assertions.assertEquals(0, capturedEvents.get(0).get("outputs").size());
    Assertions.assertEquals("COMPLETE", capturedEvents.get(0).get("eventType").asText());
  }

  @Test
  public void testSqlQueryFacets() throws IOException
  {
    String sql = "SELECT * FROM \"t\"";
    logger.logSqlQuery(sqlLine(
        sql,
        ImmutableMap.of("sqlQueryId", "qid-facets", "nativeQueryIds", "[native-1, native-2]"),
        ImmutableMap.of(
            "success", true,
            "sqlQuery/time", 300L,
            "sqlQuery/bytes", 2048L,
            "sqlQuery/planningTimeMs", 25L,
            "statusCode", "200",
            "identity", "bob"
        )
    ));

    Assertions.assertEquals(1, capturedEvents.size());
    ObjectNode event = capturedEvents.get(0);

    // OpenLineage envelope
    Assertions.assertNotNull(event.get("schemaURL"));
    Assertions.assertNotNull(event.get("producer"));
    Assertions.assertEquals("2024-01-01T00:00:00.000Z", event.get("eventTime").asText());

    // Job facets
    JsonNode jobFacets = event.get("job").get("facets");
    Assertions.assertEquals(sql, jobFacets.get("sql").get("query").asText());
    Assertions.assertEquals("BATCH", jobFacets.get("jobType").get("processingType").asText());
    Assertions.assertEquals("DRUID", jobFacets.get("jobType").get("integration").asText());
    Assertions.assertEquals("QUERY", jobFacets.get("jobType").get("jobType").asText());

    // Run facets
    JsonNode runFacets = event.get("run").get("facets");
    Assertions.assertEquals("druid", runFacets.get("processing_engine").get("name").asText());
    Assertions.assertNotNull(runFacets.get("processing_engine").get("version"));

    // Query context facet
    JsonNode ctx = runFacets.get("druid_query_context");
    Assertions.assertEquals("bob", ctx.get("identity").asText());
    Assertions.assertEquals(REMOTE_ADDR, ctx.get("remoteAddress").asText());
    Assertions.assertEquals("sql", ctx.get("queryType").asText());
    Assertions.assertEquals("[native-1, native-2]", ctx.get("nativeQueryIds").asText());

    // Statistics facet
    JsonNode stats = runFacets.get("druid_query_statistics");
    Assertions.assertEquals(300L, stats.get("durationMs").asLong());
    Assertions.assertEquals(2048L, stats.get("bytes").asLong());
    Assertions.assertEquals(25L, stats.get("planningTimeMs").asLong());
    Assertions.assertEquals("200", stats.get("statusCode").asText());

    // No error facet on success
    Assertions.assertNull(runFacets.get("errorMessage"));
  }

  @Test
  public void testSqlQueryFailure() throws IOException
  {
    logger.logSqlQuery(sqlLine(
        "SELECT * FROM \"t\"",
        ImmutableMap.of("sqlQueryId", "qid-fail"),
        ImmutableMap.of("success", false, "exception", "Query timed out")
    ));

    Assertions.assertEquals(1, capturedEvents.size());
    ObjectNode event = capturedEvents.get(0);

    Assertions.assertEquals("FAIL", event.get("eventType").asText());

    JsonNode errorFacet = event.get("run").get("facets").get("errorMessage");
    Assertions.assertNotNull(errorFacet);
    Assertions.assertEquals("Query timed out", errorFacet.get("message").asText());
    Assertions.assertEquals("SQL", errorFacet.get("programmingLanguage").asText());
  }

  @Test
  public void testNativeQuery() throws IOException
  {
    TestQuery query = new TestQuery(
        new TableDataSource("myDatasource"),
        ImmutableMap.of("queryId", "native-qid-1")
    );
    logger.logNativeQuery(nativeLine(query, ImmutableMap.of("success", true, "query/time", 50L, "query/bytes", 512L)));

    Assertions.assertEquals(1, capturedEvents.size());
    ObjectNode event = capturedEvents.get(0);

    Assertions.assertEquals("COMPLETE", event.get("eventType").asText());
    Assertions.assertTrue(inputNames(event).contains("myDatasource"));

    // Native queries use "query/time" and "query/bytes" stat keys (not "sqlQuery/*")
    JsonNode stats = event.get("run").get("facets").get("druid_query_statistics");
    Assertions.assertEquals(50L, stats.get("durationMs").asLong());
    Assertions.assertEquals(512L, stats.get("bytes").asLong());

    // No sql facet on native queries
    Assertions.assertNull(event.get("job").get("facets").get("sql"));

    // Query type reflects the native query type
    Assertions.assertEquals("test", event.get("run").get("facets").get("druid_query_context").get("queryType").asText());
  }

  @Test
  public void testNativeSubQueryOfSqlIsSkipped() throws IOException
  {
    // Queries with sqlQueryId in context are sub-queries of a SQL execution; skip to avoid
    // duplicate events since the SQL-level event already captures the lineage.
    TestQuery query = new TestQuery(
        new TableDataSource("myDatasource"),
        ImmutableMap.of("queryId", "native-qid-2", BaseQuery.SQL_QUERY_ID, "parent-sql-id")
    );
    logger.logNativeQuery(nativeLine(query, ImmutableMap.of("success", true)));

    Assertions.assertEquals(0, capturedEvents.size());
  }

  @Test
  public void testNativeQueryWithNullQueryDoesNotCrash() throws IOException
  {
    RequestLogLine logLine = RequestLogLine.forNative(
        null,
        TIMESTAMP,
        REMOTE_ADDR,
        new QueryStats(ImmutableMap.of())
    );

    logger.logNativeQuery(logLine);

    Assertions.assertEquals(0, capturedEvents.size());
  }

  @Test
  public void testStartThrowsWhenHttpWithoutUrl()
  {
    OpenLineageRequestLogger httpLogger = new OpenLineageRequestLogger(
        MAPPER,
        NAMESPACE,
        OpenLineageRequestLoggerProvider.TransportType.HTTP,
        null,
        DEFAULT_EXCLUDED_NATIVE_QUERY_TYPES
    );
    Assertions.assertThrows(IllegalStateException.class, httpLogger::start);
  }

  private static RequestLogLine sqlLine(String sql, Map<String, Object> context, Map<String, Object> stats)
  {
    return RequestLogLine.forSql(sql, context, TIMESTAMP, REMOTE_ADDR, new QueryStats(stats));
  }

  private static RequestLogLine nativeLine(Query<?> query, Map<String, Object> stats)
  {
    return RequestLogLine.forNative(query, TIMESTAMP, REMOTE_ADDR, new QueryStats(stats));
  }

  private static Set<String> inputNames(JsonNode event)
  {
    Set<String> names = new HashSet<>();
    for (JsonNode input : event.get("inputs")) {
      names.add(input.get("name").asText());
    }
    return names;
  }
}

@JsonTypeName("test")
class TestQuery extends BaseQuery<Object>
{
  private static final QuerySegmentSpec DUMMY_SPEC = new MultipleIntervalSegmentSpec(
      Collections.singletonList(Intervals.of("2024-01-01/2024-01-02"))
  );

  TestQuery(DataSource dataSource, Map<String, Object> context)
  {
    super(dataSource, DUMMY_SPEC, context);
  }

  @Override
  public boolean hasFilters()
  {
    return false;
  }

  @Override
  public DimFilter getFilter()
  {
    return null;
  }

  @Override
  public String getType()
  {
    return "test";
  }

  @Override
  public Query<Object> withQuerySegmentSpec(QuerySegmentSpec spec)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public Query<Object> withDataSource(DataSource dataSource)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public Query<Object> withOverriddenContext(Map<String, Object> contextOverride)
  {
    throw new UnsupportedOperationException();
  }
}
