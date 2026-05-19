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
import org.apache.druid.query.UnionDataSource;
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

  // --- logSqlQuery: SELECT is no-op; MSQ DML emits ---

  @Test
  public void testSqlSelectIsNoOp() throws IOException
  {
    logger.logSqlQuery(sqlLine(
        "SELECT * FROM \"kttm\"",
        ImmutableMap.of("sqlQueryId", "qid-1"),
        ImmutableMap.of("success", true)
    ));

    Assertions.assertEquals(0, capturedEvents.size());
  }

  @Test
  public void testMsqInsertEmitsOutputLineage() throws IOException
  {
    logger.logSqlQuery(sqlLine(
        "INSERT INTO \"kttm-result\" SELECT * FROM \"kttm\"",
        ImmutableMap.of("sqlQueryId", "msq-insert-1"),
        ImmutableMap.of("success", true, "sqlQuery/time", 1200L, "sqlQuery/bytes", 4096L)
    ));

    Assertions.assertEquals(1, capturedEvents.size());
    ObjectNode event = capturedEvents.get(0);

    Assertions.assertEquals("COMPLETE", event.get("eventType").asText());
    // Output table extracted from SQL
    Assertions.assertEquals(1, event.get("outputs").size());
    Assertions.assertEquals("kttm-result", event.get("outputs").get(0).get("name").asText());
    // Inputs are empty — extracting FROM clause requires a full SQL parser
    Assertions.assertEquals(0, event.get("inputs").size());
    // queryType is msq
    Assertions.assertEquals("msq", event.get("run").get("facets").get("druid_query_context").get("queryType").asText());
    // stats
    Assertions.assertEquals(1200L, event.get("run").get("facets").get("druid_query_statistics").get("durationMs").asLong());
  }

  @Test
  public void testMsqReplaceEmitsOutputLineage() throws IOException
  {
    logger.logSqlQuery(sqlLine(
        "REPLACE INTO \"kttm-result\" OVERWRITE ALL SELECT * FROM \"kttm\"",
        ImmutableMap.of("sqlQueryId", "msq-replace-1"),
        ImmutableMap.of("success", true)
    ));

    Assertions.assertEquals(1, capturedEvents.size());
    Assertions.assertEquals("kttm-result", capturedEvents.get(0).get("outputs").get(0).get("name").asText());
  }

  @Test
  public void testMsqInsertUnquotedTable() throws IOException
  {
    logger.logSqlQuery(sqlLine(
        "INSERT INTO kttm_result SELECT count(*) FROM kttm",
        ImmutableMap.of("sqlQueryId", "msq-unquoted-1"),
        ImmutableMap.of("success", true)
    ));

    Assertions.assertEquals(1, capturedEvents.size());
    Assertions.assertEquals("kttm_result", capturedEvents.get(0).get("outputs").get(0).get("name").asText());
  }

  @Test
  public void testMsqInsertFailure() throws IOException
  {
    logger.logSqlQuery(sqlLine(
        "INSERT INTO \"kttm-result\" SELECT * FROM \"kttm\"",
        ImmutableMap.of("sqlQueryId", "msq-fail-1"),
        ImmutableMap.of("success", false, "exception", "Task failed")
    ));

    Assertions.assertEquals(1, capturedEvents.size());
    ObjectNode event = capturedEvents.get(0);
    Assertions.assertEquals("FAIL", event.get("eventType").asText());
    Assertions.assertEquals("Task failed", event.get("run").get("facets").get("errorMessage").get("message").asText());
  }

  // --- Native query tests ---

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

    JsonNode stats = event.get("run").get("facets").get("druid_query_statistics");
    Assertions.assertEquals(50L, stats.get("durationMs").asLong());
    Assertions.assertEquals(512L, stats.get("bytes").asLong());

    // No sql facet on native queries
    Assertions.assertNull(event.get("job").get("facets").get("sql"));

    // Query type reflects the native query type
    Assertions.assertEquals("test", event.get("run").get("facets").get("druid_query_context").get("queryType").asText());
  }

  @Test
  public void testNativeSubQueryOfSqlEmits() throws IOException
  {
    // Native sub-queries of SQL emit events with the plain native queryType.
    // SQL origin is indicated by sqlQueryId in the context facet.
    TestQuery query = new TestQuery(
        new TableDataSource("myDatasource"),
        ImmutableMap.of("queryId", "native-qid-2", BaseQuery.SQL_QUERY_ID, "parent-sql-id")
    );
    logger.logNativeQuery(nativeLine(query, ImmutableMap.of("success", true)));

    Assertions.assertEquals(1, capturedEvents.size());
    ObjectNode event = capturedEvents.get(0);

    // queryType is the plain native type, not prefixed with "sql/"
    Assertions.assertEquals("test",
        event.get("run").get("facets").get("druid_query_context").get("queryType").asText());

    // sqlQueryId included for correlation with parent SQL
    Assertions.assertEquals("parent-sql-id",
        event.get("run").get("facets").get("druid_query_context").get("sqlQueryId").asText());

    Assertions.assertTrue(inputNames(event).contains("myDatasource"));
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
  public void testNativeQueryExcludedType() throws IOException
  {
    TestQuery query = new TestQuery(
        new TableDataSource("myDatasource"),
        ImmutableMap.of("queryId", "native-qid-excluded")
    )
    {
      @Override
      public String getType()
      {
        return "segmentMetadata";
      }
    };
    logger.logNativeQuery(nativeLine(query, ImmutableMap.of("success", true)));

    Assertions.assertEquals(0, capturedEvents.size());
  }

  @Test
  public void testNativeUnionDataSourceExtractsBothTables() throws IOException
  {
    // UnionDataSource.getTableNames() returns all member table names.
    // JoinDataSource has the same behavior but requires complex construction (ExprMacroTable etc.),
    // so we test UnionDataSource as the multi-table representative.
    DataSource unionDs = new UnionDataSource(
        List.of(new TableDataSource("leftTable"), new TableDataSource("rightTable"))
    );
    TestQuery query = new TestQuery(unionDs, ImmutableMap.of("queryId", "native-multi-1"));
    logger.logNativeQuery(nativeLine(query, ImmutableMap.of("success", true)));

    Assertions.assertEquals(1, capturedEvents.size());
    Set<String> names = inputNames(capturedEvents.get(0));
    Assertions.assertEquals(2, names.size());
    Assertions.assertTrue(names.contains("leftTable"));
    Assertions.assertTrue(names.contains("rightTable"));
  }

  @Test
  public void testNativeQueryFacets() throws IOException
  {
    TestQuery query = new TestQuery(
        new TableDataSource("t"),
        ImmutableMap.of("queryId", "native-facets")
    );
    logger.logNativeQuery(nativeLine(query, ImmutableMap.of(
        "success", true,
        "query/time", 300L,
        "query/bytes", 2048L,
        "statusCode", "200",
        "identity", "bob"
    )));

    Assertions.assertEquals(1, capturedEvents.size());
    ObjectNode event = capturedEvents.get(0);

    // OpenLineage envelope
    Assertions.assertNotNull(event.get("schemaURL"));
    Assertions.assertNotNull(event.get("producer"));
    Assertions.assertEquals("2024-01-01T00:00:00.000Z", event.get("eventTime").asText());

    // Job facets — no SQL facet
    JsonNode jobFacets = event.get("job").get("facets");
    Assertions.assertNull(jobFacets.get("sql"));
    Assertions.assertEquals("BATCH", jobFacets.get("jobType").get("processingType").asText());
    Assertions.assertEquals("DRUID", jobFacets.get("jobType").get("integration").asText());
    Assertions.assertEquals("QUERY", jobFacets.get("jobType").get("jobType").asText());

    // Run facets
    JsonNode runFacets = event.get("run").get("facets");
    Assertions.assertEquals("druid", runFacets.get("processing_engine").get("name").asText());

    // Query context facet
    JsonNode ctx = runFacets.get("druid_query_context");
    Assertions.assertEquals("bob", ctx.get("identity").asText());
    Assertions.assertEquals(REMOTE_ADDR, ctx.get("remoteAddress").asText());
    Assertions.assertEquals("test", ctx.get("queryType").asText());
    Assertions.assertNull(ctx.get("sqlQueryId"));

    // Statistics facet
    JsonNode stats = runFacets.get("druid_query_statistics");
    Assertions.assertEquals(300L, stats.get("durationMs").asLong());
    Assertions.assertEquals(2048L, stats.get("bytes").asLong());
    Assertions.assertEquals("200", stats.get("statusCode").asText());

    // No error facet on success
    Assertions.assertNull(runFacets.get("errorMessage"));
  }

  @Test
  public void testNativeQueryFailure() throws IOException
  {
    TestQuery query = new TestQuery(
        new TableDataSource("t"),
        ImmutableMap.of("queryId", "native-fail")
    );
    logger.logNativeQuery(nativeLine(query, ImmutableMap.of(
        "success", false,
        "exception", "Query timed out"
    )));

    Assertions.assertEquals(1, capturedEvents.size());
    ObjectNode event = capturedEvents.get(0);

    Assertions.assertEquals("FAIL", event.get("eventType").asText());

    JsonNode errorFacet = event.get("run").get("facets").get("errorMessage");
    Assertions.assertNotNull(errorFacet);
    Assertions.assertEquals("Query timed out", errorFacet.get("message").asText());
    // Native query — no "programmingLanguage" field
    Assertions.assertNull(errorFacet.get("programmingLanguage"));
  }

  @Test
  public void testNativeSqlSubQueryFailure() throws IOException
  {
    TestQuery query = new TestQuery(
        new TableDataSource("t"),
        ImmutableMap.of("queryId", "native-sql-fail", BaseQuery.SQL_QUERY_ID, "parent-sql")
    );
    logger.logNativeQuery(nativeLine(query, ImmutableMap.of(
        "success", false,
        "exception", "Query timed out"
    )));

    Assertions.assertEquals(1, capturedEvents.size());
    ObjectNode event = capturedEvents.get(0);

    Assertions.assertEquals("FAIL", event.get("eventType").asText());

    JsonNode errorFacet = event.get("run").get("facets").get("errorMessage");
    Assertions.assertNotNull(errorFacet);
    // SQL-originated query — has "programmingLanguage"
    Assertions.assertEquals("SQL", errorFacet.get("programmingLanguage").asText());
  }

  @Test
  public void testConstructorThrowsWhenHttpWithoutUrl()
  {
    Assertions.assertThrows(IllegalStateException.class, () -> new OpenLineageRequestLogger(
        MAPPER,
        NAMESPACE,
        OpenLineageRequestLoggerProvider.TransportType.HTTP,
        null,
        DEFAULT_EXCLUDED_NATIVE_QUERY_TYPES
    ));
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
