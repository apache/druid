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
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.Field;
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

  private List<ObjectNode> capturedEvents;
  private OpenLineageRequestLogger logger;

  @Before
  public void setUp()
  {
    capturedEvents = new ArrayList<>();
    logger = new OpenLineageRequestLogger(
        MAPPER,
        NAMESPACE,
        OpenLineageRequestLoggerProvider.TransportType.CONSOLE,
        null
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
  public void testSqlQuerySimpleSelect() throws IOException
  {
    logger.logSqlQuery(sqlLine(
        "SELECT * FROM \"kttm\"",
        ImmutableMap.of("sqlQueryId", "qid-1"),
        ImmutableMap.of("success", true, "sqlQuery/time", 200L, "sqlQuery/bytes", 1024L,
                        "sqlQuery/planningTimeMs", 10L, "statusCode", "200", "identity", "alice")
    ));

    Assert.assertEquals(1, capturedEvents.size());
    ObjectNode event = capturedEvents.get(0);

    Assert.assertEquals("COMPLETE", event.get("eventType").asText());
    Assert.assertEquals(NAMESPACE, event.get("job").get("namespace").asText());

    JsonNode inputs = event.get("inputs");
    Assert.assertEquals(1, inputs.size());
    Assert.assertEquals("kttm", inputs.get(0).get("name").asText());
    Assert.assertEquals(NAMESPACE, inputs.get(0).get("namespace").asText());

    Assert.assertEquals(0, event.get("outputs").size());
  }

  @Test
  public void testSqlQueryJoin() throws IOException
  {
    logger.logSqlQuery(sqlLine(
        "SELECT a.\"x\" FROM \"tableA\" AS a JOIN \"tableB\" AS b ON a.\"id\" = b.\"id\"",
        ImmutableMap.of("sqlQueryId", "qid-2"),
        ImmutableMap.of("success", true)
    ));

    Assert.assertEquals(1, capturedEvents.size());
    Set<String> names = inputNames(capturedEvents.get(0).get("inputs"));
    Assert.assertTrue(names.contains("tableA"));
    Assert.assertTrue(names.contains("tableB"));
  }

  @Test
  public void testSqlQueryCteExcludesCteNames() throws IOException
  {
    // "cte" is a CTE name and must NOT appear as an input; "realTable" is the physical source
    logger.logSqlQuery(sqlLine(
        "WITH \"cte\" AS (SELECT * FROM \"realTable\") SELECT * FROM \"cte\"",
        ImmutableMap.of("sqlQueryId", "qid-3"),
        ImmutableMap.of("success", true)
    ));

    Assert.assertEquals(1, capturedEvents.size());
    Set<String> names = inputNames(capturedEvents.get(0).get("inputs"));
    Assert.assertTrue(names.contains("realTable"));
    Assert.assertFalse(names.contains("cte"));
  }

  @Test
  public void testSqlQueryInsert() throws IOException
  {
    logger.logSqlQuery(sqlLine(
        "INSERT INTO \"outputTable\" SELECT * FROM \"inputTable\"",
        ImmutableMap.of("sqlQueryId", "qid-4"),
        ImmutableMap.of("success", true)
    ));

    Assert.assertEquals(1, capturedEvents.size());
    ObjectNode event = capturedEvents.get(0);

    Assert.assertTrue(inputNames(event.get("inputs")).contains("inputTable"));

    JsonNode outputs = event.get("outputs");
    Assert.assertEquals(1, outputs.size());
    Assert.assertEquals("outputTable", outputs.get(0).get("name").asText());
  }

  @Test
  public void testSqlQueryParseFailureStillEmitsEvent() throws IOException
  {
    // Druid-specific syntax (REPLACE INTO) doesn't parse with the standard Calcite parser;
    // the event is still emitted, just without table-level lineage.
    logger.logSqlQuery(sqlLine(
        "REPLACE INTO \"ds\" OVERWRITE ALL SELECT * FROM \"src\" PARTITIONED BY ALL",
        ImmutableMap.of("sqlQueryId", "qid-5"),
        ImmutableMap.of("success", true)
    ));

    Assert.assertEquals(1, capturedEvents.size());
    Assert.assertEquals(0, capturedEvents.get(0).get("inputs").size());
    Assert.assertEquals(0, capturedEvents.get(0).get("outputs").size());
  }

  @Test
  public void testSqlQueryFailedQueryHasErrorMessageFacet() throws IOException
  {
    logger.logSqlQuery(sqlLine(
        "SELECT * FROM \"t\"",
        ImmutableMap.of("sqlQueryId", "qid-6"),
        ImmutableMap.of("success", false, "exception", "Query timed out")
    ));

    Assert.assertEquals(1, capturedEvents.size());
    ObjectNode event = capturedEvents.get(0);

    Assert.assertEquals("FAIL", event.get("eventType").asText());

    JsonNode errorFacet = event.get("run").get("facets").get("errorMessage");
    Assert.assertNotNull(errorFacet);
    Assert.assertEquals("Query timed out", errorFacet.get("message").asText());
    Assert.assertEquals("SQL", errorFacet.get("programmingLanguage").asText());
  }

  @Test
  public void testSqlQueryNoErrorMessageFacetOnSuccess() throws IOException
  {
    logger.logSqlQuery(sqlLine(
        "SELECT 1",
        ImmutableMap.of("sqlQueryId", "qid-7"),
        ImmutableMap.of("success", true)
    ));

    Assert.assertEquals(1, capturedEvents.size());
    Assert.assertNull(capturedEvents.get(0).get("run").get("facets").get("errorMessage"));
  }

  @Test
  public void testSqlQueryStatsFacets() throws IOException
  {
    logger.logSqlQuery(sqlLine(
        "SELECT 1",
        ImmutableMap.of("sqlQueryId", "qid-8"),
        ImmutableMap.of(
            "success", true,
            "sqlQuery/time", 300L,
            "sqlQuery/bytes", 2048L,
            "sqlQuery/planningTimeMs", 25L,
            "statusCode", "200",
            "identity", "bob"
        )
    ));

    Assert.assertEquals(1, capturedEvents.size());
    JsonNode runFacets = capturedEvents.get(0).get("run").get("facets");

    JsonNode stats = runFacets.get("druid_query_statistics");
    Assert.assertEquals(300L, stats.get("durationMs").asLong());
    Assert.assertEquals(2048L, stats.get("bytes").asLong());
    Assert.assertEquals(25L, stats.get("planningTimeMs").asLong());
    Assert.assertEquals("200", stats.get("statusCode").asText());

    JsonNode ctx = runFacets.get("druid_query_context");
    Assert.assertEquals("bob", ctx.get("identity").asText());
    Assert.assertEquals(REMOTE_ADDR, ctx.get("remoteAddress").asText());
    Assert.assertEquals("sql", ctx.get("queryType").asText());
  }

  @Test
  public void testSqlQuerySqlFacetPresent() throws IOException
  {
    String sql = "SELECT * FROM \"t\"";
    logger.logSqlQuery(sqlLine(
        sql,
        ImmutableMap.of("sqlQueryId", "qid-9"),
        ImmutableMap.of("success", true)
    ));

    Assert.assertEquals(1, capturedEvents.size());
    JsonNode sqlFacet = capturedEvents.get(0).get("job").get("facets").get("sql");
    Assert.assertNotNull(sqlFacet);
    Assert.assertEquals(sql, sqlFacet.get("query").asText());
  }

  @Test
  public void testSqlQueryJobTypeFacet() throws IOException
  {
    logger.logSqlQuery(sqlLine(
        "SELECT 1",
        ImmutableMap.of("sqlQueryId", "qid-10"),
        ImmutableMap.of("success", true)
    ));

    Assert.assertEquals(1, capturedEvents.size());
    JsonNode jobType = capturedEvents.get(0).get("job").get("facets").get("jobType");
    Assert.assertNotNull(jobType);
    Assert.assertEquals("BATCH", jobType.get("processingType").asText());
    Assert.assertEquals("DRUID", jobType.get("integration").asText());
    Assert.assertEquals("QUERY", jobType.get("jobType").asText());
  }

  @Test
  public void testSqlQueryProcessingEngineFacet() throws IOException
  {
    logger.logSqlQuery(sqlLine(
        "SELECT 1",
        ImmutableMap.of("sqlQueryId", "qid-11"),
        ImmutableMap.of("success", true)
    ));

    Assert.assertEquals(1, capturedEvents.size());
    JsonNode engine = capturedEvents.get(0).get("run").get("facets").get("processing_engine");
    Assert.assertNotNull(engine);
    Assert.assertEquals("druid", engine.get("name").asText());
  }

  @Test
  public void testNativeQueryStandaloneEmitsEvent() throws IOException
  {
    TestQuery query = new TestQuery(new TableDataSource("myDatasource"), ImmutableMap.of("queryId", "native-qid-1"));
    logger.logNativeQuery(nativeLine(query, ImmutableMap.of("success", true, "query/time", 50L)));

    Assert.assertEquals(1, capturedEvents.size());
    ObjectNode event = capturedEvents.get(0);

    Assert.assertEquals("COMPLETE", event.get("eventType").asText());
    Assert.assertTrue(inputNames(event.get("inputs")).contains("myDatasource"));
  }

  @Test
  public void testNativeQuerySpawnedBySqlIsSkipped() throws IOException
  {
    // Queries with sqlQueryId in context are sub-queries of a SQL execution; skip to avoid duplicate events.
    TestQuery query = new TestQuery(
        new TableDataSource("myDatasource"),
        ImmutableMap.of("queryId", "native-qid-2", BaseQuery.SQL_QUERY_ID, "parent-sql-id")
    );
    logger.logNativeQuery(nativeLine(query, ImmutableMap.of("success", true)));

    Assert.assertEquals(0, capturedEvents.size());
  }

  @Test
  public void testNativeQueryStatFallbackKeys() throws IOException
  {
    // Native queries report timing via "query/time" and "query/bytes", not the "sqlQuery/*" variants.
    TestQuery query = new TestQuery(new TableDataSource("t"), ImmutableMap.of("queryId", "native-qid-3"));
    logger.logNativeQuery(nativeLine(
        query,
        ImmutableMap.of("success", true, "query/time", 75L, "query/bytes", 512L)
    ));

    Assert.assertEquals(1, capturedEvents.size());
    JsonNode stats = capturedEvents.get(0).get("run").get("facets").get("druid_query_statistics");
    Assert.assertEquals(75L, stats.get("durationMs").asLong());
    Assert.assertEquals(512L, stats.get("bytes").asLong());
  }

  @Test(expected = IllegalStateException.class)
  public void testProviderHttpWithoutUrlThrows() throws Exception
  {
    OpenLineageRequestLoggerProvider provider = new OpenLineageRequestLoggerProvider();
    Field transportTypeField = OpenLineageRequestLoggerProvider.class.getDeclaredField("transportType");
    transportTypeField.setAccessible(true);
    transportTypeField.set(provider, OpenLineageRequestLoggerProvider.TransportType.HTTP);
    // Validation moved to start() method for proper lifecycle management
    OpenLineageRequestLogger logger = (OpenLineageRequestLogger) provider.get();
    logger.start();
  }

  @Test
  public void testNativeQueryWithNullQueryDoesNotCrash() throws Exception
  {
    // Simulate a native query parse failure where query is null
    RequestLogLine logLine = RequestLogLine.forNative(null, TIMESTAMP, REMOTE_ADDR, new QueryStats(ImmutableMap.of()));

    // Should not throw NPE, should return early without emitting event
    logger.logNativeQuery(logLine);

    Assert.assertEquals(0, capturedEvents.size());
  }

  private static RequestLogLine sqlLine(String sql, Map<String, Object> context, Map<String, Object> stats)
  {
    return RequestLogLine.forSql(sql, context, TIMESTAMP, REMOTE_ADDR, new QueryStats(stats));
  }

  private static RequestLogLine nativeLine(Query<?> query, Map<String, Object> stats)
  {
    return RequestLogLine.forNative(query, TIMESTAMP, REMOTE_ADDR, new QueryStats(stats));
  }

  private static Set<String> inputNames(JsonNode inputs)
  {
    Set<String> names = new HashSet<>();
    for (JsonNode input : inputs) {
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
