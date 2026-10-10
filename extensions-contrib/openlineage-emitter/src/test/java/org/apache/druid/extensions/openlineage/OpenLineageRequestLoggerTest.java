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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.BaseQuery;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.Druids;
import org.apache.druid.query.FilteredDataSource;
import org.apache.druid.query.JoinDataSource;
import org.apache.druid.query.LookupDataSource;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryDataSource;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.UnionDataSource;
import org.apache.druid.query.aggregation.FilteredAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.query.spec.QuerySegmentSpec;
import org.apache.druid.query.topn.TopNQueryBuilder;
import org.apache.druid.query.union.UnionQuery;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.join.JoinType;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;
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
  public void testLogSqlQueryWithNullSql() throws IOException
  {
    logger.logSqlQuery(RequestLogLine.forSql(
        null,
        ImmutableMap.of(),
        TIMESTAMP,
        REMOTE_ADDR,
        new QueryStats(ImmutableMap.of())
    ));

    Assertions.assertEquals(0, capturedEvents.size());
  }

  @Test
  public void testMsqInsertMissingSqlQueryId() throws IOException
  {
    // No sqlQueryId in context → falls back to UNKNOWN_QUERY_ID
    logger.logSqlQuery(sqlLine(
        "INSERT INTO \"kttm-result\" SELECT * FROM \"kttm\"",
        ImmutableMap.of(),
        ImmutableMap.of("success", true)
    ));

    Assertions.assertEquals(1, capturedEvents.size());
    Assertions.assertEquals(
        OpenLineageRequestLogger.UNKNOWN_QUERY_ID,
        capturedEvents.get(0).get("job").get("name").asText()
    );
  }

  @Test
  public void testMsqInsertNullContext() throws IOException
  {
    // Null sqlQueryContext → falls back to UNKNOWN_QUERY_ID
    logger.logSqlQuery(RequestLogLine.forSql(
        "INSERT INTO \"kttm-result\" SELECT * FROM \"kttm\"",
        null,
        TIMESTAMP,
        REMOTE_ADDR,
        new QueryStats(ImmutableMap.of("success", true))
    ));

    Assertions.assertEquals(1, capturedEvents.size());
    Assertions.assertEquals(
        OpenLineageRequestLogger.UNKNOWN_QUERY_ID,
        capturedEvents.get(0).get("job").get("name").asText()
    );
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
    // sql facet present with the original SQL text
    Assertions.assertEquals(
        "INSERT INTO \"kttm-result\" SELECT * FROM \"kttm\"",
        event.get("job").get("facets").get("sql").get("query").asText()
    );
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
  public void testMsqInsertDruidSchemaPrefix() throws IOException
  {
    // Druid normalizes "druid.foo" → "foo" — our AST extraction should match the planner.
    logger.logSqlQuery(sqlLine(
        "INSERT INTO druid.kttm_result SELECT * FROM kttm",
        ImmutableMap.of("sqlQueryId", "msq-druid-schema-1"),
        ImmutableMap.of("success", true)
    ));

    Assertions.assertEquals(1, capturedEvents.size());
    Assertions.assertEquals("kttm_result", capturedEvents.get(0).get("outputs").get(0).get("name").asText());
  }

  @Test
  public void testMsqInsertCteUnwrapped() throws IOException
  {
    // In Druid MSQ, CTEs appear as the source of the INSERT, not as a wrapper around it.
    // The target table must still be extracted correctly from the outer SqlInsert node.
    logger.logSqlQuery(sqlLine(
        "INSERT INTO kttm_result WITH staged AS (SELECT * FROM kttm) SELECT * FROM staged",
        ImmutableMap.of("sqlQueryId", "msq-cte-1"),
        ImmutableMap.of("success", true)
    ));

    Assertions.assertEquals(1, capturedEvents.size());
    Assertions.assertEquals("kttm_result", capturedEvents.get(0).get("outputs").get(0).get("name").asText());
  }

  @Test
  public void testMsqInsertThreePartCatalogPrefix() throws IOException
  {
    // catalog.druid.foo → Druid normalizes to bare name "foo"; AST extraction matches.
    logger.logSqlQuery(sqlLine(
        "INSERT INTO catalog.druid.kttm_result SELECT * FROM kttm",
        ImmutableMap.of("sqlQueryId", "msq-catalog-1"),
        ImmutableMap.of("success", true)
    ));

    Assertions.assertEquals(1, capturedEvents.size());
    Assertions.assertEquals("kttm_result", capturedEvents.get(0).get("outputs").get(0).get("name").asText());
  }

  @Test
  public void testMsqInsertWithSetPreamble() throws IOException
  {
    // Druid SQL accepts SET statements before DML. We call DruidSqlParser with
    // allowSetStatements=true; the SET is dropped and only the main INSERT is inspected.
    logger.logSqlQuery(sqlLine(
        "SET maxNumTasks = 2;\nINSERT INTO kttm_result SELECT * FROM kttm",
        ImmutableMap.of("sqlQueryId", "msq-set-1"),
        ImmutableMap.of("success", true)
    ));

    Assertions.assertEquals(1, capturedEvents.size());
    Assertions.assertEquals("kttm_result", capturedEvents.get(0).get("outputs").get(0).get("name").asText());
  }

  @Test
  public void testMsqInsertExternExportSkipped() throws IOException
  {
    // INSERT INTO EXTERN(...) AS CSV writes to a file, not a Druid datasource.
    // The AST target is a SqlCall, not a SqlIdentifier; emit no event at all.
    logger.logSqlQuery(sqlLine(
        "INSERT INTO EXTERN(s3(bucket => 'x', prefix => 'y')) AS CSV SELECT * FROM kttm",
        ImmutableMap.of("sqlQueryId", "msq-extern-1"),
        ImmutableMap.of("success", true)
    ));

    Assertions.assertEquals(0, capturedEvents.size(), "EXTERN exports should not emit an output dataset");
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
  public void testNativeUnionQueryExtractsBranchTables() throws IOException
  {
    // A top-level UnionQuery's getDataSource() throws by design; the logger must not crash on it and
    // must still emit an event carrying each branch's tables as inputs.
    ScanQuery branchA = Druids.newScanQueryBuilder()
        .dataSource("leftTable")
        .intervals(everyInterval())
        .columns("country")
        .context(ImmutableMap.of("queryId", "native-union-1"))
        .build();
    ScanQuery branchB = Druids.newScanQueryBuilder()
        .dataSource("rightTable")
        .intervals(everyInterval())
        .columns("region")
        .build();
    UnionQuery union = new UnionQuery(List.of(branchA, branchB));
    logger.logNativeQuery(nativeLine(union, ImmutableMap.of("success", true)));

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

  @Test
  public void testStopWithHttpTransport()
  {
    OpenLineageRequestLogger httpLogger = new OpenLineageRequestLogger(
        MAPPER,
        NAMESPACE,
        OpenLineageRequestLoggerProvider.TransportType.HTTP,
        "http://localhost:9999/api/v1/lineage",
        DEFAULT_EXCLUDED_NATIVE_QUERY_TYPES
    );
    // Covers the executor-shutdown and httpClient-close branches in stop()
    Assertions.assertDoesNotThrow(httpLogger::stop);
  }

  // --- Column-level lineage tests (BDCE-559) ---

  @Test
  public void testScanColumnLineage() throws IOException
  {
    ScanQuery query = Druids.newScanQueryBuilder()
        .dataSource("events")
        .intervals(everyInterval())
        .columns("userId", "page")
        .filters(new SelectorDimFilter("country", "US", null))
        .context(ImmutableMap.of("queryId", "scan-1"))
        .build();
    logger.logNativeQuery(nativeLine(query, ImmutableMap.of("success", true)));

    JsonNode input = inputByName(capturedEvents.get(0), "events");
    Assertions.assertEquals(ImmutableList.of("country", "page", "userId"), schemaFieldNames(input));
    Assertions.assertEquals(ImmutableList.of("PROJECTION"), usagesOf(input, "userId"));
    Assertions.assertEquals(ImmutableList.of("PROJECTION"), usagesOf(input, "page"));
    Assertions.assertEquals(ImmutableList.of("FILTER"), usagesOf(input, "country"));
  }

  @Test
  public void testGroupByColumnLineage() throws IOException
  {
    GroupByQuery query = GroupByQuery.builder()
        .setDataSource("sales")
        .setQuerySegmentSpec(everyInterval())
        .setGranularity(Granularities.ALL)
        .setDimensions(new DefaultDimensionSpec("country", "country"))
        .setAggregatorSpecs(new LongSumAggregatorFactory("total", "amount"))
        .setDimFilter(new SelectorDimFilter("status", "active", null))
        .setContext(ImmutableMap.of("queryId", "gb-1"))
        .build();
    logger.logNativeQuery(nativeLine(query, ImmutableMap.of("success", true)));

    JsonNode input = inputByName(capturedEvents.get(0), "sales");
    Assertions.assertEquals(ImmutableList.of("amount", "country", "status"), schemaFieldNames(input));
    Assertions.assertEquals(ImmutableList.of("AGGREGATION"), usagesOf(input, "amount"));
    Assertions.assertEquals(ImmutableList.of("GROUP_BY"), usagesOf(input, "country"));
    Assertions.assertEquals(ImmutableList.of("FILTER"), usagesOf(input, "status"));
  }

  @Test
  public void testVirtualColumnExpandsToBaseColumns() throws IOException
  {
    GroupByQuery query = GroupByQuery.builder()
        .setDataSource("sales")
        .setQuerySegmentSpec(everyInterval())
        .setGranularity(Granularities.ALL)
        .setVirtualColumns(VirtualColumns.create(ImmutableList.of(
            new ExpressionVirtualColumn("v0", "\"base\" * 2", ColumnType.LONG, ExprMacroTable.nil())
        )))
        .setDimensions(new DefaultDimensionSpec("v0", "v0"))
        .setContext(ImmutableMap.of("queryId", "vc-1"))
        .build();
    logger.logNativeQuery(nativeLine(query, ImmutableMap.of("success", true)));

    JsonNode input = inputByName(capturedEvents.get(0), "sales");
    // The virtual column "v0" is expanded to its underlying base column "base"; "v0" is not emitted.
    Assertions.assertEquals(ImmutableList.of("base"), schemaFieldNames(input));
    Assertions.assertEquals(ImmutableList.of("GROUP_BY"), usagesOf(input, "base"));
  }

  @Test
  public void testSelectStarEmitsNoColumnFacets() throws IOException
  {
    // Scan with no explicit columns (SELECT *) -> table-level lineage only, no column facets.
    ScanQuery query = Druids.newScanQueryBuilder()
        .dataSource("events")
        .intervals(everyInterval())
        .context(ImmutableMap.of("queryId", "scan-star"))
        .build();
    logger.logNativeQuery(nativeLine(query, ImmutableMap.of("success", true)));

    JsonNode input = inputByName(capturedEvents.get(0), "events");
    Assertions.assertNotNull(input);
    Assertions.assertNull(input.get("facets").get("schema"));
    Assertions.assertNull(input.get("facets").get("druid_columnUsage"));
  }

  @Test
  public void testUnionReplicatesColumnsToMembers() throws IOException
  {
    // Union members share the same output schema, so referenced columns are attributed to each member.
    GroupByQuery query = GroupByQuery.builder()
        .setDataSource(new UnionDataSource(ImmutableList.of(
            new TableDataSource("sales_a"),
            new TableDataSource("sales_b")
        )))
        .setQuerySegmentSpec(everyInterval())
        .setGranularity(Granularities.ALL)
        .setDimensions(new DefaultDimensionSpec("country", "country"))
        .setContext(ImmutableMap.of("queryId", "union-1"))
        .build();
    logger.logNativeQuery(nativeLine(query, ImmutableMap.of("success", true)));

    JsonNode event = capturedEvents.get(0);
    Set<String> names = inputNames(event);
    Assertions.assertEquals(2, names.size());
    Assertions.assertTrue(names.contains("sales_a") && names.contains("sales_b"));
    for (String table : ImmutableList.of("sales_a", "sales_b")) {
      JsonNode input = inputByName(event, table);
      Assertions.assertEquals(ImmutableList.of("country"), schemaFieldNames(input), table);
      Assertions.assertEquals(ImmutableList.of("GROUP_BY"), usagesOf(input, "country"));
    }
  }

  @Test
  public void testJoinColumnLineageAttributedPerSide() throws IOException
  {
    JoinDataSource join = JoinDataSource.create(
        new TableDataSource("sales"),
        new TableDataSource("users"),
        "j0.",
        "\"country\" == \"j0.country\"",
        JoinType.INNER,
        null,
        ExprMacroTable.nil(),
        null,
        null
    );
    ScanQuery query = Druids.newScanQueryBuilder()
        .dataSource(join)
        .intervals(everyInterval())
        .columns("country", "j0.age")
        .context(ImmutableMap.of("queryId", "join-1"))
        .build();
    logger.logNativeQuery(nativeLine(query, ImmutableMap.of("success", true)));

    JsonNode event = capturedEvents.get(0);
    JsonNode sales = inputByName(event, "sales");
    JsonNode users = inputByName(event, "users");

    // Left-side column "country" is both projected and used as a join key.
    Assertions.assertEquals(ImmutableList.of("country"), schemaFieldNames(sales));
    Assertions.assertEquals(ImmutableList.of("PROJECTION", "JOIN"), usagesOf(sales, "country"));
    // Right-side columns are un-prefixed and attributed to "users".
    Assertions.assertEquals(ImmutableList.of("age", "country"), schemaFieldNames(users));
    Assertions.assertEquals(ImmutableList.of("PROJECTION"), usagesOf(users, "age"));
    Assertions.assertEquals(ImmutableList.of("JOIN"), usagesOf(users, "country"));
  }

  @Test
  public void testSubQueryColumnLineageRecursesToBaseTable() throws IOException
  {
    GroupByQuery inner = GroupByQuery.builder()
        .setDataSource("sales")
        .setQuerySegmentSpec(everyInterval())
        .setGranularity(Granularities.ALL)
        .setDimensions(new DefaultDimensionSpec("country", "country"))
        .setAggregatorSpecs(new LongSumAggregatorFactory("total", "amount"))
        .build();
    GroupByQuery outer = GroupByQuery.builder()
        .setDataSource(new QueryDataSource(inner))
        .setQuerySegmentSpec(everyInterval())
        .setGranularity(Granularities.ALL)
        .setDimensions(new DefaultDimensionSpec("country", "country"))
        .setAggregatorSpecs(new LongSumAggregatorFactory("grand", "total"))
        .setContext(ImmutableMap.of("queryId", "subq-1"))
        .build();
    logger.logNativeQuery(nativeLine(outer, ImmutableMap.of("success", true)));

    JsonNode sales = inputByName(capturedEvents.get(0), "sales");
    // Base columns come from the sub-query's own parts; the outer references to sub-query outputs
    // ("total", "grand") must not be fabricated as base columns.
    Assertions.assertEquals(ImmutableList.of("amount", "country"), schemaFieldNames(sales));
    Assertions.assertEquals(ImmutableList.of("AGGREGATION"), usagesOf(sales, "amount"));
    Assertions.assertEquals(ImmutableList.of("GROUP_BY"), usagesOf(sales, "country"));
  }

  @Test
  public void testTopNColumnLineage() throws IOException
  {
    Query<?> query = new TopNQueryBuilder()
        .dataSource("sales")
        .intervals(everyInterval())
        .granularity(Granularities.ALL)
        .dimension(new DefaultDimensionSpec("country", "country"))
        .metric("total")
        .threshold(10)
        .aggregators(new LongSumAggregatorFactory("total", "amount"))
        .filters(new SelectorDimFilter("status", "active", null))
        .context(ImmutableMap.of("queryId", "topn-1"))
        .build();
    logger.logNativeQuery(nativeLine(query, ImmutableMap.of("success", true)));

    JsonNode sales = inputByName(capturedEvents.get(0), "sales");
    Assertions.assertEquals(ImmutableList.of("amount", "country", "status"), schemaFieldNames(sales));
    Assertions.assertEquals(ImmutableList.of("GROUP_BY"), usagesOf(sales, "country"));
    Assertions.assertEquals(ImmutableList.of("AGGREGATION"), usagesOf(sales, "amount"));
    Assertions.assertEquals(ImmutableList.of("FILTER"), usagesOf(sales, "status"));
  }

  @Test
  public void testTimeseriesColumnLineage() throws IOException
  {
    Query<?> query = Druids.newTimeseriesQueryBuilder()
        .dataSource("sales")
        .intervals(everyInterval())
        .granularity(Granularities.ALL)
        .aggregators(new LongSumAggregatorFactory("total", "amount"))
        .filters(new SelectorDimFilter("status", "active", null))
        .context(ImmutableMap.of("queryId", "ts-1"))
        .build();
    logger.logNativeQuery(nativeLine(query, ImmutableMap.of("success", true)));

    JsonNode sales = inputByName(capturedEvents.get(0), "sales");
    // Timeseries has no dimensions, so no GROUP_BY role.
    Assertions.assertEquals(ImmutableList.of("amount", "status"), schemaFieldNames(sales));
    Assertions.assertEquals(ImmutableList.of("AGGREGATION"), usagesOf(sales, "amount"));
    Assertions.assertEquals(ImmutableList.of("FILTER"), usagesOf(sales, "status"));
  }

  @Test
  public void testColumnUsedInMultipleRoles() throws IOException
  {
    GroupByQuery query = GroupByQuery.builder()
        .setDataSource("sales")
        .setQuerySegmentSpec(everyInterval())
        .setGranularity(Granularities.ALL)
        .setDimensions(new DefaultDimensionSpec("country", "country"))
        .setDimFilter(new SelectorDimFilter("country", "US", null))
        .setContext(ImmutableMap.of("queryId", "multirole-1"))
        .build();
    logger.logNativeQuery(nativeLine(query, ImmutableMap.of("success", true)));

    JsonNode sales = inputByName(capturedEvents.get(0), "sales");
    // "country" is both a grouping dimension and a filter; roles merge in enum declaration order.
    Assertions.assertEquals(ImmutableList.of("GROUP_BY", "FILTER"), usagesOf(sales, "country"));
  }

  @Test
  public void testLookupJoinDropsRightColumnsNotFabricated() throws IOException
  {
    JoinDataSource join = JoinDataSource.create(
        new TableDataSource("sales"),
        new LookupDataSource("country_lookup"),
        "j0.",
        "\"country\" == \"j0.k\"",
        JoinType.LEFT,
        null,
        ExprMacroTable.nil(),
        null,
        null
    );
    ScanQuery query = Druids.newScanQueryBuilder()
        .dataSource(join)
        .intervals(everyInterval())
        .columns("country", "j0.v")
        .context(ImmutableMap.of("queryId", "lookupjoin-1"))
        .build();
    logger.logNativeQuery(nativeLine(query, ImmutableMap.of("success", true)));

    JsonNode event = capturedEvents.get(0);
    // The lookup has no base table -> it contributes no input dataset and no fabricated columns.
    Assertions.assertEquals(Collections.singleton("sales"), inputNames(event));
    JsonNode sales = inputByName(event, "sales");
    Assertions.assertEquals(ImmutableList.of("country"), schemaFieldNames(sales));
    // "j0.v" (lookup value) and "j0.k" (lookup key) are dropped, not attributed to "sales".
    Assertions.assertEquals(ImmutableList.of("PROJECTION", "JOIN"), usagesOf(sales, "country"));
  }

  @Test
  public void testColumnLineageDisabledEmitsTableLevelOnly() throws IOException
  {
    OpenLineageRequestLogger disabled = new OpenLineageRequestLogger(
        MAPPER,
        NAMESPACE,
        OpenLineageRequestLoggerProvider.TransportType.CONSOLE,
        null,
        DEFAULT_EXCLUDED_NATIVE_QUERY_TYPES,
        false,
        OpenLineageRequestLogger.DEFAULT_EMIT_QUEUE_CAPACITY,
        OpenLineageRequestLogger.DEFAULT_EMIT_THREAD_COUNT,
        null
    )
    {
      @Override
      protected void emit(ObjectNode event)
      {
        capturedEvents.add(event);
      }
    };
    GroupByQuery query = GroupByQuery.builder()
        .setDataSource("sales")
        .setQuerySegmentSpec(everyInterval())
        .setGranularity(Granularities.ALL)
        .setDimensions(new DefaultDimensionSpec("country", "country"))
        .setContext(ImmutableMap.of("queryId", "disabled-1"))
        .build();
    disabled.logNativeQuery(nativeLine(query, ImmutableMap.of("success", true)));

    JsonNode sales = inputByName(capturedEvents.get(0), "sales");
    Assertions.assertNull(sales.get("facets").get("schema"), "column lineage disabled -> no schema facet");
    Assertions.assertNull(sales.get("facets").get("druid_columnUsage"));
  }

  @Test
  public void testFilteredAggregatorSplitsAggregationAndFilterRoles() throws IOException
  {
    // SUM(added) FILTER (WHERE status = 'active'): "added" is the aggregation input, "status" the filter.
    Query<?> query = Druids.newTimeseriesQueryBuilder()
        .dataSource("sales")
        .intervals(everyInterval())
        .granularity(Granularities.ALL)
        .aggregators(new FilteredAggregatorFactory(
            new LongSumAggregatorFactory("s", "added"),
            new SelectorDimFilter("status", "active", null)
        ))
        .context(ImmutableMap.of("queryId", "filtagg-1"))
        .build();
    logger.logNativeQuery(nativeLine(query, ImmutableMap.of("success", true)));

    JsonNode sales = inputByName(capturedEvents.get(0), "sales");
    Assertions.assertEquals(ImmutableList.of("added", "status"), schemaFieldNames(sales));
    Assertions.assertEquals(ImmutableList.of("AGGREGATION"), usagesOf(sales, "added"));
    Assertions.assertEquals(ImmutableList.of("FILTER"), usagesOf(sales, "status"));
  }

  @Test
  public void testJoinBaseTableFilterColumns() throws IOException
  {
    JoinDataSource join = JoinDataSource.create(
        new TableDataSource("sales"),
        new TableDataSource("users"),
        "j0.",
        "\"country\" == \"j0.country\"",
        JoinType.INNER,
        new SelectorDimFilter("status", "active", null),
        ExprMacroTable.nil(),
        null,
        null
    );
    ScanQuery query = Druids.newScanQueryBuilder()
        .dataSource(join)
        .intervals(everyInterval())
        .columns("country")
        .context(ImmutableMap.of("queryId", "joinfilter-1"))
        .build();
    logger.logNativeQuery(nativeLine(query, ImmutableMap.of("success", true)));

    JsonNode sales = inputByName(capturedEvents.get(0), "sales");
    // The join's base-table (left) filter on "status" is captured as FILTER on the base table.
    Assertions.assertEquals(ImmutableList.of("FILTER"), usagesOf(sales, "status"));
    Assertions.assertEquals(ImmutableList.of("PROJECTION", "JOIN"), usagesOf(sales, "country"));
  }

  @Test
  public void testFilteredDataSourceCapturesFilterColumns() throws IOException
  {
    FilteredDataSource filtered = FilteredDataSource.create(
        new TableDataSource("sales"),
        new SelectorDimFilter("status", "active", null)
    );
    ScanQuery query = Druids.newScanQueryBuilder()
        .dataSource(filtered)
        .intervals(everyInterval())
        .columns("country")
        .context(ImmutableMap.of("queryId", "filtds-1"))
        .build();
    logger.logNativeQuery(nativeLine(query, ImmutableMap.of("success", true)));

    JsonNode sales = inputByName(capturedEvents.get(0), "sales");
    Assertions.assertEquals(ImmutableList.of("country", "status"), schemaFieldNames(sales));
    Assertions.assertEquals(ImmutableList.of("PROJECTION"), usagesOf(sales, "country"));
    Assertions.assertEquals(ImmutableList.of("FILTER"), usagesOf(sales, "status"));
  }

  private static QuerySegmentSpec everyInterval()
  {
    return new MultipleIntervalSegmentSpec(Collections.singletonList(Intervals.of("2024-01-01/2024-01-02")));
  }

  private static JsonNode inputByName(JsonNode event, String name)
  {
    for (JsonNode input : event.get("inputs")) {
      if (input.get("name").asText().equals(name)) {
        return input;
      }
    }
    return null;
  }

  private static List<String> schemaFieldNames(JsonNode dataset)
  {
    List<String> names = new ArrayList<>();
    for (JsonNode field : dataset.get("facets").get("schema").get("fields")) {
      names.add(field.get("name").asText());
    }
    return names;
  }

  private static List<String> usagesOf(JsonNode dataset, String column)
  {
    List<String> usages = new ArrayList<>();
    JsonNode entry = dataset.get("facets").get("druid_columnUsage").get("fields").get(column);
    for (JsonNode usage : entry.get("usages")) {
      usages.add(usage.asText());
    }
    return usages;
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
