/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.sql.calcite.http;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.Pair;
import io.druid.math.expr.ExprMacroTable;
import io.druid.query.QueryInterruptedException;
import io.druid.query.ResourceLimitExceededException;
import io.druid.server.security.AllowAllAuthenticator;
import io.druid.server.security.AuthConfig;
import io.druid.server.security.AuthTestUtils;
import io.druid.sql.calcite.planner.DruidOperatorTable;
import io.druid.sql.calcite.planner.PlannerConfig;
import io.druid.sql.calcite.planner.PlannerContext;
import io.druid.sql.calcite.planner.PlannerFactory;
import io.druid.sql.calcite.schema.DruidSchema;
import io.druid.sql.calcite.util.CalciteTestBase;
import io.druid.sql.calcite.util.CalciteTests;
import io.druid.sql.calcite.util.QueryLogHook;
import io.druid.sql.calcite.util.SpecificSegmentsQuerySegmentWalker;
import io.druid.sql.http.SqlQuery;
import io.druid.sql.http.SqlResource;
import org.apache.calcite.tools.ValidationException;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import java.io.ByteArrayOutputStream;
import java.util.List;
import java.util.Map;

public class SqlResourceTest extends CalciteTestBase
{
  private static final ObjectMapper JSON_MAPPER = new DefaultObjectMapper();

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public QueryLogHook queryLogHook = QueryLogHook.create();

  private SpecificSegmentsQuerySegmentWalker walker = null;

  private SqlResource resource;

  private HttpServletRequest req;


  @Before
  public void setUp() throws Exception
  {
    walker = CalciteTests.createMockWalker(temporaryFolder.newFolder());

    final PlannerConfig plannerConfig = new PlannerConfig();
    final DruidSchema druidSchema = CalciteTests.createMockSchema(walker, plannerConfig);
    final DruidOperatorTable operatorTable = CalciteTests.createOperatorTable();
    final ExprMacroTable macroTable = CalciteTests.createExprMacroTable();
    req = EasyMock.createStrictMock(HttpServletRequest.class);
    EasyMock.expect(req.getAttribute(AuthConfig.DRUID_ALLOW_UNSECURED_PATH)).andReturn(null).anyTimes();
    EasyMock.expect(req.getAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED))
            .andReturn(null)
            .anyTimes();
    EasyMock.expect(req.getAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT))
            .andReturn(AllowAllAuthenticator.ALLOW_ALL_RESULT)
            .anyTimes();
    req.setAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED, true);
    EasyMock.expectLastCall().anyTimes();
    EasyMock.expect(req.getAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT))
            .andReturn(AllowAllAuthenticator.ALLOW_ALL_RESULT)
            .anyTimes();
    EasyMock.replay(req);

    resource = new SqlResource(
        JSON_MAPPER,
        new PlannerFactory(
            druidSchema,
            CalciteTests.createMockQueryLifecycleFactory(walker),
            operatorTable,
            macroTable,
            plannerConfig,
            AuthTestUtils.TEST_AUTHORIZER_MAPPER,
            CalciteTests.getJsonMapper()
        )
    );
  }

  @After
  public void tearDown() throws Exception
  {
    walker.close();
    walker = null;
  }

  @Test
  public void testCountStar() throws Exception
  {
    final List<Map<String, Object>> rows = doPost(
        new SqlQuery("SELECT COUNT(*) AS cnt, 'foo' AS TheFoo FROM druid.foo", null, null)
    ).rhs;

    Assert.assertEquals(
        ImmutableList.of(
            ImmutableMap.of("cnt", 6, "TheFoo", "foo")
        ),
        rows
    );
  }

  @Test
  public void testCountStarAsArray() throws Exception
  {
    final List<List<Object>> rows = doPost(
        new SqlQuery("SELECT COUNT(*), 'foo' FROM druid.foo", SqlQuery.ResultFormat.ARRAY, null),
        new TypeReference<List<List<Object>>>()
        {
        }
    ).rhs;

    Assert.assertEquals(
        ImmutableList.of(
            ImmutableList.of(6, "foo")
        ),
        rows
    );
  }

  @Test
  public void testTimestampsInResponse() throws Exception
  {
    final List<Map<String, Object>> rows = doPost(
        new SqlQuery(
            "SELECT __time, CAST(__time AS DATE) AS t2 FROM druid.foo LIMIT 1",
            SqlQuery.ResultFormat.OBJECT,
            null
        )
    ).rhs;

    Assert.assertEquals(
        ImmutableList.of(
            ImmutableMap.of("__time", "2000-01-01T00:00:00.000Z", "t2", "2000-01-01T00:00:00.000Z")
        ),
        rows
    );
  }

  @Test
  public void testTimestampsInResponseLosAngelesTimeZone() throws Exception
  {
    final List<Map<String, Object>> rows = doPost(
        new SqlQuery(
            "SELECT __time, CAST(__time AS DATE) AS t2 FROM druid.foo LIMIT 1",
            SqlQuery.ResultFormat.OBJECT,
            ImmutableMap.<String, Object>of(PlannerContext.CTX_SQL_TIME_ZONE, "America/Los_Angeles")
        )
    ).rhs;

    Assert.assertEquals(
        ImmutableList.of(
            ImmutableMap.of("__time", "1999-12-31T16:00:00.000-08:00", "t2", "1999-12-31T00:00:00.000-08:00")
        ),
        rows
    );
  }

  @Test
  public void testFieldAliasingSelect() throws Exception
  {
    final List<Map<String, Object>> rows = doPost(
        new SqlQuery("SELECT dim2 \"x\", dim2 \"y\" FROM druid.foo LIMIT 1", SqlQuery.ResultFormat.OBJECT, null)
    ).rhs;

    Assert.assertEquals(
        ImmutableList.of(
            ImmutableMap.of("x", "a", "y", "a")
        ),
        rows
    );
  }

  @Test
  public void testFieldAliasingGroupBy() throws Exception
  {
    final List<Map<String, Object>> rows = doPost(
        new SqlQuery("SELECT dim2 \"x\", dim2 \"y\" FROM druid.foo GROUP BY dim2", SqlQuery.ResultFormat.OBJECT, null)
    ).rhs;

    Assert.assertEquals(
        ImmutableList.of(
            ImmutableMap.of("x", "", "y", ""),
            ImmutableMap.of("x", "a", "y", "a"),
            ImmutableMap.of("x", "abc", "y", "abc")
        ),
        rows
    );
  }

  @Test
  public void testExplainCountStar() throws Exception
  {
    final List<Map<String, Object>> rows = doPost(
        new SqlQuery("EXPLAIN PLAN FOR SELECT COUNT(*) AS cnt FROM druid.foo", SqlQuery.ResultFormat.OBJECT, null)
    ).rhs;

    Assert.assertEquals(
        ImmutableList.of(
            ImmutableMap.<String, Object>of(
                "PLAN",
                "DruidQueryRel(query=[{\"queryType\":\"timeseries\",\"dataSource\":{\"type\":\"table\",\"name\":\"foo\"},\"intervals\":{\"type\":\"intervals\",\"intervals\":[\"-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z\"]},\"descending\":false,\"virtualColumns\":[],\"filter\":null,\"granularity\":{\"type\":\"all\"},\"aggregations\":[{\"type\":\"count\",\"name\":\"a0\"}],\"postAggregations\":[],\"context\":{\"skipEmptyBuckets\":true}}], signature=[{a0:LONG}])\n"
            )
        ),
        rows
    );
  }

  @Test
  public void testCannotValidate() throws Exception
  {
    final QueryInterruptedException exception = doPost(
        new SqlQuery(
            "SELECT dim3 FROM druid.foo",
            SqlQuery.ResultFormat.OBJECT,
            null
        )
    ).lhs;

    Assert.assertNotNull(exception);
    Assert.assertEquals(QueryInterruptedException.UNKNOWN_EXCEPTION, exception.getErrorCode());
    Assert.assertEquals(ValidationException.class.getName(), exception.getErrorClass());
    Assert.assertTrue(exception.getMessage().contains("Column 'dim3' not found in any table"));
  }

  @Test
  public void testCannotConvert() throws Exception
  {
    // SELECT + ORDER unsupported
    final QueryInterruptedException exception = doPost(
        new SqlQuery("SELECT dim1 FROM druid.foo ORDER BY dim1", SqlQuery.ResultFormat.OBJECT, null)
    ).lhs;

    Assert.assertNotNull(exception);
    Assert.assertEquals(QueryInterruptedException.UNKNOWN_EXCEPTION, exception.getErrorCode());
    Assert.assertEquals(ISE.class.getName(), exception.getErrorClass());
    Assert.assertTrue(
        exception.getMessage()
                 .contains("Cannot build plan for query: SELECT dim1 FROM druid.foo ORDER BY dim1")
    );
  }

  @Test
  public void testResourceLimitExceeded() throws Exception
  {
    final QueryInterruptedException exception = doPost(
        new SqlQuery(
            "SELECT DISTINCT dim1 FROM foo",
            SqlQuery.ResultFormat.OBJECT,
            ImmutableMap.<String, Object>of("maxMergingDictionarySize", 1)
        )
    ).lhs;

    Assert.assertNotNull(exception);
    Assert.assertEquals(exception.getErrorCode(), QueryInterruptedException.RESOURCE_LIMIT_EXCEEDED);
    Assert.assertEquals(exception.getErrorClass(), ResourceLimitExceededException.class.getName());
  }

  // Returns either an error or a result.
  private <T> Pair<QueryInterruptedException, T> doPost(
      final SqlQuery query,
      final TypeReference<T> typeReference
  ) throws Exception
  {
    final Response response = resource.doPost(query, req);
    if (response.getStatus() == 200) {
      final StreamingOutput output = (StreamingOutput) response.getEntity();
      final ByteArrayOutputStream baos = new ByteArrayOutputStream();
      output.write(baos);
      return Pair.of(
          null,
          JSON_MAPPER.<T>readValue(baos.toByteArray(), typeReference)
      );
    } else {
      return Pair.of(
          JSON_MAPPER.readValue((byte[]) response.getEntity(), QueryInterruptedException.class),
          null
      );
    }
  }

  private Pair<QueryInterruptedException, List<Map<String, Object>>> doPost(final SqlQuery query) throws Exception
  {
    return doPost(query, new TypeReference<List<Map<String, Object>>>()
    {
    });
  }
}
