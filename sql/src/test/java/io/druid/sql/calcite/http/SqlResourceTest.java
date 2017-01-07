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
import io.druid.query.QueryInterruptedException;
import io.druid.sql.calcite.planner.Calcites;
import io.druid.sql.calcite.planner.PlannerConfig;
import io.druid.sql.calcite.util.CalciteTests;
import io.druid.sql.http.SqlQuery;
import io.druid.sql.http.SqlResource;
import org.apache.calcite.jdbc.CalciteConnection;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import java.io.ByteArrayOutputStream;
import java.util.List;
import java.util.Map;

public class SqlResourceTest
{
  private static final ObjectMapper JSON_MAPPER = new DefaultObjectMapper();

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private CalciteConnection connection;
  private SqlResource resource;

  @Before
  public void setUp() throws Exception
  {
    final PlannerConfig plannerConfig = new PlannerConfig();
    connection = Calcites.jdbc(
        CalciteTests.createMockSchema(
            CalciteTests.createWalker(temporaryFolder.newFolder()),
            plannerConfig
        ),
        plannerConfig
    );
    resource = new SqlResource(JSON_MAPPER, connection);
  }

  @After
  public void tearDown() throws Exception
  {
    connection.close();
    connection = null;
  }

  @Test
  public void testCountStar() throws Exception
  {
    final List<Map<String, Object>> rows = doPost(
        new SqlQuery("SELECT COUNT(*) AS cnt FROM druid.foo")
    );

    Assert.assertEquals(
        ImmutableList.of(
            ImmutableMap.of("cnt", 6)
        ),
        rows
    );
  }

  @Test
  public void testTimestampsInResponse() throws Exception
  {
    final List<Map<String, Object>> rows = doPost(
        new SqlQuery("SELECT __time FROM druid.foo LIMIT 1")
    );

    Assert.assertEquals(
        ImmutableList.of(
            ImmutableMap.of("__time", "2000-01-01T00:00:00.000Z")
        ),
        rows
    );
  }

  @Test
  public void testFieldAliasingSelect() throws Exception
  {
    final List<Map<String, Object>> rows = doPost(
        new SqlQuery("SELECT dim2 \"x\", dim2 \"y\" FROM druid.foo LIMIT 1")
    );

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
        new SqlQuery("SELECT dim2 \"x\", dim2 \"y\" FROM druid.foo GROUP BY dim2")
    );

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
        new SqlQuery("EXPLAIN PLAN FOR SELECT COUNT(*) AS cnt FROM druid.foo")
    );

    Assert.assertEquals(
        ImmutableList.of(
            ImmutableMap.<String, Object>of(
                "PLAN",
                "EnumerableInterpreter\n"
                + "  DruidQueryRel(dataSource=[foo], dimensions=[[]], aggregations=[[Aggregation{aggregatorFactories=[CountAggregatorFactory{name='a0'}], postAggregator=null, finalizingPostAggregatorFactory=null}]])\n"
            )
        ),
        rows
    );
  }

  @Test
  public void testCannotPlan() throws Exception
  {
    expectedException.expect(QueryInterruptedException.class);
    expectedException.expectMessage("Column 'dim3' not found in any table");

    doPost(
        new SqlQuery("SELECT dim3 FROM druid.foo")
    );

    Assert.fail();
  }

  private List<Map<String, Object>> doPost(final SqlQuery query) throws Exception
  {
    final Response response = resource.doPost(query);
    if (response.getStatus() == 200) {
      final StreamingOutput output = (StreamingOutput) response.getEntity();
      final ByteArrayOutputStream baos = new ByteArrayOutputStream();
      output.write(baos);
      return JSON_MAPPER.readValue(
          baos.toByteArray(),
          new TypeReference<List<Map<String, Object>>>()
          {
          }
      );
    } else {
      throw JSON_MAPPER.readValue((byte[]) response.getEntity(), QueryInterruptedException.class);
    }
  }
}
