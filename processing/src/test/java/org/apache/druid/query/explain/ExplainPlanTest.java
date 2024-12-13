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

package org.apache.druid.query.explain;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.error.DruidException;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class ExplainPlanTest
{
  private static final ObjectMapper MAPPER = new DefaultObjectMapper();

  @Test
  public void testExplainPlanSerdeForSelectQuery() throws JsonProcessingException
  {
    final String plan = "[{\"query\":{\"queryType\":\"scan\",\"dataSource\":{\"type\":\"inline\",\"columnNames\":[\"EXPR$0\"],\"columnTypes\":[\"LONG\"],\"rows\":[[2]]},\"intervals\":{\"type\":\"intervals\",\"intervals\":[\"-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z\"]},\"resultFormat\":\"compactedList\",\"columns\":[\"EXPR$0\"],\"context\":{\"defaultTimeout\":300000,\"maxScatterGatherBytes\":9223372036854775807,\"sqlCurrentTimestamp\":\"2000-01-01T00:00:00Z\",\"sqlQueryId\":\"dummy\",\"vectorize\":\"false\",\"vectorizeVirtualColumns\":\"false\"},\"columnTypes\":[\"LONG\"],\"granularity\":{\"type\":\"all\"},\"legacy\":false},\"signature\":[{\"name\":\"EXPR$0\",\"type\":\"LONG\"}],\"columnMappings\":[{\"queryColumn\":\"EXPR$0\",\"outputColumn\":\"EXPR$0\"}]}]";
    final String resources = "[{\"name\":\"foo\",\"type\":\"DATASOURCE\"}]";
    final ExplainAttributes attributes = new ExplainAttributes("SELECT", null, null, null, null);

    final List<Map<String, Object>> givenPlans = ImmutableList.of(
        ImmutableMap.of(
            "PLAN",
            plan,
            "RESOURCES",
            resources,
            "ATTRIBUTES",
            MAPPER.writeValueAsString(attributes)
        )
    );

    testSerde(givenPlans, ImmutableList.of(new ExplainPlan(plan, resources, attributes)));
  }

  @Test
  public void testExplainPlanSerdeForReplaceQuery() throws JsonProcessingException
  {
    final String plan = "[{\"query\":{\"queryType\":\"scan\",\"dataSource\":{\"type\":\"external\",\"inputSource\":{\"type\":\"inline\",\"data\":\"a,b,1\\nc,d,2\\n\"},\"inputFormat\":{\"type\":\"csv\",\"columns\":[\"x\",\"y\",\"z\"]},\"signature\":[{\"name\":\"x\",\"type\":\"STRING\"},{\"name\":\"y\",\"type\":\"STRING\"},{\"name\":\"z\",\"type\":\"LONG\"}]},\"intervals\":{\"type\":\"intervals\",\"intervals\":[\"-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z\"]},\"resultFormat\":\"compactedList\",\"columns\":[\"x\",\"y\",\"z\"],\"context\":{\"defaultTimeout\":300000,\"maxScatterGatherBytes\":9223372036854775807,\"sqlCurrentTimestamp\":\"2000-01-01T00:00:00Z\",\"sqlInsertSegmentGranularity\":\"{\\\"type\\\":\\\"all\\\"}\",\"sqlQueryId\":\"dummy\",\"sqlReplaceTimeChunks\":\"all\",\"vectorize\":\"false\",\"vectorizeVirtualColumns\":\"false\"},\"columnTypes\":[\"STRING\",\"STRING\",\"LONG\"],\"granularity\":{\"type\":\"all\"},\"legacy\":false},\"signature\":[{\"name\":\"x\",\"type\":\"STRING\"},{\"name\":\"y\",\"type\":\"STRING\"},{\"name\":\"z\",\"type\":\"LONG\"}],\"columnMappings\":[{\"queryColumn\":\"x\",\"outputColumn\":\"x\"},{\"queryColumn\":\"y\",\"outputColumn\":\"y\"},{\"queryColumn\":\"z\",\"outputColumn\":\"z\"}]}]";
    final String resources = "[{\"name\":\"EXTERNAL\",\"type\":\"EXTERNAL\"},{\"name\":\"dst\",\"type\":\"DATASOURCE\"}]";
    final ExplainAttributes attributes = new ExplainAttributes("REPLACE", "dst", Granularities.ALL, null, "all");

    final List<Map<String, Object>> givenPlans = ImmutableList.of(
        ImmutableMap.of(
            "PLAN",
            plan,
            "RESOURCES",
            resources,
            "ATTRIBUTES",
            MAPPER.writeValueAsString(attributes)
        )
    );

    testSerde(givenPlans, ImmutableList.of(new ExplainPlan(plan, resources, attributes)));
  }

  @Test
  public void testExplainPlanSerdeForInsertQuery() throws JsonProcessingException
  {
    final String plan = "[{\"query\":{\"queryType\":\"scan\",\"dataSource\":{\"type\":\"external\",\"inputSource\":{\"type\":\"inline\",\"data\":\"a,b,1\\nc,d,2\\n\"},\"inputFormat\":{\"type\":\"csv\",\"columns\":[\"x\",\"y\",\"z\"]},\"signature\":[{\"name\":\"x\",\"type\":\"STRING\"},{\"name\":\"y\",\"type\":\"STRING\"},{\"name\":\"z\",\"type\":\"LONG\"}]},\"intervals\":{\"type\":\"intervals\",\"intervals\":[\"-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z\"]},\"resultFormat\":\"compactedList\",\"columns\":[\"x\",\"y\",\"z\"],\"context\":{\"defaultTimeout\":300000,\"maxScatterGatherBytes\":9223372036854775807,\"sqlCurrentTimestamp\":\"2000-01-01T00:00:00Z\",\"sqlInsertSegmentGranularity\":\"{\\\"type\\\":\\\"all\\\"}\",\"sqlQueryId\":\"dummy\",\"sqlReplaceTimeChunks\":\"all\",\"vectorize\":\"false\",\"vectorizeVirtualColumns\":\"false\"},\"columnTypes\":[\"STRING\",\"STRING\",\"LONG\"],\"granularity\":{\"type\":\"all\"},\"legacy\":false},\"signature\":[{\"name\":\"x\",\"type\":\"STRING\"},{\"name\":\"y\",\"type\":\"STRING\"},{\"name\":\"z\",\"type\":\"LONG\"}],\"columnMappings\":[{\"queryColumn\":\"x\",\"outputColumn\":\"x\"},{\"queryColumn\":\"y\",\"outputColumn\":\"y\"},{\"queryColumn\":\"z\",\"outputColumn\":\"z\"}]}]";
    final String resources = "[{\"name\":\"dst\",\"type\":\"DATASOURCE\"},{\"name\":\"foo\",\"type\":\"DATASOURCE\"}]";
    final ExplainAttributes attributes = new ExplainAttributes("INSERT", "foo", Granularities.DAY, ImmutableList.of("floor_m1", "dim1", "CEIL(\"m2\")"), null);

    final List<Map<String, Object>> givenPlans = ImmutableList.of(
        ImmutableMap.of(
            "PLAN",
            plan,
            "RESOURCES",
            resources,
            "ATTRIBUTES",
            MAPPER.writeValueAsString(attributes)
        )
    );

    testSerde(givenPlans, ImmutableList.of(new ExplainPlan(plan, resources, attributes)));
  }

  private void testSerde(
      final List<Map<String, Object>> givenPlans,
      final List<ExplainPlan> expectedExplainPlans
  )
  {
    final List<ExplainPlan> observedExplainPlans;
    try {
      observedExplainPlans = MAPPER.readValue(
          MAPPER.writeValueAsString(givenPlans),
          new TypeReference<>() {}
      );
    }
    catch (Exception e) {
      throw DruidException.defensive(e, "Error deserializing given plans[%s] into explain plans.", givenPlans);
    }
    assertEquals(expectedExplainPlans, observedExplainPlans);
  }
}
