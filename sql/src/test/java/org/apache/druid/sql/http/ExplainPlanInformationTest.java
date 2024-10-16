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

package org.apache.druid.sql.http;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.error.DruidException;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.sql.calcite.planner.ExplainAttributes;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class ExplainPlanInformationTest
{
  private static final ObjectMapper MAPPER = new DefaultObjectMapper();

  @Test
  public void testExplainPlanSerdeofInsertQuery()
  {
    final String insertPlan = "[{\"PLAN\":\"[{\\\"query\\\":{\\\"queryType\\\":\\\"scan\\\",\\\"dataSource\\\":{\\\"type\\\":\\\"inline\\\",\\\"columnNames\\\":[\\\"__time\\\",\\\"c1\\\"],\\\"columnTypes\\\":[\\\"LONG\\\",\\\"STRING\\\"],\\\"rows\\\":[[1672531200000,\\\"insert_1\\\"],[1672531200000,\\\"insert_2\\\"],[1675209600000,\\\"insert3\\\"]]},\\\"intervals\\\":{\\\"type\\\":\\\"intervals\\\",\\\"intervals\\\":[\\\"-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z\\\"]},\\\"resultFormat\\\":\\\"compactedList\\\",\\\"columns\\\":[\\\"__time\\\",\\\"c1\\\"],\\\"context\\\":{\\\"scanSignature\\\":\\\"[{\\\\\\\"name\\\\\\\":\\\\\\\"__time\\\\\\\",\\\\\\\"type\\\\\\\":\\\\\\\"LONG\\\\\\\"},{\\\\\\\"name\\\\\\\":\\\\\\\"c1\\\\\\\",\\\\\\\"type\\\\\\\":\\\\\\\"STRING\\\\\\\"}]\\\",\\\"sqlInsertSegmentGranularity\\\":\\\"{\\\\\\\"type\\\\\\\":\\\\\\\"all\\\\\\\"}\\\",\\\"sqlQueryId\\\":\\\"0ec77025-6b0c-42e2-9320-8a26e69f3e4d\\\"},\\\"columnTypes\\\":[\\\"LONG\\\",\\\"STRING\\\"],\\\"granularity\\\":{\\\"type\\\":\\\"all\\\"},\\\"legacy\\\":false},\\\"signature\\\":[{\\\"name\\\":\\\"__time\\\",\\\"type\\\":\\\"LONG\\\"},{\\\"name\\\":\\\"c1\\\",\\\"type\\\":\\\"STRING\\\"}],\\\"columnMappings\\\":[{\\\"queryColumn\\\":\\\"__time\\\",\\\"outputColumn\\\":\\\"__time\\\"},{\\\"queryColumn\\\":\\\"c1\\\",\\\"outputColumn\\\":\\\"c1\\\"}]}]\",\"RESOURCES\":\"[{\\\"name\\\":\\\"demo1\\\",\\\"type\\\":\\\"DATASOURCE\\\"}]\",\"ATTRIBUTES\":\"{\\\"statementType\\\":\\\"INSERT\\\",\\\"targetDataSource\\\":\\\"demo1\\\",\\\"partitionedBy\\\":{\\\"type\\\":\\\"all\\\"}}\"}]";
    final List<ExplainPlanInformation> explainPlanInfos = deserializeExplainPlan(insertPlan);
    assertEquals(1, explainPlanInfos.size());

    final ExplainPlanInformation explainPlanInformation = explainPlanInfos.get(0);
    assertEquals(
        "[{\"query\":{\"queryType\":\"scan\",\"dataSource\":{\"type\":\"inline\",\"columnNames\":[\"__time\",\"c1\"],\"columnTypes\":[\"LONG\",\"STRING\"],\"rows\":[[1672531200000,\"insert_1\"],[1672531200000,\"insert_2\"],[1675209600000,\"insert3\"]]},\"intervals\":{\"type\":\"intervals\",\"intervals\":[\"-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z\"]},\"resultFormat\":\"compactedList\",\"columns\":[\"__time\",\"c1\"],\"context\":{\"scanSignature\":\"[{\\\"name\\\":\\\"__time\\\",\\\"type\\\":\\\"LONG\\\"},{\\\"name\\\":\\\"c1\\\",\\\"type\\\":\\\"STRING\\\"}]\",\"sqlInsertSegmentGranularity\":\"{\\\"type\\\":\\\"all\\\"}\",\"sqlQueryId\":\"0ec77025-6b0c-42e2-9320-8a26e69f3e4d\"},\"columnTypes\":[\"LONG\",\"STRING\"],\"granularity\":{\"type\":\"all\"},\"legacy\":false},\"signature\":[{\"name\":\"__time\",\"type\":\"LONG\"},{\"name\":\"c1\",\"type\":\"STRING\"}],\"columnMappings\":[{\"queryColumn\":\"__time\",\"outputColumn\":\"__time\"},{\"queryColumn\":\"c1\",\"outputColumn\":\"c1\"}]}]",
        explainPlanInformation.getPlan()
    );
    assertEquals(
        "[{\"name\":\"demo1\",\"type\":\"DATASOURCE\"}]",
        explainPlanInformation.getResources()
    );
    assertEquals(
        new ExplainAttributes("INSERT", "demo1", Granularities.ALL, null, null),
        explainPlanInformation.getAttributes()
    );
  }

  @Test
  public void testExplainPlanSerdeOfSelectQuery()
  {
    final String selectPlan = "[{\"PLAN\":\"[{\\\"query\\\":{\\\"queryType\\\":\\\"scan\\\",\\\"dataSource\\\":{\\\"type\\\":\\\"inline\\\",\\\"columnNames\\\":[\\\"__time\\\",\\\"c1\\\"],\\\"columnTypes\\\":[\\\"LONG\\\",\\\"STRING\\\"],\\\"rows\\\":[[1672531200000,\\\"insert_1\\\"],[1672531200000,\\\"insert_2\\\"],[1675209600000,\\\"insert3\\\"]]},\\\"intervals\\\":{\\\"type\\\":\\\"intervals\\\",\\\"intervals\\\":[\\\"-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z\\\"]},\\\"resultFormat\\\":\\\"compactedList\\\",\\\"columns\\\":[\\\"__time\\\",\\\"c1\\\"],\\\"context\\\":{\\\"scanSignature\\\":\\\"[{\\\\\\\"name\\\\\\\":\\\\\\\"__time\\\\\\\",\\\\\\\"type\\\\\\\":\\\\\\\"LONG\\\\\\\"},{\\\\\\\"name\\\\\\\":\\\\\\\"c1\\\\\\\",\\\\\\\"type\\\\\\\":\\\\\\\"STRING\\\\\\\"}]\\\",\\\"sqlInsertSegmentGranularity\\\":\\\"{\\\\\\\"type\\\\\\\":\\\\\\\"all\\\\\\\"}\\\",\\\"sqlQueryId\\\":\\\"0ec77025-6b0c-42e2-9320-8a26e69f3e4d\\\"},\\\"columnTypes\\\":[\\\"LONG\\\",\\\"STRING\\\"],\\\"granularity\\\":{\\\"type\\\":\\\"all\\\"},\\\"legacy\\\":false},\\\"signature\\\":[{\\\"name\\\":\\\"__time\\\",\\\"type\\\":\\\"LONG\\\"},{\\\"name\\\":\\\"c1\\\",\\\"type\\\":\\\"STRING\\\"}],\\\"columnMappings\\\":[{\\\"queryColumn\\\":\\\"__time\\\",\\\"outputColumn\\\":\\\"__time\\\"},{\\\"queryColumn\\\":\\\"c1\\\",\\\"outputColumn\\\":\\\"c1\\\"}]}]\",\"RESOURCES\":\"[{\\\"name\\\":\\\"demo1\\\",\\\"type\\\":\\\"DATASOURCE\\\"}]\",\"ATTRIBUTES\":\"{\\\"statementType\\\":\\\"SELECT\\\"}\"}]";
    final List<ExplainPlanInformation> explainPlanInfos = deserializeExplainPlan(selectPlan);
    assertEquals(1, explainPlanInfos.size());

    final ExplainPlanInformation explainPlanInformation = explainPlanInfos.get(0);
    assertEquals(
        "[{\"query\":{\"queryType\":\"scan\",\"dataSource\":{\"type\":\"inline\",\"columnNames\":[\"__time\",\"c1\"],\"columnTypes\":[\"LONG\",\"STRING\"],\"rows\":[[1672531200000,\"insert_1\"],[1672531200000,\"insert_2\"],[1675209600000,\"insert3\"]]},\"intervals\":{\"type\":\"intervals\",\"intervals\":[\"-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z\"]},\"resultFormat\":\"compactedList\",\"columns\":[\"__time\",\"c1\"],\"context\":{\"scanSignature\":\"[{\\\"name\\\":\\\"__time\\\",\\\"type\\\":\\\"LONG\\\"},{\\\"name\\\":\\\"c1\\\",\\\"type\\\":\\\"STRING\\\"}]\",\"sqlInsertSegmentGranularity\":\"{\\\"type\\\":\\\"all\\\"}\",\"sqlQueryId\":\"0ec77025-6b0c-42e2-9320-8a26e69f3e4d\"},\"columnTypes\":[\"LONG\",\"STRING\"],\"granularity\":{\"type\":\"all\"},\"legacy\":false},\"signature\":[{\"name\":\"__time\",\"type\":\"LONG\"},{\"name\":\"c1\",\"type\":\"STRING\"}],\"columnMappings\":[{\"queryColumn\":\"__time\",\"outputColumn\":\"__time\"},{\"queryColumn\":\"c1\",\"outputColumn\":\"c1\"}]}]",
        explainPlanInformation.getPlan()
    );
    assertEquals(
        "[{\"name\":\"demo1\",\"type\":\"DATASOURCE\"}]",
        explainPlanInformation.getResources()
    );
    assertEquals(
        new ExplainAttributes("SELECT", null, null, null, null),
        explainPlanInformation.getAttributes()
    );
  }

  private List<ExplainPlanInformation> deserializeExplainPlan(final String plan)
  {
    final List<ExplainPlanInformation> explainPlanInfos;
    try {
      explainPlanInfos = MAPPER.readValue(plan, new TypeReference<List<ExplainPlanInformation>>() {});
    }
    catch (Exception e) {
      throw DruidException.defensive(e, "Error deserializing list of explain plan infos for plan[%s].", plan);
    }
    return explainPlanInfos;
  }

}
