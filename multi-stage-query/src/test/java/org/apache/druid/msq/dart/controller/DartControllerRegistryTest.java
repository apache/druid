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

package org.apache.druid.msq.dart.controller;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.error.DruidException;
import org.apache.druid.indexer.report.TaskReport;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.msq.dart.guice.DartControllerConfig;
import org.apache.druid.msq.exec.Controller;
import org.apache.druid.msq.exec.ControllerContext;
import org.apache.druid.msq.indexing.report.MSQTaskReport;
import org.apache.druid.msq.indexing.report.MSQTaskReportPayload;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.security.AuthenticationResult;
import org.joda.time.Period;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.util.Collections;

public class DartControllerRegistryTest
{
  private static final String AUTHENTICATOR_NAME = "authn";

  private AutoCloseable mockCloser;

  @Mock
  private MSQTaskReportPayload reportPayload;

  @BeforeEach
  public void setUp()
  {
    mockCloser = MockitoAnnotations.openMocks(this);
  }

  @AfterEach
  public void tearDown() throws Exception
  {
    mockCloser.close();
  }

  @Test
  public void test_register_addsToRegistry()
  {
    final DartControllerRegistry registry = new DartControllerRegistry(makeConfig(0, Period.ZERO));
    final ControllerHolder holder = makeControllerHolder("dart1", "sql1", "user1");

    registry.register(holder);

    Assertions.assertEquals(1, registry.getAllControllers().size());
    Assertions.assertSame(holder, registry.getController("dart1"));
  }

  @Test
  public void test_register_duplicateThrows()
  {
    final DartControllerRegistry registry = new DartControllerRegistry(makeConfig(0, Period.ZERO));
    final ControllerHolder holder1 = makeControllerHolder("dart1", "sql1", "user1");
    final ControllerHolder holder2 = makeControllerHolder("dart1", "sql2", "user2");

    registry.register(holder1);

    Assertions.assertThrows(DruidException.class, () -> registry.register(holder2));
  }

  @Test
  public void test_deregister_noReport_removesFromRegistry()
  {
    final DartControllerRegistry registry = new DartControllerRegistry(makeConfig(10, Period.hours(1)));
    final ControllerHolder holder = makeControllerHolder("dart1", "sql1", "user1");

    registry.register(holder);
    registry.deregister(holder, null);

    Assertions.assertEquals(0, registry.getAllControllers().size());
    Assertions.assertNull(registry.getController("dart1"));
    Assertions.assertNull(registry.getQueryInfoAndReport("dart1"));
    Assertions.assertNull(registry.getQueryInfoAndReportBySqlQueryId("sql1"));
  }

  @Test
  public void test_deregister_withReport_retainsReport()
  {
    final DartControllerRegistry registry = new DartControllerRegistry(makeConfig(10, Period.hours(1)));
    final ControllerHolder holder = makeControllerHolder("dart1", "sql1", "user1");
    final TaskReport report = new MSQTaskReport("dart1", reportPayload);

    final TaskReport.ReportMap reportMap = new TaskReport.ReportMap();
    reportMap.put(MSQTaskReport.REPORT_KEY, report);
    registry.register(holder);
    registry.deregister(holder, reportMap);

    // Controller is removed
    Assertions.assertEquals(0, registry.getAllControllers().size());
    Assertions.assertNull(registry.getController("dart1"));

    // But report is retained
    final QueryInfoAndReport infoAndReport = registry.getQueryInfoAndReport("dart1");
    Assertions.assertNotNull(infoAndReport);
    Assertions.assertEquals("dart1", infoAndReport.getQueryInfo().getDartQueryId());
    Assertions.assertSame(report, infoAndReport.getReportMap());

    // And can be looked up by SQL query ID
    final QueryInfoAndReport infoAndReportBySql = registry.getQueryInfoAndReportBySqlQueryId("sql1");
    Assertions.assertNotNull(infoAndReportBySql);
    Assertions.assertEquals("dart1", infoAndReportBySql.getQueryInfo().getDartQueryId());
    Assertions.assertEquals("sql1", infoAndReportBySql.getQueryInfo().getSqlQueryId());
  }

  @Test
  public void test_deregister_withReport_zeroRetainedCount_doesNotRetainReport()
  {
    final DartControllerRegistry registry = new DartControllerRegistry(makeConfig(0, Period.hours(1)));
    final ControllerHolder holder = makeControllerHolder("dart1", "sql1", "user1");
    final TaskReport report = new MSQTaskReport("dart1", reportPayload);

    final TaskReport.ReportMap reportMap = new TaskReport.ReportMap();
    reportMap.put(MSQTaskReport.REPORT_KEY, report);
    registry.register(holder);
    registry.deregister(holder, reportMap);

    Assertions.assertNull(registry.getQueryInfoAndReport("dart1"));
    Assertions.assertNull(registry.getQueryInfoAndReportBySqlQueryId("sql1"));
  }

  @Test
  public void test_getQueryInfoAndReport_runningQuery()
  {
    final DartControllerRegistry registry = new DartControllerRegistry(makeConfig(10, Period.hours(1)));
    final Controller controller = Mockito.mock(Controller.class);
    final ControllerContext controllerContext = Mockito.mock(ControllerContext.class);
    Mockito.when(controller.queryId()).thenReturn("dart1");
    Mockito.when(controller.getControllerContext()).thenReturn(controllerContext);
    Mockito.when(controllerContext.selfNode()).thenReturn(Mockito.mock(DruidNode.class));

    // Set up live reports
    final TaskReport.ReportMap liveReportMap = new TaskReport.ReportMap();
    liveReportMap.put(MSQTaskReport.REPORT_KEY, new MSQTaskReport("dart1", reportPayload));
    Mockito.when(controller.liveReports()).thenReturn(liveReportMap);

    final ControllerHolder holder = new ControllerHolder(
        controller,
        "sql1",
        "SELECT 1",
        makeAuthenticationResult("user1"),
        DateTimes.of("2000")
    );

    registry.register(holder);

    final QueryInfoAndReport infoAndReport = registry.getQueryInfoAndReport("dart1");
    Assertions.assertNotNull(infoAndReport);
    Assertions.assertEquals("dart1", infoAndReport.getQueryInfo().getDartQueryId());

    // Also works by SQL query ID
    final QueryInfoAndReport infoAndReportBySql = registry.getQueryInfoAndReportBySqlQueryId("sql1");
    Assertions.assertNotNull(infoAndReportBySql);
    Assertions.assertEquals("dart1", infoAndReportBySql.getQueryInfo().getDartQueryId());

    registry.deregister(holder, null);
  }

  @Test
  public void test_getQueryInfoAndReport_runningQuery_noLiveReports()
  {
    final DartControllerRegistry registry = new DartControllerRegistry(makeConfig(10, Period.hours(1)));
    final Controller controller = Mockito.mock(Controller.class);
    final ControllerContext controllerContext = Mockito.mock(ControllerContext.class);
    Mockito.when(controller.queryId()).thenReturn("dart1");
    Mockito.when(controller.getControllerContext()).thenReturn(controllerContext);
    Mockito.when(controllerContext.selfNode()).thenReturn(Mockito.mock(DruidNode.class));
    Mockito.when(controller.liveReports()).thenReturn(null);

    final ControllerHolder holder = new ControllerHolder(
        controller,
        "sql1",
        "SELECT 1",
        makeAuthenticationResult("user1"),
        DateTimes.of("2000")
    );

    registry.register(holder);

    // Returns null when no live reports are available
    Assertions.assertNull(registry.getQueryInfoAndReport("dart1"));

    // But the sqlQueryId mapping should still work after deregister with report
    registry.deregister(holder, null);
    Assertions.assertNull(registry.getQueryInfoAndReportBySqlQueryId("sql1"));
  }

  @Test
  public void test_reportEviction_byCount()
  {
    final DartControllerRegistry registry = new DartControllerRegistry(makeConfig(2, Period.hours(1)));

    // Register and deregister 3 queries with reports
    for (int i = 1; i <= 3; i++) {
      final ControllerHolder holder = makeControllerHolder("dart" + i, "sql" + i, "user1");
      final TaskReport.ReportMap reportMap = new TaskReport.ReportMap();
      reportMap.put(MSQTaskReport.REPORT_KEY, new MSQTaskReport("dart" + i, reportPayload));
      registry.register(holder);
      registry.deregister(holder, reportMap);
    }

    // Only the last 2 reports should be retained
    Assertions.assertNull(registry.getQueryInfoAndReport("dart1"));
    Assertions.assertNull(registry.getQueryInfoAndReportBySqlQueryId("sql1"));

    Assertions.assertNotNull(registry.getQueryInfoAndReport("dart2"));
    Assertions.assertNotNull(registry.getQueryInfoAndReportBySqlQueryId("sql2"));

    Assertions.assertNotNull(registry.getQueryInfoAndReport("dart3"));
    Assertions.assertNotNull(registry.getQueryInfoAndReportBySqlQueryId("sql3"));
  }

  @Test
  public void test_getQueryInfoAndReportBySqlQueryId_notFound()
  {
    final DartControllerRegistry registry = new DartControllerRegistry(makeConfig(10, Period.hours(1)));

    Assertions.assertNull(registry.getQueryInfoAndReportBySqlQueryId("nonexistent"));
  }

  @Test
  public void test_getAllControllers()
  {
    final DartControllerRegistry registry = new DartControllerRegistry(makeConfig(10, Period.hours(1)));
    final ControllerHolder holder1 = makeControllerHolder("dart1", "sql1", "user1");
    final ControllerHolder holder2 = makeControllerHolder("dart2", "sql2", "user2");

    registry.register(holder1);
    registry.register(holder2);

    Assertions.assertEquals(2, registry.getAllControllers().size());
    Assertions.assertTrue(registry.getAllControllers().contains(holder1));
    Assertions.assertTrue(registry.getAllControllers().contains(holder2));

    registry.deregister(holder1, null);
    registry.deregister(holder2, null);
  }

  private ControllerHolder makeControllerHolder(
      final String dartQueryId,
      final String sqlQueryId,
      final String identity
  )
  {
    final Controller controller = Mockito.mock(Controller.class);
    final ControllerContext controllerContext = Mockito.mock(ControllerContext.class);
    Mockito.when(controller.queryId()).thenReturn(dartQueryId);
    Mockito.when(controller.getControllerContext()).thenReturn(controllerContext);
    Mockito.when(controllerContext.selfNode()).thenReturn(Mockito.mock(DruidNode.class));

    return new ControllerHolder(
        controller,
        sqlQueryId,
        "SELECT 1",
        makeAuthenticationResult(identity),
        DateTimes.of("2000")
    );
  }

  private static AuthenticationResult makeAuthenticationResult(final String identity)
  {
    return new AuthenticationResult(identity, null, AUTHENTICATOR_NAME, Collections.emptyMap());
  }

  private static DartControllerConfig makeConfig(
      final int maxRetainedReportCount,
      final Period maxRetainedReportDuration
  )
  {
    return new DartControllerConfig()
    {
      @Override
      @JsonProperty("maxRetainedReportCount")
      public int getMaxRetainedReportCount()
      {
        return maxRetainedReportCount;
      }

      @Override
      @JsonProperty("maxRetainedReportDuration")
      public Period getMaxRetainedReportDuration()
      {
        return maxRetainedReportDuration;
      }
    };
  }
}
