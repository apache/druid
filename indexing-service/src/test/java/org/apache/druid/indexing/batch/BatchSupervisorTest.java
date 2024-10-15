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

package org.apache.druid.indexing.batch;

import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.discovery.BrokerClient;
import org.apache.druid.guice.SupervisorModule;
import org.apache.druid.indexing.overlord.supervisor.SupervisorReport;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.http.client.Request;
import org.apache.druid.sql.http.SqlQuery;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;

public class BatchSupervisorTest
{
  private static final ObjectMapper OBJECT_MAPPER = new DefaultObjectMapper();
  private BrokerClient brokerClient;
  private ScheduledBatchScheduler scheduler;
  private SqlQuery query;

  @Before
  public void setUp() throws Exception
  {
    brokerClient = Mockito.mock(BrokerClient.class);
    scheduler = Mockito.mock(ScheduledBatchScheduler.class);

    OBJECT_MAPPER.setInjectableValues(
        new InjectableValues.Std()
            .addValue(BrokerClient.class, brokerClient)
            .addValue(ObjectMapper.class, OBJECT_MAPPER)
            .addValue(ScheduledBatchScheduler.class, scheduler)
    );
    OBJECT_MAPPER.registerModules(
        new SupervisorModule().getJacksonModules()
    );

    query = new SqlQuery(
        "REPLACE INTO foo OVERWRITE ALL SELECT TIME_PARSE(ts) AS __time, c1 FROM (VALUES('2023-01-01', 'insert_1'), ('2023-01-01', 'insert_2'), ('2023-02-01', 'insert3')) AS t(ts, c1) PARTITIONED BY ALL ",
        null,
        false,
        false,
        false,
        null,
        null
    );

    final Request request = Mockito.mock(Request.class);
    Mockito.when(brokerClient.makeRequest(HttpMethod.POST, "/druid/v2/sql/task/"))
           .thenReturn(request);
    final String explainPlanResp = "[{\"PLAN\":\"[{\\\"query\\\":{\\\"queryType\\\":\\\"scan\\\",\\\"dataSource\\\":{\\\"type\\\":\\\"inline\\\",\\\"columnNames\\\":[\\\"__time\\\",\\\"c1\\\"],\\\"columnTypes\\\":[\\\"LONG\\\",\\\"STRING\\\"],\\\"rows\\\":[[1672531200000,\\\"insert_1\\\"],[1672531200000,\\\"insert_2\\\"],[1675209600000,\\\"insert3\\\"]]},\\\"intervals\\\":{\\\"type\\\":\\\"intervals\\\",\\\"intervals\\\":[\\\"-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z\\\"]},\\\"resultFormat\\\":\\\"compactedList\\\",\\\"columns\\\":[\\\"__time\\\",\\\"c1\\\"],\\\"context\\\":{\\\"scanSignature\\\":\\\"[{\\\\\\\"name\\\\\\\":\\\\\\\"__time\\\\\\\",\\\\\\\"type\\\\\\\":\\\\\\\"LONG\\\\\\\"},{\\\\\\\"name\\\\\\\":\\\\\\\"c1\\\\\\\",\\\\\\\"type\\\\\\\":\\\\\\\"STRING\\\\\\\"}]\\\",\\\"sqlInsertSegmentGranularity\\\":\\\"{\\\\\\\"type\\\\\\\":\\\\\\\"all\\\\\\\"}\\\",\\\"sqlQueryId\\\":\\\"4d3776b9-8b0d-4ebc-9952-b8db32a546bb\\\",\\\"sqlReplaceTimeChunks\\\":\\\"all\\\"},\\\"columnTypes\\\":[\\\"LONG\\\",\\\"STRING\\\"],\\\"granularity\\\":{\\\"type\\\":\\\"all\\\"},\\\"legacy\\\":false},\\\"signature\\\":[{\\\"name\\\":\\\"__time\\\",\\\"type\\\":\\\"LONG\\\"},{\\\"name\\\":\\\"c1\\\",\\\"type\\\":\\\"STRING\\\"}],\\\"columnMappings\\\":[{\\\"queryColumn\\\":\\\"__time\\\",\\\"outputColumn\\\":\\\"__time\\\"},{\\\"queryColumn\\\":\\\"c1\\\",\\\"outputColumn\\\":\\\"c1\\\"}]}]\",\"RESOURCES\":\"[{\\\"name\\\":\\\"foo\\\",\\\"type\\\":\\\"DATASOURCE\\\"}]\",\"ATTRIBUTES\":\"{\\\"statementType\\\":\\\"REPLACE\\\",\\\"targetDataSource\\\":\\\"foo\\\",\\\"partitionedBy\\\":{\\\"type\\\":\\\"all\\\"},\\\"replaceTimeChunks\\\":\\\"all\\\"}\"}]";
    Mockito.when(brokerClient.sendQuery(request)).thenReturn(explainPlanResp);
  }

  @Test
  public void testStartStopSupervisorForActiveSpec()
  {
    final BatchSupervisorSpec activeSpec = new BatchSupervisorSpec(
        query,
        new UnixCronSchedulerConfig("* * * * *"),
        false,
        null,
        null,
        OBJECT_MAPPER,
        scheduler,
        null
    );
    final BatchSupervisor supervisor = activeSpec.createSupervisor();
    assertEquals(BatchSupervisor.State.RUNNING, supervisor.getState());

    supervisor.start();
    supervisor.stop(false);

    Mockito.verify(scheduler, Mockito.times(1)).startScheduledIngestion(activeSpec.getId(), activeSpec.getSchedulerConfig(), activeSpec.getSpec());
    Mockito.verify(scheduler, Mockito.times(1)).stopScheduledIngestion(activeSpec.getId());
  }

  @Test
  public void testStartStopSupervisorForSuspendedSpec()
  {
    final BatchSupervisorSpec suspendedSpec = new BatchSupervisorSpec(
        query,
        new UnixCronSchedulerConfig("* * * * *"),
        true,
        null,
        null,
        OBJECT_MAPPER,
        scheduler,
        null
    );

    final BatchSupervisor supervisor = suspendedSpec.createSupervisor();
    assertEquals(BatchSupervisor.State.SUSPENDED, supervisor.getState());

    supervisor.start();
    supervisor.stop(false);

    Mockito.verify(scheduler, Mockito.times(2)).stopScheduledIngestion(suspendedSpec.getId());
  }

  @Test
  public void testGetStatus()
  {
    final BatchSupervisorSpec activeSpec = new BatchSupervisorSpec(
        query,
        new UnixCronSchedulerConfig("* * * * *"),
        false,
        null,
        null,
        OBJECT_MAPPER,
        scheduler,
        null
    );
    final BatchSupervisor supervisor = activeSpec.createSupervisor();
    final SupervisorReport<BatchSupervisorSnapshot> observedStatus = supervisor.getStatus();
    assertEquals(activeSpec.getId(), observedStatus.getId());
    Mockito.verify(scheduler, Mockito.times(1)).getSchedulerSnapshot(activeSpec.getId());
  }
}
