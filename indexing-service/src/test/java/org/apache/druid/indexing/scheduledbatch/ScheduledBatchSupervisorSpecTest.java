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

package org.apache.druid.indexing.scheduledbatch;

import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import org.apache.druid.client.broker.BrokerClient;
import org.apache.druid.error.DruidException;
import org.apache.druid.error.DruidExceptionMatcher;
import org.apache.druid.guice.SupervisorModule;
import org.apache.druid.indexing.overlord.supervisor.SupervisorSpec;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.explain.ExplainAttributes;
import org.apache.druid.query.explain.ExplainPlan;
import org.apache.druid.query.http.ClientSqlQuery;
import org.hamcrest.MatcherAssert;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class ScheduledBatchSupervisorSpecTest
{
  private static final ObjectMapper OBJECT_MAPPER = new DefaultObjectMapper();
  private BrokerClient brokerClient;
  private ScheduledBatchTaskManager scheduler;
  private ClientSqlQuery query;

  @Before
  public void setUp()
  {
    brokerClient = Mockito.mock(BrokerClient.class);
    scheduler = Mockito.mock(ScheduledBatchTaskManager.class);

    OBJECT_MAPPER.setInjectableValues(
        new InjectableValues.Std()
            .addValue(BrokerClient.class, brokerClient)
            .addValue(ObjectMapper.class, OBJECT_MAPPER)
            .addValue(ScheduledBatchTaskManager.class, scheduler)
    );
    OBJECT_MAPPER.registerModules(
        new SupervisorModule().getJacksonModules()
    );

    query = new ClientSqlQuery(
        "REPLACE INTO foo OVERWRITE ALL SELECT TIME_PARSE(ts) AS __time, c1 FROM (VALUES('2023-01-01', 'insert_1'), ('2023-01-01', 'insert_2'), ('2023-02-01', 'insert3')) AS t(ts, c1) PARTITIONED BY ALL ",
        null,
        false,
        false,
        false,
        null,
        null
    );

    final ExplainPlan explainPlanInfo = new ExplainPlan(
        "",
        "",
        new ExplainAttributes("REPLACE", "foo", Granularities.ALL, null, null)
    );
    Mockito.when(brokerClient.fetchExplainPlan(query))
           .thenReturn(Futures.immediateFuture(ImmutableList.of(explainPlanInfo)));
  }

  @Test
  public void testSerdeOfActiveSpec()
  {
    testSerde(
        new ScheduledBatchSupervisorSpec(
            query,
            new UnixCronSchedulerConfig("* * * * *"),
            false,
            null,
            null,
            OBJECT_MAPPER,
            scheduler,
            brokerClient
        )
    );
  }

  @Test
  public void testSerdeOfSuspendedSpec()
  {
    testSerde(
        new ScheduledBatchSupervisorSpec(
            query,
            new UnixCronSchedulerConfig("@daily"),
            true,
            "foo",
            "boo",
            OBJECT_MAPPER,
            scheduler,
            brokerClient
        )
    );
  }

  @Test
  public void testGetIdAndDataSources()
  {
    final ScheduledBatchSupervisorSpec activeSpec = new ScheduledBatchSupervisorSpec(
        query,
        new UnixCronSchedulerConfig("* * * * *"),
        false,
        null,
        null,
        OBJECT_MAPPER,
        scheduler,
        brokerClient
    );
    assertTrue(activeSpec.getId().startsWith(ScheduledBatchSupervisorSpec.TYPE));
    assertEquals(Collections.singletonList("foo"), activeSpec.getDataSources());
    assertFalse(activeSpec.isSuspended());
  }

  @Test
  public void testCreateSuspendedSpec()
  {
    final ScheduledBatchSupervisorSpec activeSpec = new ScheduledBatchSupervisorSpec(
        query,
        new UnixCronSchedulerConfig("* * * * *"),
        false,
        null,
        null,
        OBJECT_MAPPER,
        scheduler,
        brokerClient
    );
    assertFalse(activeSpec.isSuspended());

    final ScheduledBatchSupervisorSpec suspendedSpec = activeSpec.createSuspendedSpec();
    assertTrue(suspendedSpec.isSuspended());
    assertEquals(activeSpec.getId(), suspendedSpec.getId());
    assertEquals(activeSpec.getSpec(), suspendedSpec.getSpec());
    assertEquals(activeSpec.getDataSources(), suspendedSpec.getDataSources());
  }

  @Test
  public void testCreateRunningSpec()
  {
    final ScheduledBatchSupervisorSpec suspendedSpec = new ScheduledBatchSupervisorSpec(
        query,
        new UnixCronSchedulerConfig("* * * * *"),
        true,
        null,
        null,
        OBJECT_MAPPER,
        scheduler,
        brokerClient
    );
    assertTrue(suspendedSpec.isSuspended());

    final ScheduledBatchSupervisorSpec activeSpec = suspendedSpec.createRunningSpec();
    assertFalse(activeSpec.isSuspended());
    assertEquals(activeSpec.getId(), suspendedSpec.getId());
    assertEquals(activeSpec.getSpec(), suspendedSpec.getSpec());
    assertEquals(activeSpec.getDataSources(), suspendedSpec.getDataSources());
  }

  @Test
  public void testCreateSupervisorWithSelectQuery()
  {
    query = new ClientSqlQuery(
        "SELECT TIME_PARSE(ts) AS __time, c1 FROM (VALUES('2023-01-01', 'insert_1'), ('2023-01-01', 'insert_2'), ('2023-02-01', 'insert3')) AS t(ts, c1) PARTITIONED BY ALL ",
        null,
        false,
        false,
        false,
        null,
        null
    );

    Mockito.when(brokerClient.fetchExplainPlan(query))
           .thenReturn(Futures.immediateFuture(ImmutableList.of(
               new ExplainPlan(
                   "",
                   "",
                   new ExplainAttributes("SELECT", null, null, null, null)
               ))
           ));

    MatcherAssert.assertThat(
        assertThrows(
            DruidException.class,
            () -> new ScheduledBatchSupervisorSpec(
              query,
              new UnixCronSchedulerConfig("* * * * *"),
              true,
              null,
              null,
              OBJECT_MAPPER,
              scheduler,
              brokerClient
            )
        ),
        DruidExceptionMatcher.invalidInput().expectMessageIs(
            "SELECT queries are not supported by the [scheduled_batch] supervisor. Only INSERT or REPLACE ingest queries are allowed."
        )
    );
  }

  @Test
  public void test_getInputSourceResources_returnsEmpty()
  {
    final ScheduledBatchSupervisorSpec supervisorSpec = new ScheduledBatchSupervisorSpec(
        query,
        new UnixCronSchedulerConfig("* * * * *"),
        true,
        null,
        null,
        OBJECT_MAPPER,
        scheduler,
        brokerClient
    );
    Assert.assertTrue(supervisorSpec.getInputSourceResources().isEmpty());
  }

  private void testSerde(final ScheduledBatchSupervisorSpec spec)
  {
    try {
      final String json = OBJECT_MAPPER.writeValueAsString(spec);
      final SupervisorSpec deserialized = OBJECT_MAPPER.readValue(json, SupervisorSpec.class);
      assertTrue(deserialized instanceof ScheduledBatchSupervisorSpec);

      final ScheduledBatchSupervisorSpec observedSpec = (ScheduledBatchSupervisorSpec) deserialized;
      assertEquals(spec.isSuspended(), observedSpec.isSuspended());
      assertEquals(spec.getSpec(), observedSpec.getSpec());
      assertEquals(spec.getId(), observedSpec.getId());
      assertEquals(spec.getDataSources(), observedSpec.getDataSources());
    }
    catch (Exception e) {
      throw DruidException.defensive(e, "Error while performing serde of spec[%s].", spec);
    }
  }
}
