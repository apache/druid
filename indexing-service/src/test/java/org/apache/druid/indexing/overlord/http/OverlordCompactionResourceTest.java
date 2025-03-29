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

package org.apache.druid.indexing.overlord.http;

import com.google.common.util.concurrent.Futures;
import org.apache.druid.client.coordinator.CoordinatorClient;
import org.apache.druid.error.DruidExceptionMatcher;
import org.apache.druid.error.ErrorResponse;
import org.apache.druid.indexing.compact.CompactionScheduler;
import org.apache.druid.indexing.compact.CompactionSupervisorSpec;
import org.apache.druid.indexing.overlord.supervisor.SupervisorResource;
import org.apache.druid.indexing.overlord.supervisor.SupervisorStatus;
import org.apache.druid.segment.TestDataSource;
import org.apache.druid.server.compaction.CompactionStatusResponse;
import org.apache.druid.server.coordinator.AutoCompactionSnapshot;
import org.apache.druid.server.coordinator.CompactionConfigValidationResult;
import org.apache.druid.server.coordinator.DataSourceCompactionConfig;
import org.easymock.EasyMock;
import org.hamcrest.MatcherAssert;
import org.joda.time.Period;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Response;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

public class OverlordCompactionResourceTest
{
  private final Random random = new Random();

  private CompactionScheduler scheduler;
  private OverlordCompactionResource compactionResource;
  private CoordinatorClient coordinatorClient;
  private SupervisorResource supervisorResource;
  private HttpServletRequest request;
  private DataSourceCompactionConfig wikiConfig;

  private final AtomicBoolean useSupervisors = new AtomicBoolean(false);

  /**
   * Mock instance of CompactionScheduler used only for validating compaction configs.
   */
  private CompactionScheduler validator;

  @Before
  public void setUp()
  {
    useSupervisors.set(true);
    scheduler = EasyMock.createStrictMock(CompactionScheduler.class);
    EasyMock.expect(scheduler.isEnabled()).andAnswer(useSupervisors::get).anyTimes();

    coordinatorClient = EasyMock.createStrictMock(CoordinatorClient.class);
    supervisorResource = EasyMock.createStrictMock(SupervisorResource.class);

    request = EasyMock.mock(HttpServletRequest.class);

    compactionResource = new OverlordCompactionResource(
        scheduler,
        coordinatorClient,
        supervisorResource
    );

    wikiConfig = DataSourceCompactionConfig
        .builder()
        .forDataSource(TestDataSource.WIKI)
        .withTaskPriority(random.nextInt(100))
        .withSkipOffsetFromLatest(Period.days(random.nextInt(5)))
        .build();

    validator = EasyMock.createStrictMock(CompactionScheduler.class);
    EasyMock.expect(validator.validateCompactionConfig(EasyMock.anyObject()))
            .andReturn(CompactionConfigValidationResult.success())
            .anyTimes();
    EasyMock.replay(validator);
  }

  @After
  public void tearDown()
  {
    EasyMock.verify(validator, scheduler, coordinatorClient, supervisorResource);
  }

  private void replayAll()
  {
    EasyMock.replay(scheduler, coordinatorClient, supervisorResource);
  }

  @Test
  public void test_getDatasourceCompactionSnapshot_returnsInvalidInput_ifDatasourceIsNullOrEmpty()
  {
    replayAll();

    verifyInvalidInputResponse(
        compactionResource.getDatasourceCompactionSnapshot(""),
        "No DataSource specified"
    );
    verifyInvalidInputResponse(
        compactionResource.getDatasourceCompactionSnapshot(null),
        "No DataSource specified"
    );
  }

  @Test
  public void test_getDatasourceCompactionSnapshot()
  {
    final AutoCompactionSnapshot snapshot = AutoCompactionSnapshot.builder(TestDataSource.WIKI).build();

    EasyMock.expect(scheduler.getCompactionSnapshot(TestDataSource.WIKI))
            .andReturn(snapshot).once();
    replayAll();

    final Response response = compactionResource.getDatasourceCompactionSnapshot(TestDataSource.WIKI);
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(snapshot, response.getEntity());
  }

  @Test
  public void test_getDatasourceCompactionSnapshot_returnsNotFound_withInvalidDatasource()
  {
    EasyMock.expect(scheduler.getCompactionSnapshot(TestDataSource.KOALA))
            .andReturn(null).once();
    replayAll();

    final Response response = compactionResource.getDatasourceCompactionSnapshot(TestDataSource.KOALA);
    Assert.assertEquals(404, response.getStatus());
  }

  @Test
  public void test_getDatasourceCompactionSnapshot_redirectsToCoordinator_ifSchedulerIsDisabled()
  {
    useSupervisors.set(false);

    final AutoCompactionSnapshot snapshot =
        AutoCompactionSnapshot.builder(TestDataSource.WIKI).build();
    EasyMock.expect(coordinatorClient.getCompactionSnapshots(TestDataSource.WIKI))
            .andReturn(Futures.immediateFuture(new CompactionStatusResponse(List.of(snapshot))));
    replayAll();

    final Response response = compactionResource.getDatasourceCompactionSnapshot(TestDataSource.WIKI);
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(snapshot, response.getEntity());
  }

  @Test
  public void test_getAllCompactionSnapshots()
  {
    final AutoCompactionSnapshot snapshot =
        AutoCompactionSnapshot.builder(TestDataSource.WIKI).build();
    EasyMock.expect(scheduler.getAllCompactionSnapshots())
            .andReturn(Map.of(TestDataSource.WIKI, snapshot)).anyTimes();
    replayAll();

    final Response response = compactionResource.getAllCompactionSnapshots();
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(new CompactionStatusResponse(List.of(snapshot)), response.getEntity());
  }

  @Test
  public void test_getDatasourceCompactionConfig()
  {
    EasyMock.expect(supervisorResource.specGet("autocompact__" + TestDataSource.WIKI))
            .andReturn(Response.ok(new CompactionSupervisorSpec(wikiConfig, false, validator)).build())
            .anyTimes();
    replayAll();

    final Response response = compactionResource.getDatasourceCompactionConfig(TestDataSource.WIKI);
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(wikiConfig, response.getEntity());
  }

  @Test
  public void test_getDatasourceCompactionConfig_returnsInvalidInput_ifDatasourceIsNullOrEmpty()
  {
    replayAll();

    verifyInvalidInputResponse(
        compactionResource.getDatasourceCompactionConfig(""),
        "No DataSource specified"
    );
    verifyInvalidInputResponse(
        compactionResource.getDatasourceCompactionConfig(null),
        "No DataSource specified"
    );
  }

  @Test
  public void test_updateDatasourceCompactionConfig()
  {
    final CompactionSupervisorSpec supervisorSpec =
        new CompactionSupervisorSpec(wikiConfig, false, validator);
    EasyMock.expect(supervisorResource.updateSupervisorSpec(supervisorSpec, true, request))
            .andReturn(Response.ok().build()).once();
    EasyMock.expect(scheduler.validateCompactionConfig(wikiConfig))
            .andReturn(CompactionConfigValidationResult.success()).once();
    replayAll();

    final Response response = compactionResource
        .updateDatasourceCompactionConfig(TestDataSource.WIKI, wikiConfig, request);
    Assert.assertEquals(200, response.getStatus());
  }

  @Test
  public void test_updateDatasourceCompactionConfig_returnsInvalidInput_ifDatasourceIsNullOrEmpty()
  {
    replayAll();

    verifyInvalidInputResponse(
        compactionResource.updateDatasourceCompactionConfig("", wikiConfig, request),
        "No DataSource specified"
    );
    verifyInvalidInputResponse(
        compactionResource.updateDatasourceCompactionConfig(null, wikiConfig, request),
        "No DataSource specified"
    );
  }

  @Test
  public void test_deleteDatasourceCompactionConfig()
  {
    EasyMock.expect(supervisorResource.terminate("autocompact__" + TestDataSource.WIKI))
            .andReturn(Response.ok().build()).once();
    replayAll();

    final Response response = compactionResource
        .deleteDatasourceCompactionConfig(TestDataSource.WIKI, request);
    Assert.assertEquals(200, response.getStatus());
  }

  @Test
  public void test_deleteDatasourceCompactionConfig_returnsInvalidInput_ifDatasourceIsNullOrEmpty()
  {
    replayAll();

    verifyInvalidInputResponse(
        compactionResource.deleteDatasourceCompactionConfig("", request),
        "No DataSource specified"
    );
    verifyInvalidInputResponse(
        compactionResource.deleteDatasourceCompactionConfig(null, request),
        "No DataSource specified"
    );
  }

  @Test
  public void test_getAllCompactionConfigs()
  {
    final List<SupervisorStatus> supervisors = List.of(
        new SupervisorStatus.Builder()
            .withId("autocompact__" + TestDataSource.WIKI)
            .withType("autocompact")
            .withSpec(new CompactionSupervisorSpec(wikiConfig, false, validator))
            .build()
    );

    EasyMock.expect(supervisorResource.specGetAll("includeSpec", true, "includeType", request))
            .andReturn(Response.ok(supervisors).build())
            .once();
    replayAll();

    final Response response = compactionResource.getAllCompactionConfigs(request);
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(new CompactionConfigsResponse(List.of(wikiConfig)), response.getEntity());
  }

  private void verifyInvalidInputResponse(Response response, String message)
  {
    Assert.assertEquals(400, response.getStatus());
    Assert.assertTrue(response.getEntity() instanceof ErrorResponse);
    MatcherAssert.assertThat(
        ((ErrorResponse) response.getEntity()).getUnderlyingException(),
        DruidExceptionMatcher.invalidInput().expectMessageIs(message)
    );
  }
}
