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

import com.google.common.base.Optional;
import com.google.common.util.concurrent.Futures;
import org.apache.druid.audit.AuditManager;
import org.apache.druid.client.coordinator.CoordinatorClient;
import org.apache.druid.error.DruidExceptionMatcher;
import org.apache.druid.error.ErrorResponse;
import org.apache.druid.indexer.CompactionEngine;
import org.apache.druid.indexing.compact.CompactionScheduler;
import org.apache.druid.indexing.compact.CompactionSupervisorSpec;
import org.apache.druid.indexing.overlord.TaskMaster;
import org.apache.druid.indexing.overlord.supervisor.CompactionSupervisorManager;
import org.apache.druid.indexing.overlord.supervisor.SupervisorManager;
import org.apache.druid.indexing.overlord.supervisor.VersionedSupervisorSpec;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.segment.TestDataSource;
import org.apache.druid.server.compaction.CompactionStatusResponse;
import org.apache.druid.server.coordinator.AutoCompactionSnapshot;
import org.apache.druid.server.coordinator.ClusterCompactionConfig;
import org.apache.druid.server.coordinator.CompactionConfigValidationResult;
import org.apache.druid.server.coordinator.CoordinatorConfigManager;
import org.apache.druid.server.coordinator.DataSourceCompactionConfig;
import org.apache.druid.server.coordinator.DataSourceCompactionConfigAuditEntry;
import org.apache.druid.server.coordinator.InlineSchemaDataSourceCompactionConfig;
import org.apache.druid.server.security.AllowAllAuthorizer;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.AuthorizerMapper;
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
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

public class OverlordCompactionResourceTest
{
  private final Random random = new Random(1000);

  private CompactionScheduler scheduler;
  private OverlordCompactionResource compactionResource;

  private CoordinatorClient coordinatorClient;
  private CoordinatorConfigManager configManager;

  private HttpServletRequest httpRequest;
  private AuthorizerMapper authorizerMapper;

  private TaskMaster taskMaster;
  private SupervisorManager supervisorManager;
  private final AtomicBoolean useSupervisors = new AtomicBoolean(false);

  private DataSourceCompactionConfig wikiConfig;

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
    supervisorManager = EasyMock.createStrictMock(SupervisorManager.class);
    configManager = EasyMock.createStrictMock(CoordinatorConfigManager.class);

    httpRequest = EasyMock.createStrictMock(HttpServletRequest.class);
    authorizerMapper = EasyMock.createStrictMock(AuthorizerMapper.class);
    EasyMock.expect(authorizerMapper.getAuthorizer("druid")).andReturn(new AllowAllAuthorizer(null)).anyTimes();

    taskMaster = EasyMock.createStrictMock(TaskMaster.class);
    EasyMock.expect(taskMaster.getSupervisorManager()).andReturn(Optional.of(supervisorManager)).anyTimes();

    final AuditManager auditManager = EasyMock.createStrictMock(AuditManager.class);

    compactionResource = new OverlordCompactionResource(
        scheduler,
        authorizerMapper,
        coordinatorClient,
        configManager,
        new CompactionSupervisorManager(taskMaster, auditManager)
    );

    wikiConfig = InlineSchemaDataSourceCompactionConfig
        .builder()
        .forDataSource(TestDataSource.WIKI)
        .withTaskPriority(random.nextInt(100))
        .withSkipOffsetFromLatest(Period.days(random.nextInt(5)))
        .build();

    validator = EasyMock.createStrictMock(CompactionScheduler.class);
    EasyMock.expect(validator.validateCompactionConfig(EasyMock.anyObject()))
            .andReturn(CompactionConfigValidationResult.success())
            .anyTimes();
    EasyMock.replay(validator, taskMaster, authorizerMapper);
  }

  @After
  public void tearDown()
  {
    EasyMock.verify(
        validator,
        scheduler,
        taskMaster,
        httpRequest,
        configManager,
        authorizerMapper,
        coordinatorClient,
        supervisorManager
    );
  }

  private void replayAll()
  {
    EasyMock.replay(scheduler, httpRequest, configManager, coordinatorClient, supervisorManager);
  }

  @Test
  public void test_updateClusterConfig()
  {
    EasyMock.expect(configManager.updateClusterCompactionConfig(EasyMock.anyObject(), EasyMock.anyObject()))
            .andReturn(true)
            .once();

    setupMockRequestForAudit();
    replayAll();

    Response response = compactionResource.updateClusterCompactionConfig(
        new ClusterCompactionConfig(0.5, 10, null, true, CompactionEngine.MSQ),
        httpRequest
    );
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(Map.of("success", true), response.getEntity());
  }

  @Test
  public void test_getClusterConfig()
  {
    final ClusterCompactionConfig clusterConfig =
        new ClusterCompactionConfig(0.4, 100, null, true, CompactionEngine.MSQ);
    EasyMock.expect(configManager.getClusterCompactionConfig())
            .andReturn(clusterConfig)
            .once();
    replayAll();

    final Response response = compactionResource.getClusterCompactionConfig();
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(clusterConfig, response.getEntity());
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

    setupMockRequestForUser("druid");
    EasyMock.expect(httpRequest.getMethod()).andReturn("POST").once();

    EasyMock.expect(scheduler.getAllCompactionSnapshots())
            .andReturn(Map.of(TestDataSource.WIKI, snapshot)).anyTimes();
    replayAll();

    final Response response = compactionResource.getAllCompactionSnapshots(httpRequest);
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(new CompactionStatusResponse(List.of(snapshot)), response.getEntity());
  }

  @Test
  public void test_getAllCompactionSnapshots_redirectsToCoordinator_ifSchedulerIsDisabled()
  {
    useSupervisors.set(false);

    final AutoCompactionSnapshot snapshot =
        AutoCompactionSnapshot.builder(TestDataSource.WIKI).build();

    setupMockRequestForUser("druid");
    EasyMock.expect(httpRequest.getMethod()).andReturn("GET").once();

    EasyMock.expect(coordinatorClient.getCompactionSnapshots(null))
            .andReturn(Futures.immediateFuture(new CompactionStatusResponse(List.of(snapshot))));
    replayAll();

    final Response response = compactionResource.getAllCompactionSnapshots(httpRequest);
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(new CompactionStatusResponse(List.of(snapshot)), response.getEntity());
  }

  @Test
  public void test_getDatasourceCompactionConfig()
  {
    final String supervisorId = CompactionSupervisorSpec.getSupervisorIdForDatasource(TestDataSource.WIKI);
    EasyMock.expect(supervisorManager.getSupervisorSpec(supervisorId))
            .andReturn(Optional.of(new CompactionSupervisorSpec(wikiConfig, false, validator)))
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
    setupMockRequestForAudit();
    EasyMock.expect(httpRequest.getMethod()).andReturn("POST").once();
    EasyMock.expect(httpRequest.getRequestURI()).andReturn("supes").once();
    EasyMock.expect(httpRequest.getQueryString()).andReturn("a=b").once();

    final CompactionSupervisorSpec supervisorSpec =
        new CompactionSupervisorSpec(wikiConfig, false, validator);

    EasyMock.expect(supervisorManager.shouldUpdateSupervisor(supervisorSpec))
            .andReturn(true).once();
    EasyMock.expect(supervisorManager.createOrUpdateAndStartSupervisor(supervisorSpec))
            .andReturn(true).once();
    EasyMock.expect(scheduler.validateCompactionConfig(wikiConfig))
            .andReturn(CompactionConfigValidationResult.success()).once();
    replayAll();

    final Response response = compactionResource
        .updateDatasourceCompactionConfig(TestDataSource.WIKI, wikiConfig, httpRequest);
    Assert.assertEquals(200, response.getStatus());
  }

  @Test
  public void test_updateDatasourceCompactionConfig_returnsInvalidInput_ifDatasourceIsNullOrEmpty()
  {
    replayAll();

    verifyInvalidInputResponse(
        compactionResource.updateDatasourceCompactionConfig("", wikiConfig, httpRequest),
        "No DataSource specified"
    );
    verifyInvalidInputResponse(
        compactionResource.updateDatasourceCompactionConfig(null, wikiConfig, httpRequest),
        "No DataSource specified"
    );
  }

  @Test
  public void test_deleteDatasourceCompactionConfig()
  {
    final String supervisorId = CompactionSupervisorSpec.getSupervisorIdForDatasource(TestDataSource.WIKI);
    EasyMock.expect(supervisorManager.stopAndRemoveSupervisor(supervisorId))
            .andReturn(true)
            .once();
    replayAll();

    final Response response = compactionResource
        .deleteDatasourceCompactionConfig(TestDataSource.WIKI, httpRequest);
    Assert.assertEquals(200, response.getStatus());
  }

  @Test
  public void test_deleteDatasourceCompactionConfig_returnsInvalidInput_ifDatasourceIsNullOrEmpty()
  {
    replayAll();

    verifyInvalidInputResponse(
        compactionResource.deleteDatasourceCompactionConfig("", httpRequest),
        "No DataSource specified"
    );
    verifyInvalidInputResponse(
        compactionResource.deleteDatasourceCompactionConfig(null, httpRequest),
        "No DataSource specified"
    );
  }

  @Test
  public void test_getAllCompactionConfigs()
  {
    final String supervisorId = CompactionSupervisorSpec
        .getSupervisorIdForDatasource(TestDataSource.WIKI);

    setupMockRequestForUser("druid");
    EasyMock.expect(httpRequest.getMethod()).andReturn("POST").once();

    EasyMock.expect(supervisorManager.getSupervisorIds())
            .andReturn(Set.of(supervisorId))
            .once();
    EasyMock.expect(supervisorManager.getSupervisorSpec(supervisorId))
            .andReturn(Optional.of(new CompactionSupervisorSpec(wikiConfig, false, validator)))
            .once();
    replayAll();

    final Response response = compactionResource.getAllCompactionConfigs(httpRequest);
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(new CompactionConfigsResponse(List.of(wikiConfig)), response.getEntity());
  }

  @Test
  public void test_getDatasourceCompactionConfigHistory_withFilters()
  {
    final String supervisorId = CompactionSupervisorSpec.getSupervisorIdForDatasource(TestDataSource.WIKI);

    final CompactionSupervisorSpec spec = new CompactionSupervisorSpec(wikiConfig, false, validator);
    final List<VersionedSupervisorSpec> specVersions = List.of(
        new VersionedSupervisorSpec(spec, "2025-03"),
        new VersionedSupervisorSpec(spec, "2025-02"),
        new VersionedSupervisorSpec(spec, "2025-01")
    );

    EasyMock.expect(supervisorManager.getSupervisorHistoryForId(supervisorId, null))
            .andReturn(specVersions)
            .anyTimes();
    replayAll();

    final List<DataSourceCompactionConfigAuditEntry> history = List.of(
        new DataSourceCompactionConfigAuditEntry(null, wikiConfig, null, DateTimes.of("2025-03")),
        new DataSourceCompactionConfigAuditEntry(null, wikiConfig, null, DateTimes.of("2025-02")),
        new DataSourceCompactionConfigAuditEntry(null, wikiConfig, null, DateTimes.of("2025-01"))
    );

    Response response = compactionResource
        .getDatasourceCompactionConfigHistory(TestDataSource.WIKI, null, null);
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(toHistoryResponse(history), response.getEntity());

    // Filter by count
    response = compactionResource
        .getDatasourceCompactionConfigHistory(TestDataSource.WIKI, null, 2);
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(toHistoryResponse(history.subList(0, 2)), response.getEntity());

    // Filter by interval
    response = compactionResource
        .getDatasourceCompactionConfigHistory(TestDataSource.WIKI, "2025-01/P40D", null);
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(toHistoryResponse(history.subList(1, 3)), response.getEntity());

    // Filter by interval and count
    response = compactionResource
        .getDatasourceCompactionConfigHistory(TestDataSource.WIKI, "2025-01/P40D", 1);
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(toHistoryResponse(history.subList(1, 2)), response.getEntity());
  }

  private static CompactionConfigHistoryResponse toHistoryResponse(
      List<DataSourceCompactionConfigAuditEntry> entries
  )
  {
    return new CompactionConfigHistoryResponse(entries);
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

  private void setupMockRequestForUser(String user)
  {
    EasyMock.expect(httpRequest.getAttribute(AuthConfig.DRUID_ALLOW_UNSECURED_PATH)).andReturn(null).atLeastOnce();
    EasyMock.expect(httpRequest.getAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED)).andReturn(null).atLeastOnce();
    EasyMock.expect(httpRequest.getAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT))
            .andReturn(new AuthenticationResult(user, "druid", null, null))
            .atLeastOnce();
    httpRequest.setAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED, true);
    EasyMock.expectLastCall().anyTimes();
  }

  private void setupMockRequestForAudit()
  {
    EasyMock.expect(httpRequest.getHeader(AuditManager.X_DRUID_AUTHOR)).andReturn("author").once();
    EasyMock.expect(httpRequest.getHeader(AuditManager.X_DRUID_COMMENT)).andReturn("comment").once();

    EasyMock.expect(httpRequest.getAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT))
            .andReturn(new AuthenticationResult("druid", "druid", null, null))
            .atLeastOnce();

    EasyMock.expect(httpRequest.getRemoteAddr()).andReturn("127.0.0.1").atLeastOnce();
    // EasyMock.expect(httpRequest.getMethod()).andReturn("POST").once();

    // EasyMock.expect(httpRequest.getAttribute(AuthConfig.DRUID_ALLOW_UNSECURED_PATH)).andReturn(null).atLeastOnce();
  }
}
