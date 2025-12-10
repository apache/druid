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

package org.apache.druid.indexing.seekablestream;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import org.apache.druid.audit.AuditManager;
import org.apache.druid.indexing.overlord.IndexerMetadataStorageCoordinator;
import org.apache.druid.indexing.overlord.TaskMaster;
import org.apache.druid.indexing.overlord.TaskStorage;
import org.apache.druid.indexing.overlord.supervisor.Supervisor;
import org.apache.druid.indexing.overlord.supervisor.SupervisorManager;
import org.apache.druid.indexing.overlord.supervisor.SupervisorResource;
import org.apache.druid.indexing.overlord.supervisor.SupervisorSpec;
import org.apache.druid.indexing.overlord.supervisor.SupervisorStateManagerConfig;
import org.apache.druid.indexing.seekablestream.supervisor.SeekableStreamSupervisorIOConfig;
import org.apache.druid.indexing.seekablestream.supervisor.SeekableStreamSupervisorIngestionSpec;
import org.apache.druid.indexing.seekablestream.supervisor.SeekableStreamSupervisorSpec;
import org.apache.druid.indexing.seekablestream.supervisor.autoscaler.AutoScalerConfig;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.metrics.DruidMonitorSchedulerConfig;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.incremental.RowIngestionMetersFactory;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.server.security.Access;
import org.apache.druid.server.security.Action;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.Authorizer;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.server.security.Resource;
import org.apache.druid.server.security.ResourceAction;
import org.apache.druid.server.security.ResourceType;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.easymock.EasyMockRunner;
import org.easymock.EasyMockSupport;
import org.easymock.Mock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Response;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

@RunWith(EasyMockRunner.class)
public class SupervisorResourceConfigMergeTest extends EasyMockSupport
{
  private static final ObjectMapper OBJECT_MAPPER = TestHelper.makeJsonMapper();
  @Mock
  private TaskMaster taskMaster;

  @Mock
  private SupervisorManager supervisorManager;

  @Mock
  private HttpServletRequest request;

  @Mock
  private AuthConfig authConfig;

  @Mock
  private AuditManager auditManager;

  private SupervisorResource supervisorResource;

  @Before
  public void setUp()
  {
    supervisorResource = new SupervisorResource(
        taskMaster,
        new AuthorizerMapper(null)
        {
          @Override
          public Authorizer getAuthorizer(String name)
          {
            return (authenticationResult, resource, action) -> {
              if (authenticationResult.getIdentity().equals("druid")) {
                return Access.OK;
              } else {
                if (resource.getType().equals(ResourceType.DATASOURCE)) {
                  if (resource.getName().equals("datasource2")) {
                    return Access.deny("not authorized.");
                  } else {
                    return Access.OK;
                  }
                } else if (resource.getType().equals(ResourceType.EXTERNAL)) {
                  if (resource.getName().equals("test")) {
                    return Access.deny("not authorized.");
                  } else {
                    return Access.OK;
                  }
                }
                return Access.OK;
              }
            };
          }
        },
        OBJECT_MAPPER,
        authConfig,
        auditManager
    );
  }

  /**
   * Tests that when a new spec is submitted without taskCountStart, the merge function
   * follows the priority: provided taskCountStart > provided taskCount > existing taskCount > provided taskCountMin.
   * In this test, existing spec has taskCount=5, new spec has no taskCount set,
   * so merge should use existing taskCount (5).
   */
  @Test
  public void testSpecPostMergeUsesExistingTaskCount()
  {
    // Create an existing spec with taskCount=5 (simulating a scaled state)
    HashMap<String, Object> existingAutoScalerConfig = new HashMap<>();
    existingAutoScalerConfig.put("enableTaskAutoScaler", true);
    existingAutoScalerConfig.put("taskCountMax", 7);
    existingAutoScalerConfig.put("taskCountMin", 1);
    // Note: taskCountStart is NOT set in existing spec

    SeekableStreamSupervisorIOConfig existingIoConfig = EasyMock.createMock(SeekableStreamSupervisorIOConfig.class);
    EasyMock.expect(existingIoConfig.getAutoScalerConfig())
            .andReturn(OBJECT_MAPPER.convertValue(existingAutoScalerConfig, AutoScalerConfig.class))
            .anyTimes();
    EasyMock.expect(existingIoConfig.getStream()).andReturn("test-stream").anyTimes();
    EasyMock.expect(existingIoConfig.getTaskCount()).andReturn(5).anyTimes(); // existing taskCount is 5
    EasyMock.replay(existingIoConfig);

    DataSchema existingDataSchema = EasyMock.createMock(DataSchema.class);
    EasyMock.expect(existingDataSchema.getDataSource()).andReturn("datasource1").anyTimes();
    EasyMock.replay(existingDataSchema);

    SeekableStreamSupervisorIngestionSpec existingIngestionSchema =
        EasyMock.createMock(SeekableStreamSupervisorIngestionSpec.class);
    EasyMock.expect(existingIngestionSchema.getIOConfig()).andReturn(existingIoConfig).anyTimes();
    EasyMock.expect(existingIngestionSchema.getDataSchema()).andReturn(existingDataSchema).anyTimes();
    EasyMock.replay(existingIngestionSchema);

    TestSeekableStreamSupervisorSpec existingSpec = new TestSeekableStreamSupervisorSpec(
        "my-id",
        existingIngestionSchema
    );

    // Create a new spec WITHOUT taskCountStart and WITHOUT taskCount in autoScalerConfig
    HashMap<String, Object> newAutoScalerConfig = new HashMap<>();
    newAutoScalerConfig.put("enableTaskAutoScaler", true);
    newAutoScalerConfig.put("taskCountMax", 8);
    newAutoScalerConfig.put("taskCountMin", 2);
    // Note: taskCountStart is NOT set

    SeekableStreamSupervisorIOConfig newIoConfig = EasyMock.createMock(SeekableStreamSupervisorIOConfig.class);
    EasyMock.expect(newIoConfig.getAutoScalerConfig())
            .andReturn(OBJECT_MAPPER.convertValue(newAutoScalerConfig, AutoScalerConfig.class))
            .anyTimes();
    EasyMock.expect(newIoConfig.getStream()).andReturn("test-stream").anyTimes();
    EasyMock.expect(newIoConfig.getTaskCount()).andReturn(null).anyTimes(); // new spec has no taskCount
    newIoConfig.setTaskCount(5); // Expect merge to set taskCount to existing value (5)
    EasyMock.expectLastCall().once();
    EasyMock.replay(newIoConfig);

    DataSchema newDataSchema = EasyMock.createMock(DataSchema.class);
    EasyMock.expect(newDataSchema.getDataSource()).andReturn("datasource1").anyTimes();
    EasyMock.replay(newDataSchema);

    SeekableStreamSupervisorIngestionSpec newIngestionSchema =
        EasyMock.createMock(SeekableStreamSupervisorIngestionSpec.class);
    EasyMock.expect(newIngestionSchema.getIOConfig()).andReturn(newIoConfig).anyTimes();
    EasyMock.expect(newIngestionSchema.getDataSchema()).andReturn(newDataSchema).anyTimes();
    EasyMock.replay(newIngestionSchema);

    TestSeekableStreamSupervisorSpec newSpec = new TestSeekableStreamSupervisorSpec(
        "my-id",
        newIngestionSchema
    )
    {
      @Override
      public List<String> getDataSources()
      {
        return Collections.singletonList("datasource1");
      }
    };

    // Set up mocks for SupervisorManager behavior
    EasyMock.expect(taskMaster.getSupervisorManager()).andReturn(Optional.of(supervisorManager));

    // Mock createOrUpdateAndStartSupervisor to call merge
    final SupervisorSpec existingSpecForMerge = existingSpec;
    Capture<SupervisorSpec> capturedNewSpec = EasyMock.newCapture();
    EasyMock.expect(supervisorManager.createOrUpdateAndStartSupervisor(EasyMock.capture(capturedNewSpec)))
            .andAnswer(() -> {
              SupervisorSpec arg = (SupervisorSpec) EasyMock.getCurrentArguments()[0];
              arg.merge(existingSpecForMerge);
              return true;
            });

    // Mock getSupervisorSpec to return the existing spec (simulating an update scenario)
    EasyMock.expect(supervisorManager.getSupervisorSpec("my-id"))
            .andReturn(Optional.of(existingSpec))
            .anyTimes();

    setupMockRequest();
    setupMockRequestForAudit();

    EasyMock.expect(authConfig.isEnableInputSourceSecurity()).andReturn(true);
    auditManager.doAudit(EasyMock.anyObject());
    EasyMock.expectLastCall().once();

    replayAll();

    // Before merge, taskCountStart should be null in new spec
    Assert.assertNull(newSpec.getIoConfig().getAutoScalerConfig().getTaskCountStart());

    // When
    Response response = supervisorResource.specPost(newSpec, false, request);
    verifyAll();

    Assert.assertEquals(200, response.getStatus());
  }

  /**
   * Tests that when a new spec is submitted with taskCount set, merge uses provided taskCount
   * over existing taskCount. Priority: provided taskCountStart > provided taskCount > existing taskCount > provided taskCountMin.
   */
  @Test
  public void testSpecPostMergeUsesProvidedTaskCount()
  {
    // Create an existing spec with taskCount=5
    HashMap<String, Object> existingAutoScalerConfig = new HashMap<>();
    existingAutoScalerConfig.put("enableTaskAutoScaler", true);
    existingAutoScalerConfig.put("taskCountMax", 7);
    existingAutoScalerConfig.put("taskCountMin", 1);

    SeekableStreamSupervisorIOConfig existingIoConfig = EasyMock.createMock(SeekableStreamSupervisorIOConfig.class);
    EasyMock.expect(existingIoConfig.getAutoScalerConfig())
            .andReturn(OBJECT_MAPPER.convertValue(existingAutoScalerConfig, AutoScalerConfig.class))
            .anyTimes();
    EasyMock.expect(existingIoConfig.getStream()).andReturn("test-stream").anyTimes();
    EasyMock.expect(existingIoConfig.getTaskCount()).andReturn(5).anyTimes();
    EasyMock.replay(existingIoConfig);

    DataSchema existingDataSchema = EasyMock.createMock(DataSchema.class);
    EasyMock.expect(existingDataSchema.getDataSource()).andReturn("datasource1").anyTimes();
    EasyMock.replay(existingDataSchema);

    SeekableStreamSupervisorIngestionSpec existingIngestionSchema =
        EasyMock.createMock(SeekableStreamSupervisorIngestionSpec.class);
    EasyMock.expect(existingIngestionSchema.getIOConfig()).andReturn(existingIoConfig).anyTimes();
    EasyMock.expect(existingIngestionSchema.getDataSchema()).andReturn(existingDataSchema).anyTimes();
    EasyMock.replay(existingIngestionSchema);

    TestSeekableStreamSupervisorSpec existingSpec = new TestSeekableStreamSupervisorSpec(
        "my-id",
        existingIngestionSchema
    );

    // Create a new spec with taskCount=3 (provided taskCount should take precedence)
    HashMap<String, Object> newAutoScalerConfig = new HashMap<>();
    newAutoScalerConfig.put("enableTaskAutoScaler", true);
    newAutoScalerConfig.put("taskCountMax", 8);
    newAutoScalerConfig.put("taskCountMin", 2);

    SeekableStreamSupervisorIOConfig newIoConfig = EasyMock.createMock(SeekableStreamSupervisorIOConfig.class);
    EasyMock.expect(newIoConfig.getAutoScalerConfig())
            .andReturn(OBJECT_MAPPER.convertValue(newAutoScalerConfig, AutoScalerConfig.class))
            .anyTimes();
    EasyMock.expect(newIoConfig.getStream()).andReturn("test-stream").anyTimes();
    EasyMock.expect(newIoConfig.getTaskCount()).andReturn(3).anyTimes(); // provided taskCount=3
    newIoConfig.setTaskCount(3); // Expect merge to use provided taskCount (3)
    EasyMock.expectLastCall().once();
    EasyMock.replay(newIoConfig);

    DataSchema newDataSchema = EasyMock.createMock(DataSchema.class);
    EasyMock.expect(newDataSchema.getDataSource()).andReturn("datasource1").anyTimes();
    EasyMock.replay(newDataSchema);

    SeekableStreamSupervisorIngestionSpec newIngestionSchema =
        EasyMock.createMock(SeekableStreamSupervisorIngestionSpec.class);
    EasyMock.expect(newIngestionSchema.getIOConfig()).andReturn(newIoConfig).anyTimes();
    EasyMock.expect(newIngestionSchema.getDataSchema()).andReturn(newDataSchema).anyTimes();
    EasyMock.replay(newIngestionSchema);

    TestSeekableStreamSupervisorSpec newSpec = new TestSeekableStreamSupervisorSpec(
        "my-id",
        newIngestionSchema
    )
    {
      @Override
      public List<String> getDataSources()
      {
        return Collections.singletonList("datasource1");
      }
    };

    EasyMock.expect(taskMaster.getSupervisorManager()).andReturn(Optional.of(supervisorManager));

    final SupervisorSpec existingSpecForMerge = existingSpec;
    Capture<SupervisorSpec> capturedNewSpec = EasyMock.newCapture();
    EasyMock.expect(supervisorManager.createOrUpdateAndStartSupervisor(EasyMock.capture(capturedNewSpec)))
            .andAnswer(() -> {
              SupervisorSpec arg = (SupervisorSpec) EasyMock.getCurrentArguments()[0];
              arg.merge(existingSpecForMerge);
              return true;
            });

    EasyMock.expect(supervisorManager.getSupervisorSpec("my-id"))
            .andReturn(Optional.of(existingSpec))
            .anyTimes();

    setupMockRequest();
    setupMockRequestForAudit();

    EasyMock.expect(authConfig.isEnableInputSourceSecurity()).andReturn(true);
    auditManager.doAudit(EasyMock.anyObject());
    EasyMock.expectLastCall().once();

    replayAll();

    Response response = supervisorResource.specPost(newSpec, false, request);
    verifyAll();

    Assert.assertEquals(200, response.getStatus());
  }

  /**
   * Tests that when neither provided taskCount nor existing taskCount is set,
   * merge falls back to provided taskCountMin.
   */
  @Test
  public void testSpecPostMergeFallsBackToProvidedTaskCountMin()
  {
    // Create an existing spec with no taskCount
    HashMap<String, Object> existingAutoScalerConfig = new HashMap<>();
    existingAutoScalerConfig.put("enableTaskAutoScaler", true);
    existingAutoScalerConfig.put("taskCountMax", 7);
    existingAutoScalerConfig.put("taskCountMin", 1);

    SeekableStreamSupervisorIOConfig existingIoConfig = EasyMock.createMock(SeekableStreamSupervisorIOConfig.class);
    EasyMock.expect(existingIoConfig.getAutoScalerConfig())
            .andReturn(OBJECT_MAPPER.convertValue(existingAutoScalerConfig, AutoScalerConfig.class))
            .anyTimes();
    EasyMock.expect(existingIoConfig.getStream()).andReturn("test-stream").anyTimes();
    EasyMock.expect(existingIoConfig.getTaskCount()).andReturn(null).anyTimes(); // existing has no taskCount
    EasyMock.replay(existingIoConfig);

    DataSchema existingDataSchema = EasyMock.createMock(DataSchema.class);
    EasyMock.expect(existingDataSchema.getDataSource()).andReturn("datasource1").anyTimes();
    EasyMock.replay(existingDataSchema);

    SeekableStreamSupervisorIngestionSpec existingIngestionSchema =
        EasyMock.createMock(SeekableStreamSupervisorIngestionSpec.class);
    EasyMock.expect(existingIngestionSchema.getIOConfig()).andReturn(existingIoConfig).anyTimes();
    EasyMock.expect(existingIngestionSchema.getDataSchema()).andReturn(existingDataSchema).anyTimes();
    EasyMock.replay(existingIngestionSchema);

    TestSeekableStreamSupervisorSpec existingSpec = new TestSeekableStreamSupervisorSpec(
        "my-id",
        existingIngestionSchema
    );

    // Create a new spec with taskCountMin=4, no taskCount
    HashMap<String, Object> newAutoScalerConfig = new HashMap<>();
    newAutoScalerConfig.put("enableTaskAutoScaler", true);
    newAutoScalerConfig.put("taskCountMax", 8);
    newAutoScalerConfig.put("taskCountMin", 4); // provided taskCountMin

    SeekableStreamSupervisorIOConfig newIoConfig = EasyMock.createMock(SeekableStreamSupervisorIOConfig.class);
    EasyMock.expect(newIoConfig.getAutoScalerConfig())
            .andReturn(OBJECT_MAPPER.convertValue(newAutoScalerConfig, AutoScalerConfig.class))
            .anyTimes();
    EasyMock.expect(newIoConfig.getStream()).andReturn("test-stream").anyTimes();
    EasyMock.expect(newIoConfig.getTaskCount()).andReturn(null).anyTimes(); // no provided taskCount
    newIoConfig.setTaskCount(4); // Expect merge to fall back to provided taskCountMin (4)
    EasyMock.expectLastCall().once();
    EasyMock.replay(newIoConfig);

    DataSchema newDataSchema = EasyMock.createMock(DataSchema.class);
    EasyMock.expect(newDataSchema.getDataSource()).andReturn("datasource1").anyTimes();
    EasyMock.replay(newDataSchema);

    SeekableStreamSupervisorIngestionSpec newIngestionSchema =
        EasyMock.createMock(SeekableStreamSupervisorIngestionSpec.class);
    EasyMock.expect(newIngestionSchema.getIOConfig()).andReturn(newIoConfig).anyTimes();
    EasyMock.expect(newIngestionSchema.getDataSchema()).andReturn(newDataSchema).anyTimes();
    EasyMock.replay(newIngestionSchema);

    TestSeekableStreamSupervisorSpec newSpec = new TestSeekableStreamSupervisorSpec(
        "my-id",
        newIngestionSchema
    )
    {
      @Override
      public List<String> getDataSources()
      {
        return Collections.singletonList("datasource1");
      }
    };

    EasyMock.expect(taskMaster.getSupervisorManager()).andReturn(Optional.of(supervisorManager));

    final SupervisorSpec existingSpecForMerge = existingSpec;
    Capture<SupervisorSpec> capturedNewSpec = EasyMock.newCapture();
    EasyMock.expect(supervisorManager.createOrUpdateAndStartSupervisor(EasyMock.capture(capturedNewSpec)))
            .andAnswer(() -> {
              SupervisorSpec arg = (SupervisorSpec) EasyMock.getCurrentArguments()[0];
              arg.merge(existingSpecForMerge);
              return true;
            });

    EasyMock.expect(supervisorManager.getSupervisorSpec("my-id"))
            .andReturn(Optional.of(existingSpec))
            .anyTimes();

    setupMockRequest();
    setupMockRequestForAudit();

    EasyMock.expect(authConfig.isEnableInputSourceSecurity()).andReturn(true);
    auditManager.doAudit(EasyMock.anyObject());
    EasyMock.expectLastCall().once();

    replayAll();

    Response response = supervisorResource.specPost(newSpec, false, request);
    verifyAll();

    Assert.assertEquals(200, response.getStatus());
  }

  private void setupMockRequest()
  {
    EasyMock.expect(request.getAttribute(AuthConfig.DRUID_ALLOW_UNSECURED_PATH)).andReturn(null).atLeastOnce();
    EasyMock.expect(request.getAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED)).andReturn(null).atLeastOnce();
    EasyMock.expect(request.getAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT))
            .andReturn(new AuthenticationResult("druid", "druid", null, null))
            .atLeastOnce();
    request.setAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED, true);
    EasyMock.expectLastCall().anyTimes();
  }

  private void setupMockRequestForAudit()
  {
    EasyMock.expect(request.getHeader(AuditManager.X_DRUID_AUTHOR)).andReturn("author").once();
    EasyMock.expect(request.getHeader(AuditManager.X_DRUID_COMMENT)).andReturn("comment").once();

    EasyMock.expect(request.getRemoteAddr()).andReturn("127.0.0.1").once();
    EasyMock.expect(request.getMethod()).andReturn("POST").once();
    EasyMock.expect(request.getRequestURI()).andReturn("supes").once();
    EasyMock.expect(request.getQueryString()).andReturn("a=b").once();
  }

  static class TestSeekableStreamSupervisorSpec extends SeekableStreamSupervisorSpec
  {
    public TestSeekableStreamSupervisorSpec(
        @Nullable String id,
        SeekableStreamSupervisorIngestionSpec ingestionSchema
    )
    {
      super(
          id,
          ingestionSchema,
          null,
          false,
          EasyMock.createMock(TaskStorage.class),
          EasyMock.createMock(TaskMaster.class),
          EasyMock.createMock(IndexerMetadataStorageCoordinator.class),
          EasyMock.createMock(SeekableStreamIndexTaskClientFactory.class),
          OBJECT_MAPPER,
          EasyMock.createMock(ServiceEmitter.class),
          EasyMock.createMock(DruidMonitorSchedulerConfig.class),
          EasyMock.createMock(RowIngestionMetersFactory.class),
          EasyMock.createMock(SupervisorStateManagerConfig.class)
      );
    }

    @Override
    public Supervisor createSupervisor()
    {
      return null;
    }

    @Override
    public String getType()
    {
      return "test";
    }

    @Override
    public String getSource()
    {
      return "test-stream";
    }

    @Override
    protected SeekableStreamSupervisorSpec toggleSuspend(boolean suspend)
    {
      return null;
    }

    @JsonIgnore
    @Nonnull
    @Override
    public Set<ResourceAction> getInputSourceResources() throws UnsupportedOperationException
    {
      return Collections.singleton(new ResourceAction(new Resource("test", ResourceType.EXTERNAL), Action.READ));
    }
  }


}
