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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.data.input.InputStats;
import org.apache.druid.data.input.impl.ByteEntity;
import org.apache.druid.data.input.impl.CsvInputFormat;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.indexing.common.LockGranularity;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.seekablestream.common.OrderedPartitionableRecord;
import org.apache.druid.indexing.seekablestream.common.OrderedSequenceNumber;
import org.apache.druid.indexing.seekablestream.common.RecordSupplier;
import org.apache.druid.indexing.seekablestream.common.StreamPartition;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.granularity.AllGranularity;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.indexing.granularity.ArbitraryGranularitySpec;
import org.apache.druid.segment.transform.TransformSpec;
import org.apache.druid.server.security.Access;
import org.apache.druid.server.security.Action;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.Authorizer;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.server.security.ForbiddenException;
import org.apache.druid.server.security.ResourceType;
import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.servlet.http.HttpServletRequest;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Consumer;

import static org.easymock.EasyMock.mock;
import static org.easymock.EasyMock.replay;

/**
 * Unit Test to test authorization requirements of
 * {@link SeekableStreamIndexTaskRunner} and {@link SeekableStreamIndexTask}.
 */
public class SeekableStreamIndexTaskRunnerAuthTest
{

  /**
   * Test target.
   */
  private TestSeekableStreamIndexTaskRunner taskRunner;

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Before
  public void setUp()
  {
    // Create an AuthorizerMapper that only allows access to a Datasource resource
    AuthorizerMapper authorizerMapper = new AuthorizerMapper(null)
    {
      @Override
      public Authorizer getAuthorizer(String name)
      {
        return (authenticationResult, resource, action) -> {
          final String username = authenticationResult.getIdentity();

          // Allow access to a Datasource if
          // - Datasource Read User requests Read access
          // - or, Datasource Write User requests Write access
          if (resource.getType().equals(ResourceType.DATASOURCE)) {
            return new Access(
                (action == Action.READ && username.equals(Users.DATASOURCE_READ))
                || (action == Action.WRITE && username.equals(Users.DATASOURCE_WRITE))
            );
          }

          // Do not allow access to any other resource
          return new Access(false);
        };
      }
    };

    DataSchema dataSchema = new DataSchema(
        "datasource",
        new TimestampSpec(null, null, null),
        new DimensionsSpec(Collections.emptyList()),
        new AggregatorFactory[]{},
        new ArbitraryGranularitySpec(new AllGranularity(), Collections.emptyList()),
        TransformSpec.NONE,
        null,
        null
    );
    SeekableStreamIndexTaskTuningConfig tuningConfig = mock(SeekableStreamIndexTaskTuningConfig.class);
    SeekableStreamIndexTaskIOConfig<String, String> ioConfig = new TestSeekableStreamIndexTaskIOConfig();

    // Initiliaze task and task runner
    SeekableStreamIndexTask<String, String, ByteEntity> indexTask
        = new TestSeekableStreamIndexTask("id", dataSchema, tuningConfig, ioConfig);
    taskRunner = new TestSeekableStreamIndexTaskRunner(indexTask, authorizerMapper);
  }

  @Test
  public void testGetStatusHttp()
  {
    verifyOnlyDatasourceReadUserCanAccess(taskRunner::getStatusHTTP);
  }

  @Test
  public void testGetStartTime()
  {
    verifyOnlyDatasourceWriteUserCanAccess(taskRunner::getStartTime);
  }

  @Test
  public void testStop()
  {
    verifyOnlyDatasourceWriteUserCanAccess(taskRunner::stop);
  }

  @Test
  public void testPauseHttp()
  {
    verifyOnlyDatasourceWriteUserCanAccess(req -> {
      try {
        taskRunner.pauseHTTP(req);
      }
      catch (InterruptedException e) {
        // ignore
      }
    });
  }

  @Test
  public void testResumeHttp()
  {
    verifyOnlyDatasourceWriteUserCanAccess(req -> {
      try {
        taskRunner.resumeHTTP(req);
      }
      catch (InterruptedException e) {

      }
    });
  }

  @Test
  public void testGetEndOffsets()
  {
    verifyOnlyDatasourceReadUserCanAccess(taskRunner::getCurrentOffsets);
  }

  @Test
  public void testSetEndOffsetsHttp()
  {
    verifyOnlyDatasourceWriteUserCanAccess(request -> {
      try {
        taskRunner.setEndOffsetsHTTP(Collections.emptyMap(), false, request);
      }
      catch (InterruptedException e) {

      }
    });
  }

  @Test
  public void testGetCheckpointsHttp()
  {
    verifyOnlyDatasourceReadUserCanAccess(taskRunner::getCheckpointsHTTP);
  }


  private void verifyOnlyDatasourceWriteUserCanAccess(
      Consumer<HttpServletRequest> method
  )
  {
    // Verify that datasource write user can access
    HttpServletRequest allowedRequest = createRequest(Users.DATASOURCE_WRITE);
    replay(allowedRequest);
    method.accept(allowedRequest);

    // Verify that no other user can access
    HttpServletRequest blockedRequest = createRequest(Users.DATASOURCE_READ);
    replay(blockedRequest);
    expectedException.expect(ForbiddenException.class);
    method.accept(blockedRequest);
  }

  private void verifyOnlyDatasourceReadUserCanAccess(
      Consumer<HttpServletRequest> method
  )
  {
    // Verify that datasource read user can access
    HttpServletRequest allowedRequest = createRequest(Users.DATASOURCE_READ);
    replay(allowedRequest);
    method.accept(allowedRequest);

    // Verify that no other user can access
    HttpServletRequest blockedRequest = createRequest(Users.DATASOURCE_WRITE);
    replay(blockedRequest);
    expectedException.expect(ForbiddenException.class);
    method.accept(blockedRequest);
  }

  private HttpServletRequest createRequest(String username)
  {
    HttpServletRequest request = mock(HttpServletRequest.class);

    AuthenticationResult authenticationResult = new AuthenticationResult(username, "druid", null, null);
    EasyMock.expect(request.getAttribute(AuthConfig.DRUID_ALLOW_UNSECURED_PATH)).andReturn(null).anyTimes();
    EasyMock.expect(request.getAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED)).andReturn(null).atLeastOnce();
    EasyMock.expect(request.getAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT))
            .andReturn(authenticationResult)
            .atLeastOnce();

    request.setAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED, false);
    EasyMock.expectLastCall().anyTimes();

    request.setAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED, true);
    EasyMock.expectLastCall().anyTimes();

    return request;
  }

  /**
   * Dummy implementation used as the test target to test out the non-abstract methods.
   */
  private static class TestSeekableStreamIndexTaskRunner
      extends SeekableStreamIndexTaskRunner<String, String, ByteEntity>
  {

    private TestSeekableStreamIndexTaskRunner(
        SeekableStreamIndexTask<String, String, ByteEntity> task,
        AuthorizerMapper authorizerMapper
    )
    {
      super(task, null, authorizerMapper, LockGranularity.SEGMENT, new InputStats());
    }

    @Override
    protected boolean isEndOfShard(String seqNum)
    {
      return false;
    }

    @Nullable
    @Override
    protected TreeMap<Integer, Map<String, String>> getCheckPointsFromContext(
        TaskToolbox toolbox,
        String checkpointsString
    )
    {
      return null;
    }

    @Override
    protected String getNextStartOffset(String sequenceNumber)
    {
      return null;
    }

    @Override
    protected SeekableStreamEndSequenceNumbers<String, String> deserializePartitionsFromMetadata(
        ObjectMapper mapper,
        Object object
    )
    {
      return null;
    }

    @Nonnull
    @Override
    protected List<OrderedPartitionableRecord<String, String, ByteEntity>> getRecords(
        RecordSupplier<String, String, ByteEntity> recordSupplier,
        TaskToolbox toolbox
    )
    {
      return null;
    }

    @Override
    protected SeekableStreamDataSourceMetadata<String, String> createDataSourceMetadata(
        SeekableStreamSequenceNumbers<String, String> partitions
    )
    {
      return null;
    }

    @Override
    protected OrderedSequenceNumber<String> createSequenceNumber(String sequenceNumber)
    {
      return null;
    }

    @Override
    protected void possiblyResetDataSourceMetadata(
        TaskToolbox toolbox,
        RecordSupplier<String, String, ByteEntity> recordSupplier,
        Set<StreamPartition<String>> assignment
    )
    {

    }

    @Override
    protected boolean isEndOffsetExclusive()
    {
      return false;
    }

    @Override
    protected TypeReference<List<SequenceMetadata<String, String>>> getSequenceMetadataTypeReference()
    {
      return null;
    }
  }

  private static class TestSeekableStreamIndexTask extends SeekableStreamIndexTask<String, String, ByteEntity>
  {

    public TestSeekableStreamIndexTask(
        String id,
        DataSchema dataSchema,
        SeekableStreamIndexTaskTuningConfig tuningConfig,
        SeekableStreamIndexTaskIOConfig<String, String> ioConfig
    )
    {
      super(id, null, dataSchema, tuningConfig, ioConfig, null, null);
    }

    @Override
    public String getType()
    {
      return null;
    }

    @Override
    protected SeekableStreamIndexTaskRunner<String, String, ByteEntity> createTaskRunner()
    {
      return null;
    }

    @Override
    protected RecordSupplier<String, String, ByteEntity> newTaskRecordSupplier()
    {
      return null;
    }
  }

  private static class TestSeekableStreamIndexTaskIOConfig extends SeekableStreamIndexTaskIOConfig<String, String>
  {
    public TestSeekableStreamIndexTaskIOConfig()
    {
      super(
          null,
          "someSequence",
          new SeekableStreamStartSequenceNumbers<>("abc", "def", Collections.emptyMap(), Collections.emptyMap(), null),
          new SeekableStreamEndSequenceNumbers<>("abc", "def", Collections.emptyMap(), Collections.emptyMap()),
          false,
          DateTimes.nowUtc().minusDays(2),
          DateTimes.nowUtc(),
          new CsvInputFormat(null, null, true, null, 0)
      );
    }
  }

  /**
   * Usernames used in the tests.
   */
  private static class Users
  {
    private static final String DATASOURCE_READ = "datasourceRead";
    private static final String DATASOURCE_WRITE = "datasourceWrite";
  }

}
