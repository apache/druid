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

package io.druid.indexing.overlord.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.druid.jackson.DefaultObjectMapper;
import org.joda.time.Period;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class RemoteTaskRunnerConfigTest
{
  private static final ObjectMapper mapper = new DefaultObjectMapper();
  private static final Period DEFAULT_TIMEOUT = Period.ZERO;
  private static final String DEFAULT_VERSION = "";
  private static final long DEFAULT_MAX_ZNODE = 10 * 1024;
  private static final int DEFAULT_PENDING_TASKS_RUNNER_NUM_THREADS = 5;

  @Test
  public void testGetTaskAssignmentTimeout() throws Exception
  {
    final Period timeout = Period.hours(1);
    Assert.assertEquals(
        timeout,
        reflect(generateRemoteTaskRunnerConfig(
            timeout,
            DEFAULT_TIMEOUT,
            DEFAULT_VERSION,
            DEFAULT_MAX_ZNODE,
            DEFAULT_TIMEOUT,
            DEFAULT_PENDING_TASKS_RUNNER_NUM_THREADS
        )).getTaskAssignmentTimeout()
    );
  }

  @Test
  public void testGetPendingTasksRunnerNumThreads() throws Exception
  {
    final int pendingTasksRunnerNumThreads = 20;
    Assert.assertEquals(
        pendingTasksRunnerNumThreads,
        reflect(generateRemoteTaskRunnerConfig(
            DEFAULT_TIMEOUT,
            DEFAULT_TIMEOUT,
            DEFAULT_VERSION,
            DEFAULT_MAX_ZNODE,
            DEFAULT_TIMEOUT,
            pendingTasksRunnerNumThreads
        )).getPendingTasksRunnerNumThreads()
    );
  }

  @Test
  public void testGetMinWorkerVersion() throws Exception
  {
    final String version = "some version";
    Assert.assertEquals(
        version,
        reflect(generateRemoteTaskRunnerConfig(
            DEFAULT_TIMEOUT,
            DEFAULT_TIMEOUT,
            version,
            DEFAULT_MAX_ZNODE,
            DEFAULT_TIMEOUT,
            DEFAULT_PENDING_TASKS_RUNNER_NUM_THREADS
        )).getMinWorkerVersion()
    );
  }

  @Test
  public void testGetMaxZnodeBytes() throws Exception
  {
    final long max = 20 * 1024;
    Assert.assertEquals(
        max,
        reflect(generateRemoteTaskRunnerConfig(
            DEFAULT_TIMEOUT,
            DEFAULT_TIMEOUT,
            DEFAULT_VERSION,
            max,
            DEFAULT_TIMEOUT,
            DEFAULT_PENDING_TASKS_RUNNER_NUM_THREADS
        )).getMaxZnodeBytes()
    );
  }

  @Test
  public void testGetTaskShutdownLinkTimeout() throws Exception
  {
    final Period timeout = Period.hours(1);
    Assert.assertEquals(
        timeout,
        reflect(generateRemoteTaskRunnerConfig(
            DEFAULT_TIMEOUT,
            DEFAULT_TIMEOUT,
            DEFAULT_VERSION,
            DEFAULT_MAX_ZNODE,
            timeout,
            DEFAULT_PENDING_TASKS_RUNNER_NUM_THREADS
        )).getTaskShutdownLinkTimeout()
    );
  }

  @Test
  public void testGetTaskCleanupTimeout() throws Exception
  {
    final Period timeout = Period.hours(1);
    Assert.assertEquals(
        timeout,
        reflect(generateRemoteTaskRunnerConfig(
                    DEFAULT_TIMEOUT,
                    timeout,
                    DEFAULT_VERSION,
                    DEFAULT_MAX_ZNODE,
                    DEFAULT_TIMEOUT,
                    DEFAULT_PENDING_TASKS_RUNNER_NUM_THREADS
                )).getTaskCleanupTimeout()
    );
  }

  @Test
  public void testEquals() throws Exception
  {
    Assert.assertEquals(
        reflect(generateRemoteTaskRunnerConfig(
            DEFAULT_TIMEOUT,
            DEFAULT_TIMEOUT,
            DEFAULT_VERSION,
            DEFAULT_MAX_ZNODE,
            DEFAULT_TIMEOUT,
            DEFAULT_PENDING_TASKS_RUNNER_NUM_THREADS
        )),
        reflect(generateRemoteTaskRunnerConfig(
            DEFAULT_TIMEOUT,
            DEFAULT_TIMEOUT,
            DEFAULT_VERSION,
            DEFAULT_MAX_ZNODE,
            DEFAULT_TIMEOUT,
            DEFAULT_PENDING_TASKS_RUNNER_NUM_THREADS
        ))
    );
    final Period timeout = Period.years(999);
    final String version = "someVersion";
    final long max = 20 * 1024;
    final int pendingTasksRunnerNumThreads = 20;
    Assert.assertEquals(
        reflect(generateRemoteTaskRunnerConfig(
            timeout,
            timeout,
            version,
            max,
            timeout,
            pendingTasksRunnerNumThreads
        )),
        reflect(generateRemoteTaskRunnerConfig(
            timeout,
            timeout,
            version,
            max,
            timeout,
            pendingTasksRunnerNumThreads
        ))
    );
    Assert.assertNotEquals(
        reflect(generateRemoteTaskRunnerConfig(
            timeout,
            timeout,
            version,
            max,
            timeout,
            pendingTasksRunnerNumThreads
        )),
        reflect(generateRemoteTaskRunnerConfig(
            DEFAULT_TIMEOUT,
            timeout,
            version,
            max,
            timeout,
            pendingTasksRunnerNumThreads
        ))
    );
    Assert.assertNotEquals(
        reflect(generateRemoteTaskRunnerConfig(
            timeout,
            timeout,
            version,
            max,
            timeout,
            pendingTasksRunnerNumThreads
        )),
        reflect(generateRemoteTaskRunnerConfig(
            timeout,
            DEFAULT_TIMEOUT,
            version,
            max,
            timeout,
            pendingTasksRunnerNumThreads
        ))
    );
    Assert.assertNotEquals(
        reflect(generateRemoteTaskRunnerConfig(
            timeout,
            timeout,
            version,
            max,
            timeout,
            pendingTasksRunnerNumThreads
        )),
        reflect(generateRemoteTaskRunnerConfig(
            timeout,
            timeout,
            DEFAULT_VERSION,
            max,
            timeout,
            pendingTasksRunnerNumThreads
        ))
    );

    Assert.assertNotEquals(
        reflect(generateRemoteTaskRunnerConfig(
            timeout,
            timeout,
            version,
            max,
            timeout,
            pendingTasksRunnerNumThreads
        )),
        reflect(generateRemoteTaskRunnerConfig(
            timeout,
            timeout,
            version,
            DEFAULT_MAX_ZNODE,
            timeout,
            pendingTasksRunnerNumThreads
        ))
    );


    Assert.assertNotEquals(
        reflect(generateRemoteTaskRunnerConfig(
            timeout,
            timeout,
            version,
            max,
            timeout,
            pendingTasksRunnerNumThreads
        )),
        reflect(generateRemoteTaskRunnerConfig(
            timeout,
            timeout,
            version,
            max,
            DEFAULT_TIMEOUT,
            pendingTasksRunnerNumThreads
        ))
    );

    Assert.assertNotEquals(
        reflect(generateRemoteTaskRunnerConfig(
                    timeout,
                    timeout,
                    version,
                    max,
                    timeout,
                    pendingTasksRunnerNumThreads
                )),
        reflect(generateRemoteTaskRunnerConfig(
                    timeout,
                    timeout,
                    version,
                    max,
                    timeout,
                    DEFAULT_PENDING_TASKS_RUNNER_NUM_THREADS
                ))
    );
  }

  @Test
  public void testHashCode() throws Exception
  {
    Assert.assertEquals(
        reflect(generateRemoteTaskRunnerConfig(
            DEFAULT_TIMEOUT,
            DEFAULT_TIMEOUT,
            DEFAULT_VERSION,
            DEFAULT_MAX_ZNODE,
            DEFAULT_TIMEOUT,
            DEFAULT_PENDING_TASKS_RUNNER_NUM_THREADS
        )).hashCode(),
        reflect(generateRemoteTaskRunnerConfig(
            DEFAULT_TIMEOUT,
            DEFAULT_TIMEOUT,
            DEFAULT_VERSION,
            DEFAULT_MAX_ZNODE,
            DEFAULT_TIMEOUT,
            DEFAULT_PENDING_TASKS_RUNNER_NUM_THREADS
        )).hashCode()
    );
    final Period timeout = Period.years(999);
    final String version = "someVersion";
    final long max = 20 * 1024;
    final int pendingTasksRunnerNumThreads = 20;
    Assert.assertEquals(
        reflect(generateRemoteTaskRunnerConfig(
            timeout,
            timeout,
            version,
            max,
            timeout,
            pendingTasksRunnerNumThreads
        )).hashCode(),
        reflect(generateRemoteTaskRunnerConfig(
            timeout,
            timeout,
            version,
            max,
            timeout,
            pendingTasksRunnerNumThreads
        )).hashCode()
    );
    Assert.assertNotEquals(
        reflect(generateRemoteTaskRunnerConfig(
            timeout,
            timeout,
            version,
            max,
            timeout,
            pendingTasksRunnerNumThreads
        )).hashCode(),
        reflect(generateRemoteTaskRunnerConfig(
            DEFAULT_TIMEOUT,
            timeout,
            version,
            max,
            timeout,
            pendingTasksRunnerNumThreads
        )).hashCode()
    );
    Assert.assertNotEquals(
        reflect(generateRemoteTaskRunnerConfig(
            timeout,
            timeout,
            version,
            max,
            timeout,
            pendingTasksRunnerNumThreads
        )).hashCode(),
        reflect(generateRemoteTaskRunnerConfig(
            timeout,
            DEFAULT_TIMEOUT,
            version,
            max,
            timeout,
            pendingTasksRunnerNumThreads
        )).hashCode()
    );
    Assert.assertNotEquals(
        reflect(generateRemoteTaskRunnerConfig(
            timeout,
            timeout,
            version,
            max,
            timeout,
            pendingTasksRunnerNumThreads
        )).hashCode(),
        reflect(generateRemoteTaskRunnerConfig(
            timeout,
            timeout,
            DEFAULT_VERSION,
            max,
            timeout,
            pendingTasksRunnerNumThreads
        )).hashCode()
    );

    Assert.assertNotEquals(
        reflect(generateRemoteTaskRunnerConfig(
            timeout,
            timeout,
            version,
            max,
            timeout,
            pendingTasksRunnerNumThreads
        )).hashCode(),
        reflect(generateRemoteTaskRunnerConfig(
            timeout,
            timeout,
            version,
            DEFAULT_MAX_ZNODE,
            timeout,
            pendingTasksRunnerNumThreads
        )).hashCode()
    );


    Assert.assertNotEquals(
        reflect(generateRemoteTaskRunnerConfig(
            timeout,
            timeout,
            version,
            max,
            timeout,
            pendingTasksRunnerNumThreads
        )).hashCode(),
        reflect(generateRemoteTaskRunnerConfig(
            timeout,
            timeout,
            version,
            max,
            DEFAULT_TIMEOUT,
            pendingTasksRunnerNumThreads
        )).hashCode()
    );

    Assert.assertNotEquals(
        reflect(generateRemoteTaskRunnerConfig(
                    timeout,
                    timeout,
                    version,
                    max,
                    timeout,
                    pendingTasksRunnerNumThreads
                )).hashCode(),
        reflect(generateRemoteTaskRunnerConfig(
                    timeout,
                    timeout,
                    version,
                    max,
                    timeout,
                    DEFAULT_PENDING_TASKS_RUNNER_NUM_THREADS
                )).hashCode()
    );
  }

  private RemoteTaskRunnerConfig reflect(RemoteTaskRunnerConfig config) throws IOException
  {
    return mapper.readValue(mapper.writeValueAsString(config), RemoteTaskRunnerConfig.class);
  }

  private RemoteTaskRunnerConfig generateRemoteTaskRunnerConfig(
      Period taskAssignmentTimeout,
      Period taskCleanupTimeout,
      String minWorkerVersion,
      long maxZnodeBytes,
      Period taskShutdownLinkTimeout,
      int pendingTasksRunnerNumThreads
  )
  {
    final Map<String, Object> objectMap = new HashMap<>();
    objectMap.put("taskAssignmentTimeout", taskAssignmentTimeout);
    objectMap.put("taskCleanupTimeout", taskCleanupTimeout);
    objectMap.put("minWorkerVersion", minWorkerVersion);
    objectMap.put("maxZnodeBytes", maxZnodeBytes);
    objectMap.put("taskShutdownLinkTimeout", taskShutdownLinkTimeout);
    objectMap.put("pendingTasksRunnerNumThreads", pendingTasksRunnerNumThreads);
    return mapper.convertValue(objectMap, RemoteTaskRunnerConfig.class);
  }
}
