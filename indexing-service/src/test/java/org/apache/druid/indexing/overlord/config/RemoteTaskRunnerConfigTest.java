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

package org.apache.druid.indexing.overlord.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.guice.JsonConfigurator;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.joda.time.Period;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class RemoteTaskRunnerConfigTest
{
  private static final ObjectMapper MAPPER = new DefaultObjectMapper();
  private static final Period DEFAULT_TIMEOUT = Period.ZERO;
  private static final String DEFAULT_VERSION = "";
  private static final long DEFAULT_MAX_ZNODE = 10 * 1024;
  private static final int DEFAULT_PENDING_TASKS_RUNNER_NUM_THREADS = 5;
  private static final int DEFAULT_MAX_RETRIES_BEFORE_BLACKLIST = 5;
  private static final Period DEFAULT_TASK_BACKOFF = new Period("PT10M");
  private static final Period DEFAULT_BLACKLIST_CLEANUP_PERIOD = new Period("PT5M");

  @Test
  public void testIsJsonConfiguratable()
  {
    JsonConfigurator.verifyClazzIsConfigurable(MAPPER, RemoteTaskRunnerConfig.class, null);
  }

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
            DEFAULT_PENDING_TASKS_RUNNER_NUM_THREADS,
            DEFAULT_MAX_RETRIES_BEFORE_BLACKLIST,
            DEFAULT_TASK_BACKOFF,
            DEFAULT_BLACKLIST_CLEANUP_PERIOD
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
            pendingTasksRunnerNumThreads,
            DEFAULT_MAX_RETRIES_BEFORE_BLACKLIST,
            DEFAULT_TASK_BACKOFF,
            DEFAULT_BLACKLIST_CLEANUP_PERIOD
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
            DEFAULT_PENDING_TASKS_RUNNER_NUM_THREADS,
            DEFAULT_MAX_RETRIES_BEFORE_BLACKLIST,
            DEFAULT_TASK_BACKOFF,
            DEFAULT_BLACKLIST_CLEANUP_PERIOD
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
            DEFAULT_PENDING_TASKS_RUNNER_NUM_THREADS,
            DEFAULT_MAX_RETRIES_BEFORE_BLACKLIST,
            DEFAULT_TASK_BACKOFF,
            DEFAULT_BLACKLIST_CLEANUP_PERIOD
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
            DEFAULT_PENDING_TASKS_RUNNER_NUM_THREADS,
            DEFAULT_MAX_RETRIES_BEFORE_BLACKLIST,
            DEFAULT_TASK_BACKOFF,
            DEFAULT_BLACKLIST_CLEANUP_PERIOD
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
                    DEFAULT_PENDING_TASKS_RUNNER_NUM_THREADS,
                    DEFAULT_MAX_RETRIES_BEFORE_BLACKLIST,
                    DEFAULT_TASK_BACKOFF,
                    DEFAULT_BLACKLIST_CLEANUP_PERIOD
                )).getTaskCleanupTimeout()
    );
  }

  @Test
  public void testGetMaxRetriesBeforeBlacklist() throws Exception
  {
    final int maxRetriesBeforeBlacklist = 2;
    Assert.assertEquals(
            maxRetriesBeforeBlacklist,
            reflect(generateRemoteTaskRunnerConfig(
                    DEFAULT_TIMEOUT,
                    DEFAULT_TIMEOUT,
                    DEFAULT_VERSION,
                    DEFAULT_MAX_ZNODE,
                    DEFAULT_TIMEOUT,
                    DEFAULT_PENDING_TASKS_RUNNER_NUM_THREADS,
                    maxRetriesBeforeBlacklist,
                    DEFAULT_TASK_BACKOFF,
                    DEFAULT_BLACKLIST_CLEANUP_PERIOD
            )).getMaxRetriesBeforeBlacklist()
    );
  }

  @Test
  public void testGetWorkerBlackListBackoffTime() throws Exception
  {
    final Period taskBlackListBackoffTime = new Period("PT1M");
    Assert.assertEquals(
            taskBlackListBackoffTime,
            reflect(generateRemoteTaskRunnerConfig(
                    DEFAULT_TIMEOUT,
                    DEFAULT_TIMEOUT,
                    DEFAULT_VERSION,
                    DEFAULT_MAX_ZNODE,
                    DEFAULT_TIMEOUT,
                    DEFAULT_PENDING_TASKS_RUNNER_NUM_THREADS,
                    DEFAULT_MAX_RETRIES_BEFORE_BLACKLIST,
                    taskBlackListBackoffTime,
                    DEFAULT_BLACKLIST_CLEANUP_PERIOD
            )).getWorkerBlackListBackoffTime()
    );
  }

  @Test
  public void testGetTaskBlackListCleanupPeriod() throws Exception
  {
    final Period taskBlackListCleanupPeriod = Period.years(100);
    Assert.assertEquals(
            taskBlackListCleanupPeriod,
            reflect(generateRemoteTaskRunnerConfig(
                    DEFAULT_TIMEOUT,
                    DEFAULT_TIMEOUT,
                    DEFAULT_VERSION,
                    DEFAULT_MAX_ZNODE,
                    DEFAULT_TIMEOUT,
                    DEFAULT_PENDING_TASKS_RUNNER_NUM_THREADS,
                    DEFAULT_MAX_RETRIES_BEFORE_BLACKLIST,
                    DEFAULT_TASK_BACKOFF,
                    taskBlackListCleanupPeriod
            )).getWorkerBlackListCleanupPeriod()
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
            DEFAULT_PENDING_TASKS_RUNNER_NUM_THREADS,
            DEFAULT_MAX_RETRIES_BEFORE_BLACKLIST,
            DEFAULT_TASK_BACKOFF,
            DEFAULT_BLACKLIST_CLEANUP_PERIOD
        )),
        reflect(generateRemoteTaskRunnerConfig(
            DEFAULT_TIMEOUT,
            DEFAULT_TIMEOUT,
            DEFAULT_VERSION,
            DEFAULT_MAX_ZNODE,
            DEFAULT_TIMEOUT,
            DEFAULT_PENDING_TASKS_RUNNER_NUM_THREADS,
            DEFAULT_MAX_RETRIES_BEFORE_BLACKLIST,
            DEFAULT_TASK_BACKOFF,
            DEFAULT_BLACKLIST_CLEANUP_PERIOD
        ))
    );
    final Period timeout = Period.years(999);
    final String version = "someVersion";
    final long max = 20 * 1024;
    final int pendingTasksRunnerNumThreads = 20;
    final int maxRetriesBeforeBlacklist = 1;
    final Period taskBlackListBackoffTime = new Period("PT1M");
    final Period taskBlackListCleanupPeriod = Period.years(10);
    Assert.assertEquals(
        reflect(generateRemoteTaskRunnerConfig(
            timeout,
            timeout,
            version,
            max,
            timeout,
            pendingTasksRunnerNumThreads,
            maxRetriesBeforeBlacklist,
            taskBlackListBackoffTime,
            taskBlackListCleanupPeriod
        )),
        reflect(generateRemoteTaskRunnerConfig(
            timeout,
            timeout,
            version,
            max,
            timeout,
            pendingTasksRunnerNumThreads,
            maxRetriesBeforeBlacklist,
            taskBlackListBackoffTime,
            taskBlackListCleanupPeriod
        ))
    );
    Assert.assertNotEquals(
        reflect(generateRemoteTaskRunnerConfig(
            timeout,
            timeout,
            version,
            max,
            timeout,
            pendingTasksRunnerNumThreads,
            maxRetriesBeforeBlacklist,
            taskBlackListBackoffTime,
            taskBlackListCleanupPeriod
        )),
        reflect(generateRemoteTaskRunnerConfig(
            DEFAULT_TIMEOUT,
            timeout,
            version,
            max,
            timeout,
            pendingTasksRunnerNumThreads,
            maxRetriesBeforeBlacklist,
            taskBlackListBackoffTime,
            taskBlackListCleanupPeriod
        ))
    );
    Assert.assertNotEquals(
        reflect(generateRemoteTaskRunnerConfig(
            timeout,
            timeout,
            version,
            max,
            timeout,
            pendingTasksRunnerNumThreads,
            maxRetriesBeforeBlacklist,
            taskBlackListBackoffTime,
            taskBlackListCleanupPeriod
        )),
        reflect(generateRemoteTaskRunnerConfig(
            timeout,
            DEFAULT_TIMEOUT,
            version,
            max,
            timeout,
            pendingTasksRunnerNumThreads,
            maxRetriesBeforeBlacklist,
            taskBlackListBackoffTime,
            taskBlackListCleanupPeriod
        ))
    );
    Assert.assertNotEquals(
        reflect(generateRemoteTaskRunnerConfig(
            timeout,
            timeout,
            version,
            max,
            timeout,
            pendingTasksRunnerNumThreads,
            maxRetriesBeforeBlacklist,
            taskBlackListBackoffTime,
            taskBlackListCleanupPeriod
        )),
        reflect(generateRemoteTaskRunnerConfig(
            timeout,
            timeout,
            DEFAULT_VERSION,
            max,
            timeout,
            pendingTasksRunnerNumThreads,
            maxRetriesBeforeBlacklist,
            taskBlackListBackoffTime,
            taskBlackListCleanupPeriod
        ))
    );

    Assert.assertNotEquals(
        reflect(generateRemoteTaskRunnerConfig(
            timeout,
            timeout,
            version,
            max,
            timeout,
            pendingTasksRunnerNumThreads,
            maxRetriesBeforeBlacklist,
            taskBlackListBackoffTime,
            taskBlackListCleanupPeriod
        )),
        reflect(generateRemoteTaskRunnerConfig(
            timeout,
            timeout,
            version,
            DEFAULT_MAX_ZNODE,
            timeout,
            pendingTasksRunnerNumThreads,
            maxRetriesBeforeBlacklist,
            taskBlackListBackoffTime,
            taskBlackListCleanupPeriod
        ))
    );


    Assert.assertNotEquals(
        reflect(generateRemoteTaskRunnerConfig(
            timeout,
            timeout,
            version,
            max,
            timeout,
            pendingTasksRunnerNumThreads,
            maxRetriesBeforeBlacklist,
            taskBlackListBackoffTime,
            taskBlackListCleanupPeriod
        )),
        reflect(generateRemoteTaskRunnerConfig(
            timeout,
            timeout,
            version,
            max,
            DEFAULT_TIMEOUT,
            pendingTasksRunnerNumThreads,
            maxRetriesBeforeBlacklist,
            taskBlackListBackoffTime,
            taskBlackListCleanupPeriod
        ))
    );

    Assert.assertNotEquals(
        reflect(generateRemoteTaskRunnerConfig(
                    timeout,
                    timeout,
                    version,
                    max,
                    timeout,
                    pendingTasksRunnerNumThreads,
                    maxRetriesBeforeBlacklist,
                    taskBlackListBackoffTime,
                    taskBlackListCleanupPeriod
                )),
        reflect(generateRemoteTaskRunnerConfig(
                    timeout,
                    timeout,
                    version,
                    max,
                    timeout,
                    DEFAULT_PENDING_TASKS_RUNNER_NUM_THREADS,
                    maxRetriesBeforeBlacklist,
                    taskBlackListBackoffTime,
                    taskBlackListCleanupPeriod
                ))
    );

    Assert.assertNotEquals(
            reflect(generateRemoteTaskRunnerConfig(
                    timeout,
                    timeout,
                    version,
                    max,
                    timeout,
                    pendingTasksRunnerNumThreads,
                    maxRetriesBeforeBlacklist,
                    taskBlackListBackoffTime,
                    taskBlackListCleanupPeriod
            )),
            reflect(generateRemoteTaskRunnerConfig(
                    timeout,
                    timeout,
                    version,
                    max,
                    timeout,
                    pendingTasksRunnerNumThreads,
                    DEFAULT_MAX_RETRIES_BEFORE_BLACKLIST,
                    taskBlackListBackoffTime,
                    taskBlackListCleanupPeriod
            ))
    );

    Assert.assertNotEquals(
            reflect(generateRemoteTaskRunnerConfig(
                    timeout,
                    timeout,
                    version,
                    max,
                    timeout,
                    pendingTasksRunnerNumThreads,
                    maxRetriesBeforeBlacklist,
                    taskBlackListBackoffTime,
                    taskBlackListCleanupPeriod
            )),
            reflect(generateRemoteTaskRunnerConfig(
                    timeout,
                    timeout,
                    version,
                    max,
                    timeout,
                    pendingTasksRunnerNumThreads,
                    maxRetriesBeforeBlacklist,
                    DEFAULT_TASK_BACKOFF,
                    taskBlackListCleanupPeriod
            ))
    );

    Assert.assertNotEquals(
            reflect(generateRemoteTaskRunnerConfig(
                    timeout,
                    timeout,
                    version,
                    max,
                    timeout,
                    pendingTasksRunnerNumThreads,
                    maxRetriesBeforeBlacklist,
                    taskBlackListBackoffTime,
                    taskBlackListCleanupPeriod
            )),
            reflect(generateRemoteTaskRunnerConfig(
                    timeout,
                    timeout,
                    version,
                    max,
                    timeout,
                    pendingTasksRunnerNumThreads,
                    maxRetriesBeforeBlacklist,
                    taskBlackListBackoffTime,
                    DEFAULT_BLACKLIST_CLEANUP_PERIOD
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
            DEFAULT_PENDING_TASKS_RUNNER_NUM_THREADS,
            DEFAULT_MAX_RETRIES_BEFORE_BLACKLIST,
            DEFAULT_TASK_BACKOFF,
            DEFAULT_BLACKLIST_CLEANUP_PERIOD
        )).hashCode(),
        reflect(generateRemoteTaskRunnerConfig(
            DEFAULT_TIMEOUT,
            DEFAULT_TIMEOUT,
            DEFAULT_VERSION,
            DEFAULT_MAX_ZNODE,
            DEFAULT_TIMEOUT,
            DEFAULT_PENDING_TASKS_RUNNER_NUM_THREADS,
            DEFAULT_MAX_RETRIES_BEFORE_BLACKLIST,
            DEFAULT_TASK_BACKOFF,
            DEFAULT_BLACKLIST_CLEANUP_PERIOD
        )).hashCode()
    );
    final Period timeout = Period.years(999);
    final String version = "someVersion";
    final long max = 20 * 1024;
    final int pendingTasksRunnerNumThreads = 20;
    final int maxRetriesBeforeBlacklist = 80;
    final Period taskBlackListBackoffTime = new Period("PT1M");
    final Period taskBlackListCleanupPeriod = Period.years(10);
    Assert.assertEquals(
        reflect(generateRemoteTaskRunnerConfig(
            timeout,
            timeout,
            version,
            max,
            timeout,
            pendingTasksRunnerNumThreads,
            maxRetriesBeforeBlacklist,
            taskBlackListBackoffTime,
            taskBlackListCleanupPeriod
        )).hashCode(),
        reflect(generateRemoteTaskRunnerConfig(
            timeout,
            timeout,
            version,
            max,
            timeout,
            pendingTasksRunnerNumThreads,
            maxRetriesBeforeBlacklist,
            taskBlackListBackoffTime,
            taskBlackListCleanupPeriod
        )).hashCode()
    );
    Assert.assertNotEquals(
        reflect(generateRemoteTaskRunnerConfig(
            timeout,
            timeout,
            version,
            max,
            timeout,
            pendingTasksRunnerNumThreads,
            maxRetriesBeforeBlacklist,
            taskBlackListBackoffTime,
            taskBlackListCleanupPeriod
        )).hashCode(),
        reflect(generateRemoteTaskRunnerConfig(
            DEFAULT_TIMEOUT,
            timeout,
            version,
            max,
            timeout,
            pendingTasksRunnerNumThreads,
            maxRetriesBeforeBlacklist,
            taskBlackListBackoffTime,
            taskBlackListCleanupPeriod
        )).hashCode()
    );
    Assert.assertNotEquals(
        reflect(generateRemoteTaskRunnerConfig(
            timeout,
            timeout,
            version,
            max,
            timeout,
            pendingTasksRunnerNumThreads,
            maxRetriesBeforeBlacklist,
            taskBlackListBackoffTime,
            taskBlackListCleanupPeriod
        )).hashCode(),
        reflect(generateRemoteTaskRunnerConfig(
            timeout,
            DEFAULT_TIMEOUT,
            version,
            max,
            timeout,
            pendingTasksRunnerNumThreads,
            maxRetriesBeforeBlacklist,
            taskBlackListBackoffTime,
            taskBlackListCleanupPeriod
        )).hashCode()
    );
    Assert.assertNotEquals(
        reflect(generateRemoteTaskRunnerConfig(
            timeout,
            timeout,
            version,
            max,
            timeout,
            pendingTasksRunnerNumThreads,
            maxRetriesBeforeBlacklist,
            taskBlackListBackoffTime,
            taskBlackListCleanupPeriod
        )).hashCode(),
        reflect(generateRemoteTaskRunnerConfig(
            timeout,
            timeout,
            DEFAULT_VERSION,
            max,
            timeout,
            pendingTasksRunnerNumThreads,
            maxRetriesBeforeBlacklist,
            taskBlackListBackoffTime,
            taskBlackListCleanupPeriod
        )).hashCode()
    );

    Assert.assertNotEquals(
        reflect(generateRemoteTaskRunnerConfig(
            timeout,
            timeout,
            version,
            max,
            timeout,
            pendingTasksRunnerNumThreads,
            maxRetriesBeforeBlacklist,
            taskBlackListBackoffTime,
            taskBlackListCleanupPeriod
        )).hashCode(),
        reflect(generateRemoteTaskRunnerConfig(
            timeout,
            timeout,
            version,
            DEFAULT_MAX_ZNODE,
            timeout,
            pendingTasksRunnerNumThreads,
            maxRetriesBeforeBlacklist,
            taskBlackListBackoffTime,
            taskBlackListCleanupPeriod
        )).hashCode()
    );


    Assert.assertNotEquals(
        reflect(generateRemoteTaskRunnerConfig(
            timeout,
            timeout,
            version,
            max,
            timeout,
            pendingTasksRunnerNumThreads,
            maxRetriesBeforeBlacklist,
            taskBlackListBackoffTime,
            taskBlackListCleanupPeriod
        )).hashCode(),
        reflect(generateRemoteTaskRunnerConfig(
            timeout,
            timeout,
            version,
            max,
            DEFAULT_TIMEOUT,
            pendingTasksRunnerNumThreads,
            maxRetriesBeforeBlacklist,
            taskBlackListBackoffTime,
            taskBlackListCleanupPeriod
        )).hashCode()
    );

    Assert.assertNotEquals(
        reflect(generateRemoteTaskRunnerConfig(
                    timeout,
                    timeout,
                    version,
                    max,
                    timeout,
                    pendingTasksRunnerNumThreads,
                    maxRetriesBeforeBlacklist,
                    taskBlackListBackoffTime,
                    taskBlackListCleanupPeriod
                )).hashCode(),
        reflect(generateRemoteTaskRunnerConfig(
                    timeout,
                    timeout,
                    version,
                    max,
                    timeout,
                    DEFAULT_PENDING_TASKS_RUNNER_NUM_THREADS,
                    maxRetriesBeforeBlacklist,
                    taskBlackListBackoffTime,
                    taskBlackListCleanupPeriod
                )).hashCode()
    );

    Assert.assertNotEquals(
            reflect(generateRemoteTaskRunnerConfig(
                    timeout,
                    timeout,
                    version,
                    max,
                    timeout,
                    pendingTasksRunnerNumThreads,
                    maxRetriesBeforeBlacklist,
                    taskBlackListBackoffTime,
                    taskBlackListCleanupPeriod
            )).hashCode(),
            reflect(generateRemoteTaskRunnerConfig(
                    timeout,
                    timeout,
                    version,
                    max,
                    timeout,
                    pendingTasksRunnerNumThreads,
                    DEFAULT_MAX_RETRIES_BEFORE_BLACKLIST,
                    taskBlackListBackoffTime,
                    taskBlackListCleanupPeriod
            )).hashCode()
    );

    Assert.assertNotEquals(
            reflect(generateRemoteTaskRunnerConfig(
                    timeout,
                    timeout,
                    version,
                    max,
                    timeout,
                    pendingTasksRunnerNumThreads,
                    maxRetriesBeforeBlacklist,
                    taskBlackListBackoffTime,
                    taskBlackListCleanupPeriod
            )).hashCode(),
            reflect(generateRemoteTaskRunnerConfig(
                    timeout,
                    timeout,
                    version,
                    max,
                    timeout,
                    pendingTasksRunnerNumThreads,
                    maxRetriesBeforeBlacklist,
                    DEFAULT_TASK_BACKOFF,
                    taskBlackListCleanupPeriod
            )).hashCode()
    );

    Assert.assertNotEquals(
            reflect(generateRemoteTaskRunnerConfig(
                    timeout,
                    timeout,
                    version,
                    max,
                    timeout,
                    pendingTasksRunnerNumThreads,
                    maxRetriesBeforeBlacklist,
                    taskBlackListBackoffTime,
                    taskBlackListCleanupPeriod
            )).hashCode(),
            reflect(generateRemoteTaskRunnerConfig(
                    timeout,
                    timeout,
                    version,
                    max,
                    timeout,
                    pendingTasksRunnerNumThreads,
                    maxRetriesBeforeBlacklist,
                    taskBlackListBackoffTime,
                    DEFAULT_BLACKLIST_CLEANUP_PERIOD
            )).hashCode()
    );
  }

  private RemoteTaskRunnerConfig reflect(RemoteTaskRunnerConfig config) throws IOException
  {
    return MAPPER.readValue(MAPPER.writeValueAsString(config), RemoteTaskRunnerConfig.class);
  }

  private RemoteTaskRunnerConfig generateRemoteTaskRunnerConfig(
      Period taskAssignmentTimeout,
      Period taskCleanupTimeout,
      String minWorkerVersion,
      long maxZnodeBytes,
      Period taskShutdownLinkTimeout,
      int pendingTasksRunnerNumThreads,
      int maxRetriesBeforeBlacklist,
      Period taskBlackListBackoffTime,
      Period taskBlackListCleanupPeriod
  )
  {
    final Map<String, Object> objectMap = new HashMap<>();
    objectMap.put("taskAssignmentTimeout", taskAssignmentTimeout);
    objectMap.put("taskCleanupTimeout", taskCleanupTimeout);
    objectMap.put("minWorkerVersion", minWorkerVersion);
    objectMap.put("maxZnodeBytes", maxZnodeBytes);
    objectMap.put("taskShutdownLinkTimeout", taskShutdownLinkTimeout);
    objectMap.put("pendingTasksRunnerNumThreads", pendingTasksRunnerNumThreads);
    objectMap.put("maxRetriesBeforeBlacklist", maxRetriesBeforeBlacklist);
    objectMap.put("workerBlackListBackoffTime", taskBlackListBackoffTime);
    objectMap.put("workerBlackListCleanupPeriod", taskBlackListCleanupPeriod);
    return MAPPER.convertValue(objectMap, RemoteTaskRunnerConfig.class);
  }
}
