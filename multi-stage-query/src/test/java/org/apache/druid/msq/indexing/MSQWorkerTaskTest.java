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

package org.apache.druid.msq.indexing;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.error.DruidException;
import org.apache.druid.server.lookup.cache.LookupLoadingSpec;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

public class MSQWorkerTaskTest
{

  private final String controllerTaskId = "ctr";
  private final String dataSource = "ds";
  private final int workerNumber = 1;
  private final ImmutableMap<String, Object> context = ImmutableMap.of("key", "val");
  private final int retryCount = 0;

  private final MSQWorkerTask msqWorkerTask = new MSQWorkerTask(
      controllerTaskId,
      dataSource,
      workerNumber,
      context,
      retryCount
  );


  @Test
  public void testEquals()
  {
    Assert.assertEquals(
        msqWorkerTask,
        new MSQWorkerTask(controllerTaskId, dataSource, workerNumber, context, retryCount)
    );
    Assert.assertEquals(
        msqWorkerTask.getRetryTask(),
        new MSQWorkerTask(controllerTaskId, dataSource, workerNumber, context, retryCount + 1)
    );
    Assert.assertNotEquals(msqWorkerTask, msqWorkerTask.getRetryTask());
  }

  @Test
  public void testHashCode()
  {
    Set<MSQWorkerTask> msqWorkerTaskSet = new HashSet<>();

    msqWorkerTaskSet.add(msqWorkerTask);
    msqWorkerTaskSet.add(new MSQWorkerTask(controllerTaskId, dataSource, workerNumber, context, retryCount));
    Assert.assertTrue(msqWorkerTaskSet.size() == 1);

    msqWorkerTaskSet.add(msqWorkerTask.getRetryTask());
    Assert.assertTrue(msqWorkerTaskSet.size() == 2);

    msqWorkerTaskSet.add(new MSQWorkerTask(controllerTaskId + 1, dataSource, workerNumber, context, retryCount));
    Assert.assertTrue(msqWorkerTaskSet.size() == 3);

    msqWorkerTaskSet.add(new MSQWorkerTask(controllerTaskId, dataSource + 1, workerNumber, context, retryCount));
    Assert.assertTrue(msqWorkerTaskSet.size() == 4);

    msqWorkerTaskSet.add(new MSQWorkerTask(controllerTaskId, dataSource, workerNumber + 1, context, retryCount));
    Assert.assertTrue(msqWorkerTaskSet.size() == 5);

    msqWorkerTaskSet.add(new MSQWorkerTask(
        controllerTaskId,
        dataSource,
        workerNumber,
        ImmutableMap.of("key1", "v1"),
        retryCount
    ));
    Assert.assertTrue(msqWorkerTaskSet.size() == 6);

    msqWorkerTaskSet.add(new MSQWorkerTask(controllerTaskId, dataSource, workerNumber, context, retryCount + 1));
    Assert.assertTrue(msqWorkerTaskSet.size() == 6);
  }

  @Test
  public void testGetter()
  {
    Assert.assertEquals(controllerTaskId, msqWorkerTask.getControllerTaskId());
    Assert.assertEquals(dataSource, msqWorkerTask.getDataSource());
    Assert.assertEquals(workerNumber, msqWorkerTask.getWorkerNumber());
    Assert.assertEquals(retryCount, msqWorkerTask.getRetryCount());
  }

  @Test
  public void testGetInputSourceResources()
  {
    MSQWorkerTask msqWorkerTask = new MSQWorkerTask(controllerTaskId, dataSource, workerNumber, context, retryCount);
    Assert.assertTrue(msqWorkerTask.getInputSourceResources().isEmpty());
  }

  @Test
  public void testGetDefaultLookupLoadingSpec()
  {
    MSQWorkerTask msqWorkerTask = new MSQWorkerTask(controllerTaskId, dataSource, workerNumber, context, retryCount);
    Assert.assertEquals(LookupLoadingSpec.ALL, msqWorkerTask.getLookupLoadingSpec());
  }

  @Test
  public void testGetLookupLoadingWithModeNoneInContext()
  {
    final ImmutableMap<String, Object> context = ImmutableMap.of(LookupLoadingSpec.CTX_LOOKUP_LOADING_MODE, LookupLoadingSpec.Mode.NONE);
    MSQWorkerTask msqWorkerTask = new MSQWorkerTask(controllerTaskId, dataSource, workerNumber, context, retryCount);
    Assert.assertEquals(LookupLoadingSpec.NONE, msqWorkerTask.getLookupLoadingSpec());
  }

  @Test
  public void testGetLookupLoadingSpecWithLookupListInContext()
  {
    final ImmutableMap<String, Object> context = ImmutableMap.of(
        LookupLoadingSpec.CTX_LOOKUPS_TO_LOAD, Arrays.asList("lookupName1", "lookupName2"),
        LookupLoadingSpec.CTX_LOOKUP_LOADING_MODE, LookupLoadingSpec.Mode.ONLY_REQUIRED);
    MSQWorkerTask msqWorkerTask = new MSQWorkerTask(controllerTaskId, dataSource, workerNumber, context, retryCount);
    Assert.assertEquals(LookupLoadingSpec.Mode.ONLY_REQUIRED, msqWorkerTask.getLookupLoadingSpec().getMode());
    Assert.assertEquals(ImmutableSet.of("lookupName1", "lookupName2"), msqWorkerTask.getLookupLoadingSpec().getLookupsToLoad());
  }

  @Test
  public void testGetLookupLoadingSpecWithInvalidInput()
  {
    final HashMap<String, Object> context = new HashMap<>();
    context.put(LookupLoadingSpec.CTX_LOOKUP_LOADING_MODE, LookupLoadingSpec.Mode.ONLY_REQUIRED);

    // Setting CTX_LOOKUPS_TO_LOAD as null
    context.put(LookupLoadingSpec.CTX_LOOKUPS_TO_LOAD, null);

    MSQWorkerTask taskWithNullLookups = new MSQWorkerTask(controllerTaskId, dataSource, workerNumber, context, retryCount);
    DruidException exception = Assert.assertThrows(
        DruidException.class,
        taskWithNullLookups::getLookupLoadingSpec
    );
    Assert.assertEquals(
        "Set of lookups to load cannot be null for mode[ONLY_REQUIRED].",
        exception.getMessage());

    // Setting CTX_LOOKUPS_TO_LOAD as empty list
    context.put(LookupLoadingSpec.CTX_LOOKUPS_TO_LOAD, Collections.emptyList());

    MSQWorkerTask taskWithEmptyLookups = new MSQWorkerTask(controllerTaskId, dataSource, workerNumber, context, retryCount);
    exception = Assert.assertThrows(
        DruidException.class,
        taskWithEmptyLookups::getLookupLoadingSpec
    );
    Assert.assertEquals(
        "Set of lookups to load cannot be [] for mode[ONLY_REQUIRED].",
        exception.getMessage());
  }
}
