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

package org.apache.druid.rpc.indexing;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.druid.client.indexing.TaskStatusResponse;
import org.apache.druid.indexer.TaskLocation;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.indexer.TaskStatusPlus;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.rpc.ServiceLocation;
import org.apache.druid.rpc.ServiceLocations;
import org.hamcrest.CoreMatchers;
import org.hamcrest.MatcherAssert;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.internal.matchers.ThrowableMessageMatcher;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.mockito.quality.Strictness;

import java.util.Collections;
import java.util.concurrent.ExecutionException;

public class SpecificTaskServiceLocatorTest
{
  private static final String TASK_ID = "test-task";
  private static final TaskLocation TASK_LOCATION1 = TaskLocation.create("example.com", -1, 9998);
  private static final ServiceLocation SERVICE_LOCATION1 =
      new ServiceLocation("example.com", -1, 9998, "/druid/worker/v1/chat/test-task");

  @Rule
  public MockitoRule mockitoRule = MockitoJUnit.rule().strictness(Strictness.STRICT_STUBS);

  @Mock
  private OverlordClient overlordClient;

  @Test
  public void test_locate_noLocationYet() throws Exception
  {
    Mockito.when(overlordClient.taskStatus(TASK_ID))
           .thenReturn(makeResponse(TaskState.RUNNING, TaskLocation.unknown()));

    final SpecificTaskServiceLocator locator = new SpecificTaskServiceLocator(TASK_ID, overlordClient);
    final ListenableFuture<ServiceLocations> future = locator.locate();
    Assert.assertEquals(ServiceLocations.forLocations(Collections.emptySet()), future.get());
  }

  @Test
  public void test_locate_taskRunning() throws Exception
  {
    Mockito.when(overlordClient.taskStatus(TASK_ID))
           .thenReturn(makeResponse(TaskState.RUNNING, TASK_LOCATION1));

    final SpecificTaskServiceLocator locator = new SpecificTaskServiceLocator(TASK_ID, overlordClient);
    Assert.assertEquals(ServiceLocations.forLocation(SERVICE_LOCATION1), locator.locate().get());
  }

  @Test
  public void test_locate_taskNotFound() throws Exception
  {
    Mockito.when(overlordClient.taskStatus(TASK_ID))
           .thenReturn(Futures.immediateFuture(new TaskStatusResponse(TASK_ID, null)));

    final SpecificTaskServiceLocator locator = new SpecificTaskServiceLocator(TASK_ID, overlordClient);
    final ListenableFuture<ServiceLocations> future = locator.locate();
    Assert.assertEquals(ServiceLocations.closed(), future.get());
  }

  @Test
  public void test_locate_taskSuccess() throws Exception
  {
    Mockito.when(overlordClient.taskStatus(TASK_ID))
           .thenReturn(makeResponse(TaskState.SUCCESS, TaskLocation.unknown()));

    final SpecificTaskServiceLocator locator = new SpecificTaskServiceLocator(TASK_ID, overlordClient);
    final ListenableFuture<ServiceLocations> future = locator.locate();
    Assert.assertEquals(ServiceLocations.closed(), future.get());
  }

  @Test
  public void test_locate_taskFailed() throws Exception
  {
    Mockito.when(overlordClient.taskStatus(TASK_ID))
           .thenReturn(makeResponse(TaskState.FAILED, TaskLocation.unknown()));

    final SpecificTaskServiceLocator locator = new SpecificTaskServiceLocator(TASK_ID, overlordClient);
    final ListenableFuture<ServiceLocations> future = locator.locate();
    Assert.assertEquals(ServiceLocations.closed(), future.get());
  }

  @Test
  public void test_locate_overlordError()
  {
    Mockito.when(overlordClient.taskStatus(TASK_ID))
           .thenReturn(Futures.immediateFailedFuture(new ISE("oh no")));

    final SpecificTaskServiceLocator locator = new SpecificTaskServiceLocator(TASK_ID, overlordClient);
    final ListenableFuture<ServiceLocations> future = locator.locate();

    final ExecutionException e = Assert.assertThrows(
        ExecutionException.class,
        future::get
    );

    MatcherAssert.assertThat(e, ThrowableMessageMatcher.hasMessage(CoreMatchers.containsString("oh no")));
    MatcherAssert.assertThat(e.getCause(), CoreMatchers.instanceOf(IllegalStateException.class));
  }

  @Test
  public void test_locate_afterClose() throws Exception
  {
    // Overlord call will never return.
    final SettableFuture<TaskStatusResponse> overlordFuture = SettableFuture.create();
    Mockito.when(overlordClient.taskStatus(TASK_ID))
           .thenReturn(overlordFuture);

    final SpecificTaskServiceLocator locator = new SpecificTaskServiceLocator(TASK_ID, overlordClient);
    final ListenableFuture<ServiceLocations> future = locator.locate();
    locator.close();

    Assert.assertEquals(ServiceLocations.closed(), future.get()); // Call prior to close
    Assert.assertEquals(ServiceLocations.closed(), locator.locate().get()); // Call after close
    Assert.assertTrue(overlordFuture.isCancelled());
  }

  private static ListenableFuture<TaskStatusResponse> makeResponse(final TaskState state, final TaskLocation location)
  {
    final TaskStatusResponse response = new TaskStatusResponse(
        TASK_ID,
        new TaskStatusPlus(
            TASK_ID,
            null,
            null,
            DateTimes.utc(0),
            DateTimes.utc(0),
            state,
            null,
            null,
            1L,
            location,
            null,
            null
        )
    );

    return Futures.immediateFuture(response);
  }
}
