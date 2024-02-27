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

package org.apache.druid.indexing.overlord.duty;

import com.google.common.collect.ImmutableSet;
import org.apache.druid.java.util.common.concurrent.ScheduledExecutorFactory;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.concurrent.ScheduledExecutorService;

public class OverlordDutyExecutorTest
{

  @Test
  public void testStartAndStop()
  {
    OverlordDuty testDuty1 = Mockito.mock(OverlordDuty.class);
    Mockito.when(testDuty1.isEnabled()).thenReturn(true);
    Mockito.when(testDuty1.getSchedule()).thenReturn(new DutySchedule(0, 0));

    OverlordDuty testDuty2 = Mockito.mock(OverlordDuty.class);
    Mockito.when(testDuty2.isEnabled()).thenReturn(true);
    Mockito.when(testDuty2.getSchedule()).thenReturn(new DutySchedule(0, 0));

    ScheduledExecutorFactory executorFactory = Mockito.mock(ScheduledExecutorFactory.class);
    ScheduledExecutorService executorService = Mockito.mock(ScheduledExecutorService.class);
    Mockito.when(executorFactory.create(ArgumentMatchers.eq(1), ArgumentMatchers.anyString()))
           .thenReturn(executorService);

    OverlordDutyExecutor dutyExecutor =
        new OverlordDutyExecutor(executorFactory, ImmutableSet.of(testDuty1, testDuty2));

    // Invoke start multiple times
    dutyExecutor.start();
    dutyExecutor.start();
    dutyExecutor.start();

    // Verify that executor is initialized and each duty is scheduled
    Mockito.verify(executorFactory, Mockito.times(1))
           .create(ArgumentMatchers.eq(1), ArgumentMatchers.anyString());
    Mockito.verify(executorService, Mockito.times(2)).schedule(
        ArgumentMatchers.any(Runnable.class),
        ArgumentMatchers.anyLong(),
        ArgumentMatchers.any()
    );

    // Invoke stop multiple times
    dutyExecutor.stop();
    dutyExecutor.stop();
    dutyExecutor.stop();

    // Verify that the executor shutdown is invoked just once
    Mockito.verify(executorService, Mockito.times(1)).shutdownNow();
  }

  @Test
  public void testStartWithNoEnabledDuty()
  {
    OverlordDuty testDuty = Mockito.mock(OverlordDuty.class);
    Mockito.when(testDuty.isEnabled()).thenReturn(false);

    ScheduledExecutorFactory executorFactory = Mockito.mock(ScheduledExecutorFactory.class);
    OverlordDutyExecutor dutyExecutor =
        new OverlordDutyExecutor(executorFactory, Collections.singleton(testDuty));

    dutyExecutor.start();

    // Verify that executor is not initialized as there is no enabled duty
    Mockito.verify(executorFactory, Mockito.never())
           .create(ArgumentMatchers.eq(1), ArgumentMatchers.anyString());
  }

}
