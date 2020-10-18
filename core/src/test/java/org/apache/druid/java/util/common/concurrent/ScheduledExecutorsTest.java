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

package org.apache.druid.java.util.common.concurrent;

import io.timeandspace.cronscheduler.CronScheduler;
import org.apache.druid.java.util.common.concurrent.ScheduledExecutors.Signal;
import org.joda.time.Duration;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.concurrent.Callable;
import java.util.concurrent.Future;

public class ScheduledExecutorsTest
{

  @Mock
  private CronScheduler scheduler;

  @Mock
  private Callable<Signal> callable;

  @Before
  public void setUp()
  {
    MockitoAnnotations.initMocks(this);
  }

  @Test
  public void testScheduleAtFixedRateWithCronScheduler_SuccessWithRepeatSignal() throws Exception
  {
    Mockito.when(callable.call()).thenReturn(Signal.REPEAT);
    Mockito.doAnswer(new Answer<Future<?>>()
    {
      @Override
      public Future<?> answer(InvocationOnMock invocation)
      {
        final Object originalArgument = (invocation.getArguments())[1];
        // mimicking recursive scheduling with a loop
        for (int scheduledCount = 0; scheduledCount < 2; scheduledCount++) {
          Mockito.when(scheduler.scheduleAt(ArgumentMatchers.any(), ArgumentMatchers.any(Runnable.class)))
              .thenReturn(null);
          ((Runnable) originalArgument).run();
        }
        return null;
      }
    }).when(scheduler).scheduleAt(ArgumentMatchers.any(), ArgumentMatchers.any(Runnable.class));
    Duration rate = new Duration(2L);
    ScheduledExecutors.scheduleAtFixedRate(scheduler, rate, callable);
    Mockito.verify(scheduler, Mockito.times(3)).scheduleAt(ArgumentMatchers.any(),
        ArgumentMatchers.any(Runnable.class));
    Mockito.verify(callable, Mockito.times(2)).call();
  }

  @Test
  public void testScheduleAtFixedRateWithCronScheduler_SuccessWithStopSignal() throws Exception
  {
    Mockito.when(callable.call()).thenReturn(Signal.STOP);
    Mockito.doAnswer(new Answer<Future<?>>()
    {
      @Override
      public Future<?> answer(InvocationOnMock invocation)
      {
        final Object originalArgument = (invocation.getArguments())[1];
        // mimicking recursive scheduling with a loop
        for (int scheduledCount = 0; scheduledCount < 2; scheduledCount++) {
          Mockito.when(scheduler.scheduleAt(ArgumentMatchers.any(), ArgumentMatchers.any(Runnable.class)))
              .thenReturn(null);
          ((Runnable) originalArgument).run();
        }
        return null;
      }
    }).when(scheduler).scheduleAt(ArgumentMatchers.any(), ArgumentMatchers.any(Runnable.class));
    Duration rate = new Duration(2L);
    ScheduledExecutors.scheduleAtFixedRate(scheduler, rate, callable);
    Mockito.verify(scheduler, Mockito.times(2)).scheduleAt(ArgumentMatchers.any(),
        ArgumentMatchers.any(Runnable.class));
    Mockito.verify(callable, Mockito.times(2)).call();
  }

  @Test
  public void testScheduleAtFixedRateWithCronScheduler_UnexpectedFailure() throws Exception
  {
    Mockito.when(callable.call()).thenThrow(new IllegalArgumentException("Unexpected Exception"));
    Mockito.doAnswer(new Answer<Future<?>>()
    {
      @Override
      public Future<?> answer(InvocationOnMock invocation)
      {
        final Object originalArgument = (invocation.getArguments())[1];
        Mockito.when(scheduler.scheduleAt(ArgumentMatchers.any(), ArgumentMatchers.any(Runnable.class)))
            .thenReturn(null);
        ((Runnable) originalArgument).run();
        return null;
      }
    }).when(scheduler).scheduleAt(ArgumentMatchers.any(), ArgumentMatchers.any(Runnable.class));
    Duration rate = new Duration(2L);
    ScheduledExecutors.scheduleAtFixedRate(scheduler, rate, callable);
    Mockito.verify(scheduler, Mockito.times(2)).scheduleAt(ArgumentMatchers.any(),
        ArgumentMatchers.any(Runnable.class));
    Mockito.verify(callable, Mockito.times(1)).call();
  }

  
}
