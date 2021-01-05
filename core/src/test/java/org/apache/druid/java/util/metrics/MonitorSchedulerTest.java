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

package org.apache.druid.java.util.metrics;


import com.google.common.collect.ImmutableList;
import io.timeandspace.cronscheduler.CronScheduler;
import io.timeandspace.cronscheduler.CronTask;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class MonitorSchedulerTest
{
  
  @Mock
  private CronScheduler cronScheduler;
  
  @Before
  public void setUp()
  {
    MockitoAnnotations.initMocks(this);
  }
  
  @Test
  public void testFindMonitor()
  {
    class Monitor1 extends NoopMonitor
    {
    }
    class Monitor2 extends NoopMonitor
    {
    }
    class Monitor3 extends NoopMonitor
    {
    }

    final Monitor1 monitor1 = new Monitor1();
    final Monitor2 monitor2 = new Monitor2();
    
    ExecutorService executor = Mockito.mock(ExecutorService.class);

    final MonitorScheduler scheduler = new MonitorScheduler(
        Mockito.mock(MonitorSchedulerConfig.class),
        CronScheduler.newBuilder(Duration.ofSeconds(1L)).setThreadName("monitor-scheduler-test").build(),
        Mockito.mock(ServiceEmitter.class),
        ImmutableList.of(monitor1, monitor2),
        executor
    );

    final Optional<Monitor1> maybeFound1 = scheduler.findMonitor(Monitor1.class);
    final Optional<Monitor2> maybeFound2 = scheduler.findMonitor(Monitor2.class);
    Assert.assertTrue(maybeFound1.isPresent());
    Assert.assertTrue(maybeFound2.isPresent());
    Assert.assertSame(monitor1, maybeFound1.get());
    Assert.assertSame(monitor2, maybeFound2.get());

    Assert.assertFalse(scheduler.findMonitor(Monitor3.class).isPresent());
  }
  
  @Test
  public void testStart_RepeatScheduling() 
  {
    ExecutorService executor = Mockito.mock(ExecutorService.class);

    Mockito.doAnswer(new Answer<Future<?>>()
    {
      private int scheduleCount = 0;

      @SuppressWarnings("unchecked")
      @Override
      public Future<?> answer(InvocationOnMock invocation) throws Exception
      {
        final Object originalArgument = (invocation.getArguments())[3];
        CronTask task = ((CronTask) originalArgument);

        Mockito.doAnswer(new Answer<Future<?>>()
        {
          @Override
          public Future<Boolean> answer(InvocationOnMock invocation) throws Exception
          {
            final Object originalArgument = (invocation.getArguments())[0];
            ((Callable<Boolean>) originalArgument).call();
            return CompletableFuture.completedFuture(Boolean.TRUE);
          }
        }).when(executor).submit(ArgumentMatchers.any(Callable.class));

        while (scheduleCount < 2) {
          scheduleCount++;
          task.run(123L);
        }
        return createDummyFuture();
      }
    }).when(cronScheduler).scheduleAtFixedRate(ArgumentMatchers.anyLong(), ArgumentMatchers.anyLong(),
        ArgumentMatchers.any(), ArgumentMatchers.any(CronTask.class));

    Monitor monitor = Mockito.mock(Monitor.class);

    MonitorSchedulerConfig config = Mockito.mock(MonitorSchedulerConfig.class);
    Mockito.when(config.getEmitterPeriod()).thenReturn(new org.joda.time.Duration(1000L));

    final MonitorScheduler scheduler = new MonitorScheduler(
        config,
        cronScheduler,
        Mockito.mock(ServiceEmitter.class),
        ImmutableList.of(monitor),
        executor);
    scheduler.start();

    Mockito.verify(monitor, Mockito.times(1)).start();
    Mockito.verify(cronScheduler, Mockito.times(1)).scheduleAtFixedRate(ArgumentMatchers.anyLong(),
        ArgumentMatchers.anyLong(),
        ArgumentMatchers.any(), ArgumentMatchers.any(CronTask.class));
    Mockito.verify(executor, Mockito.times(2)).submit(ArgumentMatchers.any(Callable.class));
    Mockito.verify(monitor, Mockito.times(2)).monitor(ArgumentMatchers.any());

  }
  
  @Test
  public void testStart_RepeatAndStopScheduling() 
  {
    ExecutorService executor = Mockito.mock(ExecutorService.class);

    Mockito.doAnswer(new Answer<Future<?>>()
    {
      private int scheduleCount = 0;

      @SuppressWarnings("unchecked")
      @Override
      public Future<?> answer(InvocationOnMock invocation) throws Exception
      {
        final Object originalArgument = (invocation.getArguments())[3];
        CronTask task = ((CronTask) originalArgument);
        Mockito.doAnswer(new Answer<Future<?>>()
        {
          @Override
          public Future<Boolean> answer(InvocationOnMock invocation) throws Exception
          {
            final Object originalArgument = (invocation.getArguments())[0];
            ((Callable<Boolean>) originalArgument).call();
            return CompletableFuture.completedFuture(Boolean.FALSE);
          }
        }).when(executor).submit(ArgumentMatchers.any(Callable.class));

        while (scheduleCount < 2) {
          scheduleCount++;
          task.run(123L);
        }
        return createDummyFuture();
      }
    }).when(cronScheduler).scheduleAtFixedRate(ArgumentMatchers.anyLong(), ArgumentMatchers.anyLong(),
        ArgumentMatchers.any(), ArgumentMatchers.any(CronTask.class));

    Monitor monitor = Mockito.mock(Monitor.class);
    Mockito.when(monitor.getScheduledFuture()).thenReturn(createDummyFuture());

    MonitorSchedulerConfig config = Mockito.mock(MonitorSchedulerConfig.class);
    Mockito.when(config.getEmitterPeriod()).thenReturn(new org.joda.time.Duration(1000L));

    final MonitorScheduler scheduler = new MonitorScheduler(
        config,
        cronScheduler,
        Mockito.mock(ServiceEmitter.class),
        ImmutableList.of(monitor),
        executor);
    scheduler.start();
    
    Mockito.verify(monitor, Mockito.times(1)).start();
    Mockito.verify(cronScheduler, Mockito.times(1)).scheduleAtFixedRate(ArgumentMatchers.anyLong(),
        ArgumentMatchers.anyLong(),
        ArgumentMatchers.any(), ArgumentMatchers.any(CronTask.class));
    Mockito.verify(executor, Mockito.times(1)).submit(ArgumentMatchers.any(Callable.class));
    Mockito.verify(monitor, Mockito.times(1)).monitor(ArgumentMatchers.any());
    Mockito.verify(monitor, Mockito.times(1)).stop();
    
  }
  
  @Test
  public void testStart_UnexpectedExceptionWhileMonitoring() 
  {
    ExecutorService executor = Mockito.mock(ExecutorService.class);
    Monitor monitor = Mockito.mock(Monitor.class);
    Mockito.when(monitor.getScheduledFuture()).thenReturn(createDummyFuture());
    Mockito.when(monitor.monitor(ArgumentMatchers.any(ServiceEmitter.class))).thenThrow(new RuntimeException());

    MonitorSchedulerConfig config = Mockito.mock(MonitorSchedulerConfig.class);
    Mockito.when(config.getEmitterPeriod()).thenReturn(new org.joda.time.Duration(1000L));


    Mockito.doAnswer(new Answer<Future<?>>()
    {
      @SuppressWarnings("unchecked")
      @Override
      public Future<?> answer(InvocationOnMock invocation) throws Exception
      {
        final Object originalArgument = (invocation.getArguments())[3];
        CronTask task = ((CronTask) originalArgument);

        Mockito.doAnswer(new Answer<Future<?>>()
        {
          @Override
          public Future<Boolean> answer(InvocationOnMock invocation) throws Exception
          {
            final Object originalArgument = (invocation.getArguments())[0];
            ((Callable<Boolean>) originalArgument).call();
            return CompletableFuture.completedFuture(Boolean.TRUE);
          }
        }).when(executor).submit(ArgumentMatchers.any(Callable.class));

        task.run(123L);
        return createDummyFuture();
      }
    }).when(cronScheduler).scheduleAtFixedRate(ArgumentMatchers.anyLong(), ArgumentMatchers.anyLong(),
        ArgumentMatchers.any(), ArgumentMatchers.any(CronTask.class));

    
    final MonitorScheduler scheduler = new MonitorScheduler(
        config,
        cronScheduler,
        Mockito.mock(ServiceEmitter.class),
        ImmutableList.of(monitor),
        executor);
    scheduler.start();
    
    Mockito.verify(monitor, Mockito.times(1)).start();
    Mockito.verify(cronScheduler, Mockito.times(1)).scheduleAtFixedRate(ArgumentMatchers.anyLong(),
        ArgumentMatchers.anyLong(),
        ArgumentMatchers.any(), ArgumentMatchers.any(CronTask.class));
    Mockito.verify(executor, Mockito.times(1)).submit(ArgumentMatchers.any(Callable.class));
    Mockito.verify(monitor, Mockito.times(1)).monitor(ArgumentMatchers.any());
  }
  
  
  @Test
  public void testStart_UnexpectedExceptionWhileScheduling() 
  {
    ExecutorService executor = Mockito.mock(ExecutorService.class);
    Monitor monitor = Mockito.mock(Monitor.class);
    Mockito.when(monitor.getScheduledFuture()).thenReturn(createDummyFuture());
    MonitorSchedulerConfig config = Mockito.mock(MonitorSchedulerConfig.class);
    Mockito.when(config.getEmitterPeriod()).thenReturn(new org.joda.time.Duration(1000L));


    Mockito.doAnswer(new Answer<Future<?>>()
    {
      @SuppressWarnings("unchecked")
      @Override
      public Future<?> answer(InvocationOnMock invocation) throws Exception
      {
        final Object originalArgument = (invocation.getArguments())[3];
        CronTask task = ((CronTask) originalArgument);

        Mockito.when(executor.submit(ArgumentMatchers.any(Callable.class))).thenThrow(new RuntimeException());
        task.run(123L);
        return createDummyFuture();
      }
    }).when(cronScheduler).scheduleAtFixedRate(ArgumentMatchers.anyLong(), ArgumentMatchers.anyLong(),
        ArgumentMatchers.any(), ArgumentMatchers.any(CronTask.class));

    
    final MonitorScheduler scheduler = new MonitorScheduler(
        config,
        cronScheduler,
        Mockito.mock(ServiceEmitter.class),
        ImmutableList.of(monitor),
        executor);
    scheduler.start();
    
    Mockito.verify(monitor, Mockito.times(1)).start();
    Mockito.verify(cronScheduler, Mockito.times(1)).scheduleAtFixedRate(ArgumentMatchers.anyLong(),
        ArgumentMatchers.anyLong(),
        ArgumentMatchers.any(), ArgumentMatchers.any(CronTask.class));
    Mockito.verify(executor, Mockito.times(1)).submit(ArgumentMatchers.any(Callable.class));
  }
  
  
  private Future createDummyFuture()
  {
    Future<?> future = new Future()
    {

      @Override
      public boolean cancel(boolean mayInterruptIfRunning)
      {
        return false;
      }

      @Override
      public boolean isCancelled()
      {
        return false;
      }

      @Override
      public boolean isDone()
      {
        return false;
      }

      @Override
      public Object get()
      {
        return null;
      }

      @Override
      public Object get(long timeout, TimeUnit unit)
      {
        return null;
      }

    };

    return future;
  }
  
  
  private static class NoopMonitor implements Monitor
  {
    @Override
    public void start()
    {

    }

    @Override
    public void stop()
    {

    }

    @Override
    public boolean monitor(ServiceEmitter emitter)
    {
      return true;
    }

    @Override
    public Future<?> getScheduledFuture()
    {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public void setScheduledFuture(Future<?> scheduledFuture)
    {
      // TODO Auto-generated method stub
      
    }
  }
}
