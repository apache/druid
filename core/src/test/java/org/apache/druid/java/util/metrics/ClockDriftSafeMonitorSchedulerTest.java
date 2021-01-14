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
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.junit.After;
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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class ClockDriftSafeMonitorSchedulerTest
{
  // A real executor service to execute CronTask asynchronously.
  // Many tests in this class use mocks to easily control the behavior of CronScheduler and the ExecutorService
  // used by MonitorScheduler. However, as MonitorScheduler uses two differnt threads in production, one for
  // scheduling a task to schedule a monitor (CronScheduler), and another for running a scheduled monitor
  // asynchronously, these tests also require to run some tasks in an asynchronous manner. As mocks are convenient
  // enough to control the behavior of things, we use another executorService only to run some tasks asynchronously
  // to mimic the nature of asynchronous execution in MonitorScheduler.
  private ExecutorService cronTaskRunner;
  @Mock
  private CronScheduler cronScheduler;
  
  @Before
  public void setUp()
  {
    cronTaskRunner = Execs.singleThreaded("monitor-scheduler-test");
    MockitoAnnotations.initMocks(this);
  }

  @After
  public void tearDown()
  {
    cronTaskRunner.shutdownNow();
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

    final MonitorScheduler scheduler = new ClockDriftSafeMonitorScheduler(
        Mockito.mock(MonitorSchedulerConfig.class),
        Mockito.mock(ServiceEmitter.class),
        ImmutableList.of(monitor1, monitor2),
        CronScheduler.newBuilder(Duration.ofSeconds(1L)).setThreadName("monitor-scheduler-test").build(),
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
  public void testStart_RepeatScheduling() throws InterruptedException
  {
    ExecutorService executor = Mockito.mock(ExecutorService.class);

    CountDownLatch latch = new CountDownLatch(1);
    Mockito.doAnswer(new Answer<Future<?>>()
    {
      private int scheduleCount = 0;

      @SuppressWarnings("unchecked")
      @Override
      public Future<?> answer(InvocationOnMock invocation)
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

        cronTaskRunner.submit(() -> {
          while (scheduleCount < 2) {
            scheduleCount++;
            task.run(123L);
          }
          latch.countDown();
          return null;
        });
        return createDummyFuture();
      }
    }).when(cronScheduler).scheduleAtFixedRate(ArgumentMatchers.anyLong(), ArgumentMatchers.anyLong(),
        ArgumentMatchers.any(), ArgumentMatchers.any(CronTask.class));

    Monitor monitor = Mockito.mock(Monitor.class);

    MonitorSchedulerConfig config = Mockito.mock(MonitorSchedulerConfig.class);
    Mockito.when(config.getEmitterPeriod()).thenReturn(new org.joda.time.Duration(1000L));

    final MonitorScheduler scheduler = new ClockDriftSafeMonitorScheduler(
        config,
        Mockito.mock(ServiceEmitter.class),
        ImmutableList.of(monitor),
        cronScheduler,
        executor
    );
    scheduler.start();
    latch.await(5, TimeUnit.SECONDS);

    Mockito.verify(monitor, Mockito.times(1)).start();
    Mockito.verify(cronScheduler, Mockito.times(1)).scheduleAtFixedRate(ArgumentMatchers.anyLong(),
        ArgumentMatchers.anyLong(),
        ArgumentMatchers.any(), ArgumentMatchers.any(CronTask.class));
    Mockito.verify(executor, Mockito.times(2)).submit(ArgumentMatchers.any(Callable.class));
    Mockito.verify(monitor, Mockito.times(2)).monitor(ArgumentMatchers.any());
    scheduler.stop();
  }
  
  @Test
  public void testStart_RepeatAndStopScheduling() throws InterruptedException
  {
    ExecutorService executor = Mockito.mock(ExecutorService.class);

    CountDownLatch latch = new CountDownLatch(1);
    Mockito.doAnswer(new Answer<Future<?>>()
    {
      private int scheduleCount = 0;

      @SuppressWarnings("unchecked")
      @Override
      public Future<?> answer(InvocationOnMock invocation)
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

        cronTaskRunner.submit(() -> {
          while (scheduleCount < 2) {
            scheduleCount++;
            task.run(123L);
          }
          latch.countDown();
          return null;
        });
        return createDummyFuture();
      }
    }).when(cronScheduler).scheduleAtFixedRate(ArgumentMatchers.anyLong(), ArgumentMatchers.anyLong(),
        ArgumentMatchers.any(), ArgumentMatchers.any(CronTask.class));

    Monitor monitor = Mockito.mock(Monitor.class);

    MonitorSchedulerConfig config = Mockito.mock(MonitorSchedulerConfig.class);
    Mockito.when(config.getEmitterPeriod()).thenReturn(new org.joda.time.Duration(1000L));

    final MonitorScheduler scheduler = new ClockDriftSafeMonitorScheduler(
        config,
        Mockito.mock(ServiceEmitter.class),
        ImmutableList.of(monitor),
        cronScheduler,
        executor
    );
    scheduler.start();
    latch.await(5, TimeUnit.SECONDS);

    Mockito.verify(monitor, Mockito.times(1)).start();
    Mockito.verify(cronScheduler, Mockito.times(1)).scheduleAtFixedRate(ArgumentMatchers.anyLong(),
        ArgumentMatchers.anyLong(),
        ArgumentMatchers.any(), ArgumentMatchers.any(CronTask.class));
    Mockito.verify(executor, Mockito.times(1)).submit(ArgumentMatchers.any(Callable.class));
    Mockito.verify(monitor, Mockito.times(1)).monitor(ArgumentMatchers.any());
    Mockito.verify(monitor, Mockito.times(1)).stop();
    scheduler.stop();
  }
  
  @Test
  public void testStart_UnexpectedExceptionWhileMonitoring() throws InterruptedException
  {
    ExecutorService executor = Mockito.mock(ExecutorService.class);
    Monitor monitor = Mockito.mock(Monitor.class);
    Mockito.when(monitor.monitor(ArgumentMatchers.any(ServiceEmitter.class)))
           .thenThrow(new RuntimeException("Test throwing exception while monitoring"));

    MonitorSchedulerConfig config = Mockito.mock(MonitorSchedulerConfig.class);
    Mockito.when(config.getEmitterPeriod()).thenReturn(new org.joda.time.Duration(1000L));

    CountDownLatch latch = new CountDownLatch(1);
    AtomicBoolean monitorResultHolder = new AtomicBoolean(false);
    Mockito.doAnswer(new Answer<Future<?>>()
    {
      @SuppressWarnings("unchecked")
      @Override
      public Future<?> answer(InvocationOnMock invocation)
      {
        final Object originalArgument = (invocation.getArguments())[3];
        CronTask task = ((CronTask) originalArgument);

        Mockito.doAnswer(new Answer<Future<?>>()
        {
          @Override
          public Future<Boolean> answer(InvocationOnMock invocation) throws Exception
          {
            final Object originalArgument = (invocation.getArguments())[0];
            final boolean continueMonitor = ((Callable<Boolean>) originalArgument).call();
            monitorResultHolder.set(continueMonitor);
            return CompletableFuture.completedFuture(continueMonitor);
          }
        }).when(executor).submit(ArgumentMatchers.any(Callable.class));

        cronTaskRunner.submit(() -> {
          task.run(123L);
          latch.countDown();
          return null;
        });
        return createDummyFuture();
      }
    }).when(cronScheduler).scheduleAtFixedRate(ArgumentMatchers.anyLong(), ArgumentMatchers.anyLong(),
        ArgumentMatchers.any(), ArgumentMatchers.any(CronTask.class));

    
    final MonitorScheduler scheduler = new ClockDriftSafeMonitorScheduler(
        config,
        Mockito.mock(ServiceEmitter.class),
        ImmutableList.of(monitor),
        cronScheduler,
        executor
    );
    scheduler.start();
    latch.await(5, TimeUnit.SECONDS);

    Mockito.verify(monitor, Mockito.times(1)).start();
    Mockito.verify(cronScheduler, Mockito.times(1)).scheduleAtFixedRate(ArgumentMatchers.anyLong(),
        ArgumentMatchers.anyLong(),
        ArgumentMatchers.any(), ArgumentMatchers.any(CronTask.class));
    Mockito.verify(executor, Mockito.times(1)).submit(ArgumentMatchers.any(Callable.class));
    Mockito.verify(monitor, Mockito.times(1)).monitor(ArgumentMatchers.any());
    Assert.assertTrue(monitorResultHolder.get());
    scheduler.stop();
  }
  
  @Test
  public void testStart_UnexpectedExceptionWhileScheduling() throws InterruptedException
  {
    ExecutorService executor = Mockito.mock(ExecutorService.class);
    Monitor monitor = Mockito.mock(Monitor.class);
    MonitorSchedulerConfig config = Mockito.mock(MonitorSchedulerConfig.class);
    Mockito.when(config.getEmitterPeriod()).thenReturn(new org.joda.time.Duration(1000L));

    CountDownLatch latch = new CountDownLatch(1);
    Mockito.doAnswer(new Answer<Future<?>>()
    {
      @SuppressWarnings("unchecked")
      @Override
      public Future<?> answer(InvocationOnMock invocation)
      {
        final Object originalArgument = (invocation.getArguments())[3];
        CronTask task = ((CronTask) originalArgument);

        Mockito.when(executor.submit(ArgumentMatchers.any(Callable.class)))
               .thenThrow(new RuntimeException("Test throwing exception while scheduling"));
        cronTaskRunner.submit(() -> {
          task.run(123L);
          latch.countDown();
          return null;
        });
        return createDummyFuture();
      }
    }).when(cronScheduler).scheduleAtFixedRate(ArgumentMatchers.anyLong(), ArgumentMatchers.anyLong(),
        ArgumentMatchers.any(), ArgumentMatchers.any(CronTask.class));

    
    final MonitorScheduler scheduler = new ClockDriftSafeMonitorScheduler(
        config,
        Mockito.mock(ServiceEmitter.class),
        ImmutableList.of(monitor),
        cronScheduler,
        executor
    );
    scheduler.start();
    latch.await(5, TimeUnit.SECONDS);

    Mockito.verify(monitor, Mockito.times(1)).start();
    Mockito.verify(cronScheduler, Mockito.times(1)).scheduleAtFixedRate(ArgumentMatchers.anyLong(),
        ArgumentMatchers.anyLong(),
        ArgumentMatchers.any(), ArgumentMatchers.any(CronTask.class));
    Mockito.verify(executor, Mockito.times(1)).submit(ArgumentMatchers.any(Callable.class));
    scheduler.stop();
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
  }
}
