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

package org.apache.druid.server;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.inject.Injector;
import org.apache.druid.error.DruidException;
import org.apache.druid.error.ErrorResponse;
import org.apache.druid.guice.PropertiesModule;
import org.apache.druid.guice.StartupInjectorBuilder;
import org.apache.druid.guice.TestDruidModule;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.segment.loading.SegmentLoaderConfig;
import org.apache.druid.utils.JvmUtils;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import javax.ws.rs.core.Response;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

public class StatusResourceTest
{
  @Test
  public void testLoadedModules()
  {

    Collection<DruidModule> modules = ImmutableList.of(new TestDruidModule());
    List<StatusResource.ModuleVersion> statusResourceModuleList =
        new StatusResource.Status(modules, JvmUtils.getRuntimeInfo()).getModules();

    Assert.assertEquals("Status should have all modules loaded!", modules.size(), statusResourceModuleList.size());

    for (DruidModule module : modules) {
      String moduleName = module.getClass().getName();

      boolean contains = Boolean.FALSE;
      for (StatusResource.ModuleVersion version : statusResourceModuleList) {
        if (version.getName().equals(moduleName)) {
          contains = Boolean.TRUE;
          break;
        }
      }
      Assert.assertTrue("Status resource should contain module " + moduleName, contains);
    }
  }

  @Test
  public void testHiddenProperties() throws Exception
  {
    testHiddenPropertiesWithPropertyFileName("status.resource.test.runtime.properties");
  }

  @Test
  public void testHiddenPropertiesContain() throws Exception
  {
    testHiddenPropertiesWithPropertyFileName("status.resource.test.runtime.hpc.properties");
  }

  @Test
  public void testGetReadyReturns200WhenReady()
  {
    final ServiceAnnouncementState state = new ServiceAnnouncementState();
    state.markReady();
    final StatusResource resource = new StatusResource(new Properties(), null, null, null, state);
    final Response response = resource.getReady();
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(true, response.getEntity());
  }

  @Test
  public void testGetReadyReturns503WhenNotReady()
  {
    final ServiceAnnouncementState state = new ServiceAnnouncementState();
    final StatusResource resource = new StatusResource(new Properties(), null, null, null, state);
    final Response response = resource.getReady();
    Assert.assertEquals(503, response.getStatus());
    Assert.assertEquals(false, response.getEntity());
  }

  @Test
  public void testStackTrace()
  {
    final StatusResource resource = new StatusResource(new Properties(), null, null, null, null);
    final Response httpResponse = resource.getStackTrace(null);
    Assert.assertEquals(Response.Status.OK.getStatusCode(), httpResponse.getStatus());
    final StackTraceCollector.ThreadStackTraceResponse response =
        (StackTraceCollector.ThreadStackTraceResponse) httpResponse.getEntity();

    Assert.assertNotNull(response.getCollectedAt());
    Assert.assertFalse(response.getThreads().isEmpty());

    final long currentThreadId = Thread.currentThread().threadId();
    final StackTraceCollector.ThreadStackTrace currentThread = response.getThreads()
        .stream()
        .filter(thread -> thread.getThreadId() == currentThreadId)
        .findFirst()
        .orElse(null);
    Assert.assertNotNull(currentThread);
    Assert.assertEquals(Thread.currentThread().getName(), currentThread.getThreadName());
    Assert.assertEquals(Thread.currentThread().getState().name(), currentThread.getThreadState());
    Assert.assertFalse(currentThread.getStackTrace().isEmpty());
    Assert.assertTrue(currentThread.getStackTrace().contains("\n\tat "));
    Assert.assertTrue(
        currentThread.getStackTrace().lines().filter(line -> line.startsWith("\tat ")).count() > 8
    );
    Assert.assertFalse(currentThread.getStackTrace().contains("\t...\n"));
    if (JvmUtils.isThreadCpuTimeEnabled()) {
      Assert.assertNotNull(currentThread.getCpuTimeNs());
      Assert.assertNotNull(currentThread.getUserCpuTimeNs());
    }
  }

  @Test
  public void testStackTraceWithMaxStackTraceFrameDepth()
  {
    final StatusResource resource = new StatusResource(new Properties(), null, null, null, null);
    final Response httpResponse = resource.getStackTrace("10");
    Assert.assertEquals(Response.Status.OK.getStatusCode(), httpResponse.getStatus());
    final StackTraceCollector.ThreadStackTraceResponse response =
        (StackTraceCollector.ThreadStackTraceResponse) httpResponse.getEntity();

    Assert.assertTrue(
        response.getThreads()
                .stream()
                .allMatch(
                    thread -> thread.getStackTrace()
                                   .lines()
                                   .filter(line -> line.startsWith("\tat "))
                                   .count() <= 10
                )
    );
  }

  @Test
  public void testStackTraceRejectsInvalidMaxStackTraceFrameDepth()
  {
    final StatusResource resource = new StatusResource(new Properties(), null, null, null, null);

    for (final String invalidDepth : ImmutableList.of("-1", "0", "9", "1001", "10.5", "not-an-integer")) {
      final Response response = resource.getStackTrace(invalidDepth);
      Assert.assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), response.getStatus());
      Assert.assertTrue(response.getEntity() instanceof ErrorResponse);
      final DruidException exception = ((ErrorResponse) response.getEntity()).getUnderlyingException();
      Assert.assertEquals(DruidException.Category.INVALID_INPUT, exception.getCategory());
      Assert.assertTrue(exception.getMessage().contains(StackTraceCollector.MAX_STACK_TRACE_FRAME_DEPTH_KEY));
    }
  }

  @Test
  public void testStackTraceFormatsWaitingLockOnStackFrame() throws Exception
  {
    final Object monitor = new Object();
    final CountDownLatch enteredMonitor = new CountDownLatch(1);
    final Thread waitingThread = new Thread(
        () -> {
          synchronized (monitor) {
            enteredMonitor.countDown();
            try {
              monitor.wait();
            }
            catch (InterruptedException e) {
              Thread.currentThread().interrupt();
            }
          }
        },
        "stack-trace-waiting-thread"
    );
    waitingThread.start();

    try {
      Assert.assertTrue(enteredMonitor.await(5, TimeUnit.SECONDS));
      final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
      while (waitingThread.getState() != Thread.State.WAITING && System.nanoTime() < deadline) {
        Thread.yield();
      }

      final StackTraceCollector.ThreadStackTrace thread = new StackTraceCollector().collect()
          .getThreads()
          .stream()
          .filter(stackTrace -> stackTrace.getThreadId() == waitingThread.threadId())
          .findFirst()
          .orElse(null);
      Assert.assertNotNull(thread);
      Assert.assertEquals(Thread.State.WAITING.name(), thread.getThreadState());
      Assert.assertTrue(
          thread.getStackTrace().contains(" - waiting on " + thread.getLockName() + "\n")
      );
      Assert.assertFalse(thread.getStackTrace().contains("\n\t-  waiting on "));
    }
    finally {
      waitingThread.interrupt();
      waitingThread.join(TimeUnit.SECONDS.toMillis(5));
    }
  }

  @Test
  public void testStackTraceFormatsHeldMonitorAndSynchronizer() throws Exception
  {
    final ThreadMXBean threadMxBean = ManagementFactory.getThreadMXBean();
    Assume.assumeTrue(threadMxBean.isObjectMonitorUsageSupported());
    Assume.assumeTrue(threadMxBean.isSynchronizerUsageSupported());

    final Object monitor = new Object();
    final ReentrantLock synchronizer = new ReentrantLock();
    final CountDownLatch locksHeld = new CountDownLatch(1);
    final CountDownLatch releaseLocks = new CountDownLatch(1);
    final Thread lockHolder = new Thread(
        () -> {
          synchronizer.lock();
          try {
            synchronized (monitor) {
              locksHeld.countDown();
              try {
                releaseLocks.await();
              }
              catch (InterruptedException e) {
                Thread.currentThread().interrupt();
              }
            }
          }
          finally {
            synchronizer.unlock();
          }
        },
        "stack-trace-lock-holder"
    );
    lockHolder.start();

    try {
      Assert.assertTrue(locksHeld.await(5, TimeUnit.SECONDS));
      final StackTraceCollector.ThreadStackTrace thread = new StackTraceCollector().collect()
          .getThreads()
          .stream()
          .filter(stackTrace -> stackTrace.getThreadId() == lockHolder.threadId())
          .findFirst()
          .orElse(null);
      Assert.assertNotNull(thread);
      Assert.assertTrue(thread.getStackTrace().contains("\t-  locked " + monitor));
      Assert.assertTrue(thread.getStackTrace().contains("\tNumber of locked synchronizers = 1\n"));
      Assert.assertTrue(thread.getStackTrace().contains("java.util.concurrent.locks.ReentrantLock$"));
    }
    finally {
      releaseLocks.countDown();
      lockHolder.interrupt();
      lockHolder.join(TimeUnit.SECONDS.toMillis(5));
    }
  }

  private void testHiddenPropertiesWithPropertyFileName(String fileName) throws Exception
  {
    Injector injector = new StartupInjectorBuilder()
        .add(
            new PropertiesModule(Collections.singletonList(fileName)),
            binder -> binder.bind(SegmentLoaderConfig.class).toInstance(SegmentLoaderConfig.builder().build())
        )
        .build();
    Map<String, String> returnedProperties = injector.getInstance(StatusResource.class).getProperties();
    Set<String> lowerCasePropertyNames = returnedProperties.keySet()
                                                           .stream()
                                                           .map(StringUtils::toLowerCase)
                                                           .collect(Collectors.toSet());

    Assert.assertTrue(
        "The list of unfiltered Properties is not > the list of filtered Properties?!?",
        injector.getInstance(Properties.class).stringPropertyNames().size() > returnedProperties.size()
    );

    Set<String> hiddenProperties = new ObjectMapper().readValue(
        returnedProperties.get("druid.server.hiddenProperties"),
        new TypeReference<>() {}
    );

    hiddenProperties.forEach(
        (property) -> {
          lowerCasePropertyNames.forEach(
              lowerCasePropertyName -> Assert.assertFalse(lowerCasePropertyName.contains(StringUtils.toLowerCase(
                  property)))
          );
        }
    );
  }

}
