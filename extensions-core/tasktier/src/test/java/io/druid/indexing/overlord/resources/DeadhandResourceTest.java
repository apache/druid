/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Metamarkets licenses this file
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

package io.druid.indexing.overlord.resources;

import com.google.common.base.Throwables;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import io.druid.concurrent.Execs;
import io.druid.segment.CloserRule;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Response;
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class DeadhandResourceTest
{
  @Rule
  public CloserRule closerRule = new CloserRule(true);
  DeadhandResource deadhandResource;

  @Before
  public void setUp()
  {
    deadhandResource = new DeadhandResource();
  }

  @Test(timeout = 10_000L)
  public void testConcurrentDoHeartbeat() throws Exception
  {
    final HttpServletRequest request = EasyMock.createStrictMock(HttpServletRequest.class);
    EasyMock.expect(request.getRemoteAddr()).andReturn("localhost").anyTimes(); // to make sure easymock threadsafety
    EasyMock.replay(request);

    final int numThreads = 10;
    final ListeningExecutorService service = MoreExecutors.listeningDecorator(
        Execs.multiThreaded(
            numThreads,
            "testThread-%s"
        )
    );
    closerRule.closeLater(executorAsCloseable(service));
    final CountDownLatch readyLatch = new CountDownLatch(numThreads);
    final CountDownLatch startLatch = new CountDownLatch(1);
    final CountDownLatch doneLatch = new CountDownLatch(numThreads);
    final Collection<ListenableFuture<?>> futures = new ArrayList<>(numThreads);
    for (int i = 0; i < numThreads; ++i) {
      futures.add(
          service.submit(
              new Runnable()
              {
                @Override
                public void run()
                {
                  readyLatch.countDown();
                  try {
                    startLatch.await();
                    Assert.assertEquals(
                        Response.Status.OK.getStatusCode(),
                        deadhandResource.doHeartbeat(request).getStatus()
                    );
                  }
                  catch (InterruptedException e) {
                    throw Throwables.propagate(e);
                  }
                  finally {
                    doneLatch.countDown();
                  }
                }
              }
          )
      );
    }
    readyLatch.await(1, TimeUnit.SECONDS);
    startLatch.countDown();
    doneLatch.await();
    Assert.assertEquals(numThreads, deadhandResource.getHeartbeatCount());
    EasyMock.verify(request);
    Assert.assertFalse(Futures.allAsList(futures).get().isEmpty());
  }

  @Test
  public void testDoHeartbeat() throws Exception
  {
    HttpServletRequest request = EasyMock.createStrictMock(HttpServletRequest.class);
    EasyMock.expect(request.getRemoteAddr()).andReturn("localhost").once();
    EasyMock.replay(request);
    Assert.assertEquals(Response.Status.OK.getStatusCode(), deadhandResource.doHeartbeat(request).getStatus());
    EasyMock.verify(request);
  }


  @Test(expected = TimeoutException.class)
  public void testWaitTimeout() throws Exception
  {
    deadhandResource.waitForHeartbeat(5);
  }

  @Test
  public void testWaitForHeartbeat() throws Exception
  {
    final ExecutorService service = Execs.singleThreaded("testThread-%s");
    closerRule.closeLater(executorAsCloseable(service));
    final Future<?> future = service.submit(
        new Runnable()
        {
          @Override
          public void run()
          {
            try {
              deadhandResource.waitForHeartbeat(1_000);
            }
            catch (InterruptedException | TimeoutException e) {
              throw Throwables.propagate(e);
            }
          }
        }
    );
    while (!future.isDone()) {
      testDoHeartbeat();
      Thread.sleep(1);
    }
    future.get(2_000, TimeUnit.MILLISECONDS);
  }

  @Test
  public void testConcurrentWaitForHeartbeat() throws Exception
  {
    final int numThreads = 10;
    final ListeningExecutorService service = MoreExecutors.listeningDecorator(
        Execs.multiThreaded(
            numThreads,
            "testThread-%s"
        )
    );
    closerRule.closeLater(executorAsCloseable(service));
    final CountDownLatch readyLatch = new CountDownLatch(numThreads);
    final CountDownLatch startLatch = new CountDownLatch(1);
    final Collection<ListenableFuture<?>> futures = new ArrayList<>(numThreads);
    for (int i = 0; i < numThreads; ++i) {
      futures.add(
          service.submit(
              new Runnable()
              {
                @Override
                public void run()
                {
                  try {
                    readyLatch.countDown();
                    startLatch.await();
                    deadhandResource.waitForHeartbeat(10_000);
                  }
                  catch (InterruptedException | TimeoutException e) {
                    throw Throwables.propagate(e);
                  }
                }
              }
          )
      );
    }
    service.shutdown();
    readyLatch.await(1, TimeUnit.SECONDS);
    startLatch.countDown();
    final ListenableFuture<?> collectiveFuture = Futures.allAsList(futures);
    while (!collectiveFuture.isDone()) {
      testDoHeartbeat();
      Thread.sleep(1);
    }
    collectiveFuture.get(20, TimeUnit.SECONDS);
  }


  @Test(timeout = 10_000L)
  public void testConcurrentDoHeartbeatAndWait() throws Exception
  {
    final HttpServletRequest request = EasyMock.createStrictMock(HttpServletRequest.class);
    EasyMock.expect(request.getRemoteAddr()).andReturn("localhost").anyTimes(); // to make sure easymock threadsafety
    EasyMock.replay(request);

    final int numThreads = 10;
    final ListeningExecutorService beatingService = MoreExecutors.listeningDecorator(
        Execs.multiThreaded(
            numThreads,
            "testBeatingThread-%s"
        )
    );
    final ListeningExecutorService listeningService = MoreExecutors.listeningDecorator(
        Execs.multiThreaded(
            numThreads,
            "testListeningThread-%s"
        )
    );

    closerRule.closeLater(executorAsCloseable(beatingService));
    closerRule.closeLater(executorAsCloseable(listeningService));

    final CountDownLatch readyLatch = new CountDownLatch(numThreads * 2);
    final CountDownLatch startLatch = new CountDownLatch(1);
    final CountDownLatch doneLatch = new CountDownLatch(numThreads * 2);
    final Collection<ListenableFuture<?>> futures = new ArrayList<>(numThreads * 2);

    for (int i = 0; i < numThreads; ++i) {
      futures.add(
          beatingService.submit(
              new Runnable()
              {
                @Override
                public void run()
                {
                  readyLatch.countDown();
                  try {
                    startLatch.await();
                    Assert.assertEquals(
                        Response.Status.OK.getStatusCode(),
                        deadhandResource.doHeartbeat(request).getStatus()
                    );
                  }
                  catch (InterruptedException e) {
                    throw Throwables.propagate(e);
                  }
                  finally {
                    doneLatch.countDown();
                  }
                }
              }
          )
      );
      futures.add(
          listeningService.submit(
              new Runnable()
              {
                @Override
                public void run()
                {
                  readyLatch.countDown();
                  try {
                    startLatch.await();
                    deadhandResource.waitForHeartbeat(5_000);
                  }
                  catch (InterruptedException | TimeoutException e) {
                    throw Throwables.propagate(e);
                  }
                  finally {
                    doneLatch.countDown();
                  }
                }
              }
          )
      );
    }

    readyLatch.await(1, TimeUnit.SECONDS);
    startLatch.countDown();
    doneLatch.await();
    Assert.assertEquals(numThreads, deadhandResource.getHeartbeatCount());
    EasyMock.verify(request);

    Assert.assertFalse(Futures.allAsList(futures).get().isEmpty());
  }

  private static Closeable executorAsCloseable(final ExecutorService service)
  {
    return new Closeable()
    {
      @Override
      public void close() throws IOException
      {
        service.shutdownNow();
      }
    };
  }
}

