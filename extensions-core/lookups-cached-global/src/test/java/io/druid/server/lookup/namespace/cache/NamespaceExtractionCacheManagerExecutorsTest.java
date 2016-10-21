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

package io.druid.server.lookup.namespace.cache;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

import io.druid.concurrent.Execs;
import io.druid.data.SearchableVersionedDataFinder;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.lifecycle.Lifecycle;
import io.druid.query.lookup.namespace.ExtractionNamespace;
import io.druid.query.lookup.namespace.ExtractionNamespaceCacheFactory;
import io.druid.query.lookup.namespace.URIExtractionNamespace;
import io.druid.query.lookup.namespace.URIExtractionNamespaceTest;
import io.druid.segment.loading.LocalFileTimestampVersionFinder;
import io.druid.server.lookup.namespace.URIExtractionNamespaceCacheFactory;
import io.druid.server.metrics.NoopServiceEmitter;
import org.joda.time.Period;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

/**
 *
 */
public class NamespaceExtractionCacheManagerExecutorsTest
{
  private static final String KEY = "foo";
  private static final String VALUE = "bar";
  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();
  private Lifecycle lifecycle;
  private NamespaceExtractionCacheManager manager;
  private File tmpFile;
  private final ConcurrentMap<String, Object> cacheUpdateAlerts = new ConcurrentHashMap<>();

  private final AtomicLong numRuns = new AtomicLong(0L);

  @Before
  public void setUp() throws Exception
  {
    final Path tmpDir = temporaryFolder.newFolder().toPath();
    lifecycle = new Lifecycle();
    // Lifecycle stop is used to shut down executors. Start does nothing, so it's ok to call it here.
    lifecycle.start();
    final URIExtractionNamespaceCacheFactory factory = new URIExtractionNamespaceCacheFactory(
        ImmutableMap.<String, SearchableVersionedDataFinder>of("file", new LocalFileTimestampVersionFinder())
    )
    {
      @Override
      public Callable<String> getCachePopulator(
          final String id,
          final URIExtractionNamespace extractionNamespace,
          final String lastVersion,
          final Map<String, String> cache
      )
      {
        return new Callable<String>()
        {
          @Override
          public String call() throws Exception
          {
            // Don't actually read off disk because TravisCI doesn't like that
            cache.put(KEY, VALUE);
            Thread.sleep(2);// To make absolutely sure there is a unique currentTimeMillis
            return Long.toString(System.currentTimeMillis());
          }
        };
      }
    };
    manager = new OnHeapNamespaceExtractionCacheManager(
        lifecycle, new NoopServiceEmitter(),
        ImmutableMap.<Class<? extends ExtractionNamespace>, ExtractionNamespaceCacheFactory<?>>of(
            URIExtractionNamespace.class,
            factory
        )
    )
    {
      @Override
      protected <T extends ExtractionNamespace> Runnable getPostRunnable(
          final String id,
          final T namespace,
          final ExtractionNamespaceCacheFactory<T> factory,
          final String cacheId
      )
      {
        final Runnable runnable = super.getPostRunnable(id, namespace, factory, cacheId);
        cacheUpdateAlerts.putIfAbsent(id, new Object());
        final Object cacheUpdateAlerter = cacheUpdateAlerts.get(id);
        return new Runnable()
        {
          @Override
          public void run()
          {
            synchronized (cacheUpdateAlerter) {
              try {
                runnable.run();
                numRuns.incrementAndGet();
              }
              finally {
                cacheUpdateAlerter.notifyAll();
              }
            }
          }
        };
      }
    };
    tmpFile = Files.createTempFile(tmpDir, "druidTestURIExtractionNS", ".dat").toFile();
    try (OutputStream ostream = new FileOutputStream(tmpFile)) {
      try (OutputStreamWriter out = new OutputStreamWriter(ostream)) {
        // Since Travis sucks with disk related stuff, we override the disk reading part above.
        // This is safe and should shake out any problem areas that accidentally read the file.
        out.write("SHOULDN'T TRY TO PARSE");
        out.flush();
      }
    }
  }

  @After
  public void tearDown()
  {
    lifecycle.stop();
  }

  @Test(expected = IAE.class)
  public void testDoubleSubmission()
  {
    final String namespaceID = "ns";
    URIExtractionNamespace namespace = new URIExtractionNamespace(
        tmpFile.toURI(),
        null, null,
        new URIExtractionNamespace.ObjectMapperFlatDataParser(
            URIExtractionNamespaceTest.registerTypes(new ObjectMapper())
        ),
        new Period(0),
        null
    );
    final ListenableFuture<?> future = manager.schedule(namespaceID, namespace);
    Assert.assertFalse(future.isDone());
    Assert.assertFalse(future.isCancelled());
    try {
      manager.schedule(namespaceID, namespace).cancel(true);
    }
    finally {
      future.cancel(true);
    }
  }


  @Test(timeout = 60_000)
  public void testSimpleSubmission() throws ExecutionException, InterruptedException
  {
    final String namespaceID = "ns";
    URIExtractionNamespace namespace = new URIExtractionNamespace(
        tmpFile.toURI(),
        null, null,
        new URIExtractionNamespace.ObjectMapperFlatDataParser(
            URIExtractionNamespaceTest.registerTypes(new ObjectMapper())
        ),
        new Period(0),
        null
    );
    NamespaceExtractionCacheManagersTest.waitFor(manager.schedule(namespaceID, namespace));
  }

  @Test(timeout = 60_000)
  public void testRepeatSubmission() throws ExecutionException, InterruptedException
  {
    final int repeatCount = 5;
    final long delay = 5;
    final long totalRunCount;
    final long start;
    final String namespaceID = "ns";
    try {
      final URIExtractionNamespace namespace = new URIExtractionNamespace(
          tmpFile.toURI(),
          null, null,
          new URIExtractionNamespace.ObjectMapperFlatDataParser(
              URIExtractionNamespaceTest.registerTypes(new ObjectMapper())
          ),
          new Period(delay),
          null
      );
      cacheUpdateAlerts.putIfAbsent(namespaceID, new Object());
      start = System.currentTimeMillis();
      ListenableFuture<?> future = manager.schedule(namespaceID, namespace);

      Assert.assertFalse(future.isDone());
      Assert.assertFalse(future.isCancelled());

      final long preRunCount;
      final Object cacheUpdateAlerter = cacheUpdateAlerts.get(namespaceID);
      synchronized (cacheUpdateAlerter) {
        preRunCount = numRuns.get();
      }
      for (; ; ) {
        synchronized (cacheUpdateAlerter) {
          if (numRuns.get() - preRunCount >= repeatCount) {
            break;
          } else {
            cacheUpdateAlerter.wait();
          }
        }
      }

      long minEnd = start + ((repeatCount - 1) * delay);
      long end = System.currentTimeMillis();
      Assert.assertTrue(
          String.format(
              "Didn't wait long enough between runs. Expected more than %d was %d",
              minEnd - start,
              end - start
          ), minEnd <= end
      );
    }
    finally {
      lifecycle.stop();
    }

    totalRunCount = numRuns.get();
    Thread.sleep(delay * 10);
    Assert.assertEquals(totalRunCount, numRuns.get(), 1);
  }


  @Test(timeout = 600_000) // This is very fast when run locally. Speed on Travis completely depends on noisy neighbors.
  public void testConcurrentAddDelete() throws ExecutionException, InterruptedException, TimeoutException
  {
    final int threads = 10;
    final int deletesPerThread = 5;
    ListeningExecutorService executorService = MoreExecutors.listeningDecorator(
        Execs.multiThreaded(
            threads,
            "concurrentTestingPool-%s"
        )
    );
    final CountDownLatch latch = new CountDownLatch(threads);
    Collection<ListenableFuture<?>> futures = new ArrayList<>();
    for (int i = 0; i < threads; ++i) {
      final int ii = i;
      futures.add(
          executorService.submit(
              new Runnable()
              {
                @Override
                public void run()
                {
                  try {
                    latch.countDown();
                    if (!latch.await(5, TimeUnit.SECONDS)) {
                      throw new RuntimeException(new TimeoutException("Took too long to wait for more tasks"));
                    }
                    for (int j = 0; j < deletesPerThread; ++j) {
                      testDelete(String.format("ns-%d-%d", ii, j));
                    }
                  }
                  catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw Throwables.propagate(e);
                  }
                }
              }
          )
      );
    }
    // Create an all-encompassing exception if any of them failed
    final Collection<Exception> exceptions = new ArrayList<>();
    try {
      for (ListenableFuture<?> future : futures) {
        try {
          future.get();
        }
        catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw e;
        }
        catch (Exception e) {
          exceptions.add(e);
        }
      }
      if (!exceptions.isEmpty()) {
        final RuntimeException e = new RuntimeException("Futures failed");
        for (Exception ex : exceptions) {
          e.addSuppressed(ex);
        }
      }
    }
    finally {
      executorService.shutdownNow();
    }
    checkNoMoreRunning();
  }

  @Test(timeout = 60_000L)
  public void testSimpleDelete() throws InterruptedException
  {
    testDelete("someNamespace");
  }

  public void testDelete(final String ns)
      throws InterruptedException
  {
    cacheUpdateAlerts.putIfAbsent(ns, new Object());
    final Object cacheUpdateAlerter = cacheUpdateAlerts.get(ns);

    final long period = 1_000L;// Give it some time between attempts to update
    final URIExtractionNamespace namespace = new URIExtractionNamespace(
        tmpFile.toURI(),
        null, null,
        new URIExtractionNamespace.ObjectMapperFlatDataParser(
            URIExtractionNamespaceTest.registerTypes(new ObjectMapper())
        ),
        new Period(period),
        null
    );
    Assert.assertTrue(manager.scheduleAndWait(ns, namespace, 10_000));
    final ListenableFuture<?> future = manager.implData.get(ns).future;
    Assert.assertFalse(future.isCancelled());
    Assert.assertFalse(future.isDone());

    long start = 0L;

    final long timeout = 45_000L;
    do {
      synchronized (cacheUpdateAlerter) {
        if (!manager.implData.containsKey(ns)) {
          cacheUpdateAlerter.wait(10_000);
        }
      }
      if (future.isDone()) {
        try {
          // Bubble up the exception
          Assert.assertNull(future.get());
          Assert.fail("Task finished");
        }
        catch (ExecutionException e) {
          throw Throwables.propagate(e);
        }
      }
      if (!manager.implData.containsKey(ns) && System.currentTimeMillis() - start > timeout) {
        throw new RuntimeException(
            new TimeoutException(
                String.format(
                    "Namespace took too long to appear in cache for %s",
                    namespace
                )
            )
        );
      }
    } while (!manager.implData.containsKey(ns) || !manager.implData.get(ns).enabled.get());

    Assert.assertEquals(VALUE, manager.getCacheMap(ns).get(KEY));

    Assert.assertTrue(manager.implData.containsKey(ns));

    Assert.assertTrue(manager.delete(ns));

    try {
      Assert.assertNull(future.get());
    }
    catch (CancellationException e) {
      // Ignore
    }
    catch (ExecutionException e) {
      if (!future.isCancelled()) {
        throw Throwables.propagate(e);
      }
    }

    Assert.assertFalse(manager.implData.containsKey(ns));
    Assert.assertTrue(future.isCancelled());
    Assert.assertTrue(future.isDone());
  }

  @Test(timeout = 60_000)
  public void testShutdown()
      throws NoSuchFieldException, IllegalAccessException, InterruptedException, ExecutionException
  {
    final long period = 5L;
    final ListenableFuture future;
    long prior = 0;
    final String namespaceID = "ns";
    try {

      final URIExtractionNamespace namespace = new URIExtractionNamespace(
          tmpFile.toURI(),
          null, null,
          new URIExtractionNamespace.ObjectMapperFlatDataParser(
              URIExtractionNamespaceTest.registerTypes(new ObjectMapper())
          ),
          new Period(period),
          null
      );
      cacheUpdateAlerts.putIfAbsent(namespaceID, new Object());

      future = manager.schedule(namespaceID, namespace);

      final Object cacheUpdateAlerter = cacheUpdateAlerts.get(namespaceID);
      synchronized (cacheUpdateAlerter) {
        cacheUpdateAlerter.wait();
      }

      Assert.assertFalse(future.isCancelled());
      Assert.assertFalse(future.isDone());

      synchronized (cacheUpdateAlerter) {
        prior = numRuns.get();
        cacheUpdateAlerter.wait();
      }
      Assert.assertTrue(numRuns.get() > prior);
    }
    finally {
      lifecycle.stop();
    }
    while (!manager.waitForServiceToEnd(1_000, TimeUnit.MILLISECONDS)) {
    }

    checkNoMoreRunning();

    Field execField = NamespaceExtractionCacheManager.class.getDeclaredField("listeningScheduledExecutorService");
    execField.setAccessible(true);
    Assert.assertTrue(((ListeningScheduledExecutorService) execField.get(manager)).isShutdown());
    Assert.assertTrue(((ListeningScheduledExecutorService) execField.get(manager)).isTerminated());
  }

  @Test(timeout = 60_000)
  public void testRunCount()
      throws InterruptedException, ExecutionException
  {
    final long numWaits = 5;
    final ListenableFuture<?> future;
    final String namespaceID = "ns";
    try {
      final URIExtractionNamespace namespace = new URIExtractionNamespace(
          tmpFile.toURI(),
          null, null,
          new URIExtractionNamespace.ObjectMapperFlatDataParser(
              URIExtractionNamespaceTest.registerTypes(new ObjectMapper())
          ),
          new Period(5L),
          null
      );

      cacheUpdateAlerts.putIfAbsent(namespaceID, new Object());
      future = manager.schedule(namespaceID, namespace);
      Assert.assertFalse(future.isDone());

      final Object cacheUpdateAlerter = cacheUpdateAlerts.get(namespaceID);
      for (int i = 0; i < numWaits; ++i) {
        synchronized (cacheUpdateAlerter) {
          cacheUpdateAlerter.wait();
        }
      }
      Assert.assertFalse(future.isDone());
    }
    finally {
      lifecycle.stop();
    }
    while (!manager.waitForServiceToEnd(1_000, TimeUnit.MILLISECONDS)) {
    }
    Assert.assertTrue(numRuns.get() >= numWaits);
    checkNoMoreRunning();
  }

  private void checkNoMoreRunning() throws InterruptedException
  {
    final long pre = numRuns.get();
    Thread.sleep(100L);
    Assert.assertEquals(pre, numRuns.get(), 1); // since we don't synchronize here we might have an extra increment
  }
}
