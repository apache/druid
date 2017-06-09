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
import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import io.druid.concurrent.Execs;
import io.druid.java.util.common.lifecycle.Lifecycle;
import io.druid.query.lookup.namespace.CacheGenerator;
import io.druid.query.lookup.namespace.ExtractionNamespace;
import io.druid.query.lookup.namespace.UriExtractionNamespace;
import io.druid.query.lookup.namespace.UriExtractionNamespaceTest;
import io.druid.server.metrics.NoopServiceEmitter;
import org.joda.time.Period;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import javax.annotation.Nullable;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 *
 */
@RunWith(Parameterized.class)
public class CacheSchedulerTest
{
  public static final Function<Lifecycle, NamespaceExtractionCacheManager> CREATE_ON_HEAP_CACHE_MANAGER =
      new Function<Lifecycle, NamespaceExtractionCacheManager>()
      {
        @Nullable
        @Override
        public NamespaceExtractionCacheManager apply(@Nullable Lifecycle lifecycle)
        {
          return new OnHeapNamespaceExtractionCacheManager(lifecycle, new NoopServiceEmitter());
        }
      };
  public static final Function<Lifecycle, NamespaceExtractionCacheManager> CREATE_OFF_HEAP_CACHE_MANAGER =
      new Function<Lifecycle, NamespaceExtractionCacheManager>()
      {
        @Nullable
        @Override
        public NamespaceExtractionCacheManager apply(@Nullable Lifecycle lifecycle)
        {
          return new OffHeapNamespaceExtractionCacheManager(lifecycle, new NoopServiceEmitter());
        }
      };

  @Parameterized.Parameters
  public static Collection<Object[]> data()
  {
    return Arrays.asList(new Object[][]{{CREATE_ON_HEAP_CACHE_MANAGER}});
  }

  public static void waitFor(CacheScheduler.Entry entry) throws InterruptedException
  {
    entry.awaitTotalUpdates(1);
  }


  private static final String KEY = "foo";
  private static final String VALUE = "bar";

  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();
  private final Function<Lifecycle, NamespaceExtractionCacheManager> createCacheManager;
  private Lifecycle lifecycle;
  private NamespaceExtractionCacheManager cacheManager;
  private CacheScheduler scheduler;
  private File tmpFile;

  public CacheSchedulerTest(
      Function<Lifecycle, NamespaceExtractionCacheManager> createCacheManager
  )
  {
    this.createCacheManager = createCacheManager;
  }

  @Before
  public void setUp() throws Exception
  {
    lifecycle = new Lifecycle();
    lifecycle.start();
    cacheManager = createCacheManager.apply(lifecycle);
    final Path tmpDir = temporaryFolder.newFolder().toPath();
    final CacheGenerator<UriExtractionNamespace> cacheGenerator = new
        CacheGenerator<UriExtractionNamespace>()
    {
      @Override
      public CacheScheduler.VersionedCache generateCache(
          final UriExtractionNamespace extractionNamespace,
          final CacheScheduler.EntryImpl<UriExtractionNamespace> id,
          final String lastVersion,
          final CacheScheduler scheduler
      ) throws InterruptedException
      {
        Thread.sleep(2);// To make absolutely sure there is a unique currentTimeMillis
        String version = Long.toString(System.currentTimeMillis());
        CacheScheduler.VersionedCache versionedCache = scheduler.createVersionedCache(id, version);
        // Don't actually read off disk because TravisCI doesn't like that
        versionedCache.getCache().put(KEY, VALUE);
        return versionedCache;
      }
    };
    scheduler = new CacheScheduler(
        new NoopServiceEmitter(),
        ImmutableMap.<Class<? extends ExtractionNamespace>, CacheGenerator<?>>of(
            UriExtractionNamespace.class,
            cacheGenerator
        ),
        cacheManager
    );
    tmpFile = Files.createTempFile(tmpDir, "druidTestURIExtractionNS", ".dat").toFile();
    try (OutputStream ostream = new FileOutputStream(tmpFile)) {
      try (OutputStreamWriter out = new OutputStreamWriter(ostream, StandardCharsets.UTF_8)) {
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

  @Test(timeout = 10_000)
  public void testSimpleSubmission() throws ExecutionException, InterruptedException
  {
    UriExtractionNamespace namespace = new UriExtractionNamespace(
        tmpFile.toURI(),
        null, null,
        new UriExtractionNamespace.ObjectMapperFlatDataParser(
            UriExtractionNamespaceTest.registerTypes(new ObjectMapper())
        ),
        new Period(0),
        null
    );
    CacheScheduler.Entry entry = scheduler.schedule(namespace);
    waitFor(entry);
    Map<String, String> cache = entry.getCache();
    Assert.assertNull(cache.put("key", "val"));
    Assert.assertEquals("val", cache.get("key"));
  }

  @Test(timeout = 10_000)
  public void testPeriodicUpdatesScheduled() throws ExecutionException, InterruptedException
  {
    final int repeatCount = 5;
    final long delay = 5;
    try {
      final UriExtractionNamespace namespace = getUriExtractionNamespace(delay);
      final long start = System.currentTimeMillis();
      try (CacheScheduler.Entry entry = scheduler.schedule(namespace)) {

        Assert.assertFalse(entry.getUpdaterFuture().isDone());
        Assert.assertFalse(entry.getUpdaterFuture().isCancelled());

        entry.awaitTotalUpdates(repeatCount);

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
    }
    finally {
      lifecycle.stop();
      cacheManager.waitForServiceToEnd(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
    }
    checkNoMoreRunning();
  }


  @Test(timeout = 10_000) // This is very fast when run locally. Speed on Travis completely depends on noisy neighbors.
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
                      try {
                        testDelete();
                      }
                      catch (Exception e) {
                        throw Throwables.propagate(e);
                      }
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
      executorService.shutdown();
      executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
    }
    checkNoMoreRunning();
  }

  @Test(timeout = 10_000L)
  public void testSimpleDelete() throws InterruptedException, TimeoutException, ExecutionException
  {
    testDelete();
  }

  public void testDelete()
      throws InterruptedException, TimeoutException, ExecutionException
  {
    final long period = 1_000L;// Give it some time between attempts to update
    final UriExtractionNamespace namespace = getUriExtractionNamespace(period);
    CacheScheduler.Entry entry = scheduler.scheduleAndWait(namespace, 10_000);
    Assert.assertNotNull(entry);
    final Future<?> future = entry.getUpdaterFuture();
    Assert.assertFalse(future.isCancelled());
    Assert.assertFalse(future.isDone());
    entry.awaitTotalUpdates(1);

    Assert.assertEquals(VALUE, entry.getCache().get(KEY));
    entry.close();

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

    Assert.assertTrue(future.isCancelled());
    Assert.assertTrue(future.isDone());
  }

  private UriExtractionNamespace getUriExtractionNamespace(long period)
  {
    return new UriExtractionNamespace(
        tmpFile.toURI(),
        null, null,
        new UriExtractionNamespace.ObjectMapperFlatDataParser(
            UriExtractionNamespaceTest.registerTypes(new ObjectMapper())
        ),
        new Period(period),
        null
    );
  }

  @Test(timeout = 10_000)
  public void testShutdown()
      throws NoSuchFieldException, IllegalAccessException, InterruptedException, ExecutionException
  {
    final long period = 5L;
    try {

      final UriExtractionNamespace namespace = getUriExtractionNamespace(period);

      try (CacheScheduler.Entry entry = scheduler.schedule(namespace)) {
        final Future<?> future = entry.getUpdaterFuture();
        entry.awaitNextUpdates(1);

        Assert.assertFalse(future.isCancelled());
        Assert.assertFalse(future.isDone());

        final long prior = scheduler.updatesStarted();
        entry.awaitNextUpdates(1);
        Assert.assertTrue(scheduler.updatesStarted() > prior);
      }
    }
    finally {
      lifecycle.stop();
    }
    while (!cacheManager.waitForServiceToEnd(1_000, TimeUnit.MILLISECONDS)) {
      // keep waiting
    }

    checkNoMoreRunning();

    Assert.assertTrue(cacheManager.scheduledExecutorService().isShutdown());
    Assert.assertTrue(cacheManager.scheduledExecutorService().isTerminated());
  }

  @Test(timeout = 10_000)
  public void testRunCount() throws InterruptedException, ExecutionException
  {
    final int numWaits = 5;
    try {
      final UriExtractionNamespace namespace = getUriExtractionNamespace((long) 5);
      try (CacheScheduler.Entry entry = scheduler.schedule(namespace)) {
        final Future<?> future = entry.getUpdaterFuture();
        entry.awaitNextUpdates(numWaits);
        Assert.assertFalse(future.isDone());
      }
    }
    finally {
      lifecycle.stop();
    }
    while (!cacheManager.waitForServiceToEnd(1_000, TimeUnit.MILLISECONDS)) {
      // keep waiting
    }
    Assert.assertTrue(scheduler.updatesStarted() >= numWaits);
    checkNoMoreRunning();
  }

  /**
   * Tests that even if entry.close() wasn't called, the scheduled task is cancelled when the entry becomes
   * unreachable.
   */
  @Test(timeout = 60_000)
  public void testEntryCloseForgotten() throws InterruptedException
  {
    scheduleDanglingEntry();
    Assert.assertEquals(1, scheduler.getActiveEntries());
    while (scheduler.getActiveEntries() > 0) {
      System.gc();
      Thread.sleep(1000);
    }
    Assert.assertEquals(0, scheduler.getActiveEntries());
  }

  private void scheduleDanglingEntry() throws InterruptedException
  {
    CacheScheduler.Entry entry = scheduler.schedule(getUriExtractionNamespace(5));
    entry.awaitTotalUpdates(1);
  }

  private void checkNoMoreRunning() throws InterruptedException
  {
    Assert.assertEquals(0, scheduler.getActiveEntries());
    final long pre = scheduler.updatesStarted();
    Thread.sleep(100L);
    Assert.assertEquals(pre, scheduler.updatesStarted());
  }
}
