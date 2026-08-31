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

package org.apache.druid.server.lookup.namespace.cache;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.lifecycle.Lifecycle;
import org.apache.druid.metadata.MetadataStorageConnectorConfig;
import org.apache.druid.query.lookup.namespace.CacheGenerator;
import org.apache.druid.query.lookup.namespace.JdbcExtractionNamespace;
import org.apache.druid.query.lookup.namespace.UriExtractionNamespace;
import org.apache.druid.query.lookup.namespace.UriExtractionNamespaceTest;
import org.apache.druid.server.initialization.JdbcAccessSecurityConfig;
import org.apache.druid.server.lookup.namespace.JdbcCacheGenerator;
import org.apache.druid.server.lookup.namespace.NamespaceExtractionConfig;
import org.apache.druid.server.metrics.NoopServiceEmitter;
import org.apache.druid.utils.JvmUtils;
import org.joda.time.Period;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nullable;
import java.io.Closeable;
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
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 *
 */
public class CacheSchedulerTest
{
  public static final Function<Lifecycle, NamespaceExtractionCacheManager> CREATE_ON_HEAP_CACHE_MANAGER =
      new Function<>()
      {
        @Nullable
        @Override
        public NamespaceExtractionCacheManager apply(@Nullable Lifecycle lifecycle)
        {
          return new OnHeapNamespaceExtractionCacheManager(
              lifecycle,
              new NoopServiceEmitter(),
              new NamespaceExtractionConfig()
          );
        }
      };
  public static final Function<Lifecycle, NamespaceExtractionCacheManager> CREATE_OFF_HEAP_CACHE_MANAGER =
      new Function<>()
      {
        @Nullable
        @Override
        public NamespaceExtractionCacheManager apply(@Nullable Lifecycle lifecycle)
        {
          return new OffHeapNamespaceExtractionCacheManager(
              lifecycle,
              new NoopServiceEmitter(),
              new NamespaceExtractionConfig()
          );
        }
      };

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

  @TempDir
  public File temporaryFolder;
  private Function<Lifecycle, NamespaceExtractionCacheManager> createCacheManager;
  private Lifecycle lifecycle;
  private NamespaceExtractionCacheManager cacheManager;
  private CacheScheduler scheduler;
  private File tmpFile;

  public void initCacheSchedulerTest(
      Function<Lifecycle, NamespaceExtractionCacheManager> createCacheManager
  ) throws Exception
  {
    this.createCacheManager = createCacheManager;
    initializeCacheScheduler();
  }

  private void initializeCacheScheduler() throws Exception
  {
    cacheManager = createCacheManager.apply(lifecycle);
    final Path tmpDir = newFolder(temporaryFolder, "junit").toPath();
    final CacheGenerator<UriExtractionNamespace> cacheGenerator = (extractionNamespace, id, lastVersion, cache) -> {
      Thread.sleep(2); // To make absolutely sure there is a unique currentTimeMillis
      String version = Long.toString(System.currentTimeMillis());
      // Don't actually read off disk because TravisCI doesn't like that
      cache.getCache().put(KEY, VALUE);
      return version;
    };
    scheduler = new CacheScheduler(
        new NoopServiceEmitter(),
        ImmutableMap.of(
            UriExtractionNamespace.class,
            cacheGenerator,
            JdbcExtractionNamespace.class,
            new JdbcCacheGenerator(JvmUtils.getRuntimeInfo())
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

  @BeforeEach
  public void setUp() throws Exception
  {
    lifecycle = new Lifecycle();
    lifecycle.start();
  }

  @AfterEach
  public void tearDown()
  {
    lifecycle.stop();
  }

  @MethodSource("data")
  @ParameterizedTest
  @Timeout(value = 60_000L, unit = TimeUnit.MILLISECONDS)
  public void testSimpleSubmission(Function<Lifecycle, NamespaceExtractionCacheManager> createCacheManager) throws Exception
  {
    initCacheSchedulerTest(createCacheManager);
    UriExtractionNamespace namespace = new UriExtractionNamespace(
        tmpFile.toURI(),
        null, null,
        new UriExtractionNamespace.ObjectMapperFlatDataParser(
            UriExtractionNamespaceTest.registerTypes(new ObjectMapper())
        ),
        new Period(0),
        null,
        null
    );
    CacheScheduler.Entry entry = scheduler.schedule(namespace);
    waitFor(entry);
    Assertions.assertEquals(VALUE, entry.getCache().get(KEY));
  }

  @MethodSource("data")
  @ParameterizedTest
  @Timeout(value = 60_000L, unit = TimeUnit.MILLISECONDS)
  public void testInitialization(Function<Lifecycle, NamespaceExtractionCacheManager> createCacheManager) throws Exception
  {
    initCacheSchedulerTest(createCacheManager);
    UriExtractionNamespace namespace = new UriExtractionNamespace(
        tmpFile.toURI(),
        null, null,
        new UriExtractionNamespace.ObjectMapperFlatDataParser(
            UriExtractionNamespaceTest.registerTypes(new ObjectMapper())
        ),
        new Period(0),
        null,
        null
    );
    CacheScheduler.Entry entry = scheduler.schedule(namespace);
    entry.awaitTotalUpdatesWithTimeout(1, 2000);
    Assertions.assertEquals(VALUE, entry.getCache().get(KEY));
  }

  @MethodSource("data")
  @ParameterizedTest
  @Timeout(value = 60_000L, unit = TimeUnit.MILLISECONDS)
  public void testPeriodicUpdatesScheduled(Function<Lifecycle, NamespaceExtractionCacheManager> createCacheManager) throws Exception
  {
    initCacheSchedulerTest(createCacheManager);
    final int repeatCount = 5;
    final long delay = 5;
    try {
      final UriExtractionNamespace namespace = getUriExtractionNamespace(delay);
      final long start = System.currentTimeMillis();
      try (CacheScheduler.Entry entry = scheduler.schedule(namespace)) {

        Assertions.assertFalse(entry.getUpdaterFuture().isDone());
        Assertions.assertFalse(entry.getUpdaterFuture().isCancelled());

        entry.awaitTotalUpdates(repeatCount);

        long minEnd = start + ((repeatCount - 1) * delay);
        long end = System.currentTimeMillis();
        Assertions.assertTrue(
            minEnd <= end, StringUtils.format(
                "Didn't wait long enough between runs. Expected more than %d was %d",
                minEnd - start,
                end - start
            )
        );
      }
    }
    finally {
      lifecycle.stop();
      cacheManager.waitForServiceToEnd(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
    }
    checkNoMoreRunning();
  }


  @MethodSource("data")
  @ParameterizedTest
  @Timeout(value = 60_000L, unit = TimeUnit.MILLISECONDS) // This is very fast when run locally. Speed on Travis completely depends on noisy neighbors.
  public void testConcurrentAddDelete(Function<Lifecycle, NamespaceExtractionCacheManager> createCacheManager) throws Exception
  {
    initCacheSchedulerTest(createCacheManager);
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
                        throw new RuntimeException(e);
                      }
                    }
                  }
                  catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
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

  @MethodSource("data")
  @ParameterizedTest
  @Timeout(value = 60_000L, unit = TimeUnit.MILLISECONDS)
  public void testSimpleDelete(Function<Lifecycle, NamespaceExtractionCacheManager> createCacheManager) throws Exception
  {
    initCacheSchedulerTest(createCacheManager);
    testDelete();
  }

  private void testDelete() throws InterruptedException
  {
    final long period = 1_000L; // Give it some time between attempts to update
    final UriExtractionNamespace namespace = getUriExtractionNamespace(period);
    CacheScheduler.Entry entry = scheduler.scheduleAndWait(namespace, 10_000);
    Assertions.assertNotNull(entry);
    final Future<?> future = entry.getUpdaterFuture();
    Assertions.assertFalse(future.isCancelled());
    Assertions.assertFalse(future.isDone());
    entry.awaitTotalUpdates(1);

    Assertions.assertEquals(VALUE, entry.getCache().get(KEY));
    entry.close();

    try {
      Assertions.assertNull(future.get());
    }
    catch (CancellationException e) {
      // Ignore
    }
    catch (ExecutionException e) {
      if (!future.isCancelled()) {
        throw new RuntimeException(e);
      }
    }

    Assertions.assertTrue(future.isCancelled());
    Assertions.assertTrue(future.isDone());
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
        null,
        null
    );
  }

  @MethodSource("data")
  @ParameterizedTest
  @Timeout(value = 60_000L, unit = TimeUnit.MILLISECONDS)
  public void testRetainedRetiredCacheSkipsPollUntilReferenceCloses(
      Function<Lifecycle, NamespaceExtractionCacheManager> createCacheManager
  ) throws Exception
  {
    initCacheSchedulerTest(createCacheManager);
    final NamespaceExtractionConfig config = new NamespaceExtractionConfig();
    config.setMaxRetiredCacheEntries(1);
    config.setRetiredCacheEntryTimeoutMillis(60_000L);
    scheduler = newCacheSchedulerWithVersionedCacheGenerator(config, cacheManager);

    try (CacheScheduler.Entry entry = scheduler.schedule(getUriExtractionNamespace(5))) {
      entry.awaitTotalUpdatesWithTimeout(1, 10_000L);
      final CacheScheduler.VersionedCache firstVersion =
          (CacheScheduler.VersionedCache) entry.getCacheState();
      final Closeable retainedFirstVersion = firstVersion.acquireReference();

      entry.awaitTotalUpdatesWithTimeout(2, 10_000L);
      final long updatesStartedAfterSecondVersion = scheduler.updatesStarted();

      try {
        entry.awaitTotalUpdatesWithTimeout(3, 100L);
        Assertions.fail("Expected the third cache load to be skipped while the retired cache is retained");
      }
      catch (TimeoutException expected) {
        // expected
      }
      Assertions.assertEquals(updatesStartedAfterSecondVersion, scheduler.updatesStarted());

      retainedFirstVersion.close();
      entry.awaitTotalUpdatesWithTimeout(3, 10_000L);
    }
  }

  @MethodSource("data")
  @ParameterizedTest
  @Timeout(value = 60_000L, unit = TimeUnit.MILLISECONDS)
  public void testRetiredCacheRejectsNewReference(
      Function<Lifecycle, NamespaceExtractionCacheManager> createCacheManager
  ) throws Exception
  {
    initCacheSchedulerTest(createCacheManager);
    final NamespaceExtractionConfig config = new NamespaceExtractionConfig();
    config.setMaxRetiredCacheEntries(1);
    config.setRetiredCacheEntryTimeoutMillis(60_000L);
    scheduler = newCacheSchedulerWithVersionedCacheGenerator(config, cacheManager);

    try (CacheScheduler.Entry entry = scheduler.schedule(getUriExtractionNamespace(5))) {
      entry.awaitTotalUpdatesWithTimeout(1, 10_000L);
      final CacheScheduler.VersionedCache firstVersion =
          (CacheScheduler.VersionedCache) entry.getCacheState();
      final Closeable retainedFirstVersion = firstVersion.acquireReference();
      try {
        entry.awaitTotalUpdatesWithTimeout(2, 10_000L);
        waitForCondition(() -> entry.pruneAndCountRetiredCaches() == 1, 10_000L);
        final Closeable staleRetainedFirstVersion = firstVersion.acquireReference();
        staleRetainedFirstVersion.close();
        Assertions.fail("Expected retired cache to reject a new reference");
      }
      catch (ISE expected) {
        // expected
      }
      finally {
        retainedFirstVersion.close();
      }
    }
  }

  @MethodSource("data")
  @ParameterizedTest
  @Timeout(value = 60_000L, unit = TimeUnit.MILLISECONDS)
  public void testRetiredCacheTimeoutForceDisposesActiveReference(
      Function<Lifecycle, NamespaceExtractionCacheManager> createCacheManager
  ) throws Exception
  {
    initCacheSchedulerTest(createCacheManager);
    final NamespaceExtractionConfig config = new NamespaceExtractionConfig();
    config.setMaxRetiredCacheEntries(1);
    config.setRetiredCacheEntryTimeoutMillis(0L);
    scheduler = newCacheSchedulerWithVersionedCacheGenerator(config, cacheManager);

    try (CacheScheduler.Entry entry = scheduler.schedule(getUriExtractionNamespace(5))) {
      entry.awaitTotalUpdatesWithTimeout(1, 10_000L);
      final CacheScheduler.VersionedCache firstVersion =
          (CacheScheduler.VersionedCache) entry.getCacheState();
      final Closeable retainedFirstVersion = firstVersion.acquireReference();

      entry.awaitTotalUpdatesWithTimeout(3, 10_000L);
      retainedFirstVersion.close();
    }
  }

  @MethodSource("data")
  @ParameterizedTest
  @Timeout(value = 60_000L, unit = TimeUnit.MILLISECONDS)
  public void testMaxRetiredCacheEntriesAllowsConfiguredNumberBeforeSkipping(
      Function<Lifecycle, NamespaceExtractionCacheManager> createCacheManager
  ) throws Exception
  {
    initCacheSchedulerTest(createCacheManager);
    final NamespaceExtractionConfig config = new NamespaceExtractionConfig();
    config.setMaxRetiredCacheEntries(2);
    config.setRetiredCacheEntryTimeoutMillis(60_000L);
    scheduler = newCacheSchedulerWithVersionedCacheGenerator(config, cacheManager);

    try (CacheScheduler.Entry entry = scheduler.schedule(getUriExtractionNamespace(5))) {
      entry.awaitTotalUpdatesWithTimeout(1, 10_000L);
      final Closeable retainedFirstVersion =
          ((CacheScheduler.VersionedCache) entry.getCacheState()).acquireReference();

      entry.awaitTotalUpdatesWithTimeout(2, 10_000L);
      final Closeable retainedSecondVersion =
          ((CacheScheduler.VersionedCache) entry.getCacheState()).acquireReference();

      entry.awaitTotalUpdatesWithTimeout(3, 10_000L);
      final long updatesStartedAfterThirdVersion = scheduler.updatesStarted();

      try {
        entry.awaitTotalUpdatesWithTimeout(4, 100L);
        Assertions.fail("Expected the fourth cache load to be skipped while two retired caches are retained");
      }
      catch (TimeoutException expected) {
        // expected
      }
      Assertions.assertEquals(updatesStartedAfterThirdVersion, scheduler.updatesStarted());

      retainedFirstVersion.close();
      entry.awaitTotalUpdatesWithTimeout(4, 10_000L);
      retainedSecondVersion.close();
    }
  }

  @Test
  @Timeout(value = 60_000L, unit = TimeUnit.MILLISECONDS)
  public void testFailedRetiredCacheDisposeIsRetriedOnNextPrune() throws Exception
  {
    final NamespaceExtractionConfig config = new NamespaceExtractionConfig();
    config.setMaxRetiredCacheEntries(1);
    config.setRetiredCacheEntryTimeoutMillis(0L);
    final FailingDisposeCacheManager failingCacheManager = new FailingDisposeCacheManager(
        lifecycle,
        new NoopServiceEmitter(),
        config,
        2
    );
    scheduler = newCacheSchedulerWithVersionedCacheGenerator(config, failingCacheManager);

    try (CacheScheduler.Entry entry = scheduler.schedule(getUriExtractionNamespace(5))) {
      entry.awaitTotalUpdatesWithTimeout(1, 10_000L);
      entry.awaitTotalUpdatesWithTimeout(2, 10_000L);
      waitForCondition(() -> failingCacheManager.getDisposeAttempts() >= 2, 10_000L);

      entry.awaitTotalUpdatesWithTimeout(3, 10_000L);
      Assertions.assertTrue(failingCacheManager.getDisposeAttempts() >= 3);
    }
  }

  @Test
  @Timeout(value = 60_000L, unit = TimeUnit.MILLISECONDS)
  public void testEntryCloseRetiresActiveCacheUntilReferenceCloses() throws Exception
  {
    final NamespaceExtractionConfig config = new NamespaceExtractionConfig();
    final CountingDisposeCacheManager countingCacheManager = new CountingDisposeCacheManager(
        lifecycle,
        new NoopServiceEmitter(),
        config
    );
    scheduler = newCacheSchedulerWithVersionedCacheGenerator(config, countingCacheManager);

    final CacheScheduler.Entry entry = scheduler.schedule(getUriExtractionNamespace(60_000L));
    Closeable retainedLookup = null;
    boolean entryClosed = false;
    try {
      entry.awaitTotalUpdatesWithTimeout(1, 10_000L);
      retainedLookup = ((CacheScheduler.VersionedCache) entry.getCacheState()).acquireReference();

      entry.close();
      entryClosed = true;
      Assertions.assertEquals(0, entry.pruneAndCountRetiredCaches());
      Assertions.assertEquals(0, countingCacheManager.getDisposeAttempts());

      retainedLookup.close();
      Assertions.assertEquals(1, countingCacheManager.getDisposeAttempts());
    }
    finally {
      if (retainedLookup != null) {
        retainedLookup.close();
      }
      if (!entryClosed) {
        entry.close();
      }
    }
  }

  private CacheScheduler newCacheSchedulerWithVersionedCacheGenerator(
      NamespaceExtractionConfig config,
      NamespaceExtractionCacheManager namespaceCacheManager
  )
  {
    final AtomicInteger version = new AtomicInteger();
    final CacheGenerator<UriExtractionNamespace> cacheGenerator = (extractionNamespace, id, lastVersion, cache) -> {
      final int nextVersion = version.incrementAndGet();
      cache.getCache().put(KEY, VALUE + nextVersion);
      return Integer.toString(nextVersion);
    };
    return new CacheScheduler(
        new NoopServiceEmitter(),
        ImmutableMap.of(UriExtractionNamespace.class, cacheGenerator),
        namespaceCacheManager,
        config
    );
  }

  @MethodSource("data")
  @ParameterizedTest
  @Timeout(value = 60_000L, unit = TimeUnit.MILLISECONDS)
  public void testShutdown(Function<Lifecycle, NamespaceExtractionCacheManager> createCacheManager)
      throws Exception
  {
    initCacheSchedulerTest(createCacheManager);
    final long period = 5L;
    try {

      final UriExtractionNamespace namespace = getUriExtractionNamespace(period);

      try (CacheScheduler.Entry entry = scheduler.schedule(namespace)) {
        final Future<?> future = entry.getUpdaterFuture();
        entry.awaitNextUpdates(1);

        Assertions.assertFalse(future.isCancelled());
        Assertions.assertFalse(future.isDone());

        final long prior = scheduler.updatesStarted();
        entry.awaitNextUpdates(1);
        Assertions.assertTrue(scheduler.updatesStarted() > prior);
      }
    }
    finally {
      lifecycle.stop();
    }
    while (!cacheManager.waitForServiceToEnd(1_000, TimeUnit.MILLISECONDS)) {
      // keep waiting
    }

    checkNoMoreRunning();

    Assertions.assertTrue(cacheManager.scheduledExecutorService().isShutdown());
    Assertions.assertTrue(cacheManager.scheduledExecutorService().isTerminated());
  }

  @MethodSource("data")
  @ParameterizedTest
  @Timeout(value = 60_000L, unit = TimeUnit.MILLISECONDS)
  public void testRunCount(Function<Lifecycle, NamespaceExtractionCacheManager> createCacheManager) throws Exception
  {
    initCacheSchedulerTest(createCacheManager);
    final int numWaits = 5;
    try {
      final UriExtractionNamespace namespace = getUriExtractionNamespace((long) 5);
      try (CacheScheduler.Entry entry = scheduler.schedule(namespace)) {
        final Future<?> future = entry.getUpdaterFuture();
        entry.awaitNextUpdates(numWaits);
        Assertions.assertFalse(future.isDone());
      }
    }
    finally {
      lifecycle.stop();
    }
    while (!cacheManager.waitForServiceToEnd(1_000, TimeUnit.MILLISECONDS)) {
      // keep waiting
    }
    Assertions.assertTrue(scheduler.updatesStarted() >= numWaits);
    checkNoMoreRunning();
  }

  /**
   * Tests that even if entry.close() wasn't called, the scheduled task is cancelled when the entry becomes
   * unreachable.
   */
  @MethodSource("data")
  @ParameterizedTest
  @Timeout(value = 60_000L, unit = TimeUnit.MILLISECONDS)
  public void testEntryCloseForgotten(Function<Lifecycle, NamespaceExtractionCacheManager> createCacheManager) throws Exception
  {
    initCacheSchedulerTest(createCacheManager);
    scheduleDanglingEntry();
    Assertions.assertEquals(1, scheduler.getActiveEntries());
    while (scheduler.getActiveEntries() > 0) {
      System.gc();
      Thread.sleep(1000);
    }
    Assertions.assertEquals(0, scheduler.getActiveEntries());
  }

  @MethodSource("data")
  @ParameterizedTest
  @Timeout(value = 60_000L, unit = TimeUnit.MILLISECONDS)
  public void testSimpleSubmissionSuccessWithWait(Function<Lifecycle, NamespaceExtractionCacheManager> createCacheManager) throws Exception
  {
    initCacheSchedulerTest(createCacheManager);
    UriExtractionNamespace namespace = new UriExtractionNamespace(
        tmpFile.toURI(),
        null, null,
        new UriExtractionNamespace.ObjectMapperFlatDataParser(
            UriExtractionNamespaceTest.registerTypes(new ObjectMapper())
        ),
        new Period(0),
        null,
        null
    );
    CacheScheduler.Entry entry = scheduler.scheduleAndWait(namespace, 10_000L);
    waitFor(entry);
    Assertions.assertEquals(VALUE, entry.getCache().get(KEY));
  }


  @MethodSource("data")
  @ParameterizedTest
  @Timeout(value = 20_000L, unit = TimeUnit.MILLISECONDS)
  public void testSimpleSubmissionFailureWithWait(Function<Lifecycle, NamespaceExtractionCacheManager> createCacheManager) throws Exception
  {
    initCacheSchedulerTest(createCacheManager);
    JdbcExtractionNamespace namespace = new JdbcExtractionNamespace(
        new MetadataStorageConnectorConfig()
        {
          @Override
          public String getConnectURI()
          {
            return "jdbc:mysql://dummy:3306/db";
          }
        },
        "foo",
        "k",
        "val",
        "time",
        "some filter",
        new Period(10_000),
        null,
        0,
        null,
        new JdbcAccessSecurityConfig()
        {
          @Override
          public Set<String> getAllowedProperties()
          {
            return ImmutableSet.of("valid_key1", "valid_key2");
          }

          @Override
          public boolean isEnforceAllowedProperties()
          {
            return true;
          }
        }
    );
    scheduler.scheduleAndWait(namespace, 40_000L);
  }

  private void scheduleDanglingEntry() throws InterruptedException
  {
    CacheScheduler.Entry entry = scheduler.schedule(getUriExtractionNamespace(5));
    entry.awaitTotalUpdates(1);
  }

  private void checkNoMoreRunning() throws InterruptedException
  {
    Assertions.assertEquals(0, scheduler.getActiveEntries());
    final long pre = scheduler.updatesStarted();
    Thread.sleep(100L);
    Assertions.assertEquals(pre, scheduler.updatesStarted());
  }

  private static File newFolder(File root, String... subDirs)
  {
    return FileUtils.createTempDirInLocation(root.toPath(), String.join("-", subDirs));
  }

  private static void waitForCondition(BooleanSupplier condition, long timeoutMillis) throws InterruptedException
  {
    final long start = System.currentTimeMillis();
    while (!condition.getAsBoolean()) {
      Assertions.assertTrue(System.currentTimeMillis() - start < timeoutMillis);
      Thread.sleep(10L);
    }
  }

  private static class FailingDisposeCacheManager extends OnHeapNamespaceExtractionCacheManager
  {
    private final AtomicInteger failuresRemaining;
    private final AtomicInteger disposeAttempts = new AtomicInteger();

    private FailingDisposeCacheManager(
        Lifecycle lifecycle,
        NoopServiceEmitter serviceEmitter,
        NamespaceExtractionConfig config,
        int failures
    )
    {
      super(lifecycle, serviceEmitter, config);
      this.failuresRemaining = new AtomicInteger(failures);
    }

    @Override
    void disposeCache(CacheHandler cacheHandler)
    {
      disposeAttempts.incrementAndGet();
      if (failuresRemaining.getAndDecrement() > 0) {
        throw new RuntimeException("test dispose failure");
      }
      super.disposeCache(cacheHandler);
    }

    private int getDisposeAttempts()
    {
      return disposeAttempts.get();
    }
  }

  private static class CountingDisposeCacheManager extends OnHeapNamespaceExtractionCacheManager
  {
    private final AtomicInteger disposeAttempts = new AtomicInteger();

    private CountingDisposeCacheManager(
        Lifecycle lifecycle,
        NoopServiceEmitter serviceEmitter,
        NamespaceExtractionConfig config
    )
    {
      super(lifecycle, serviceEmitter, config);
    }

    @Override
    void disposeCache(CacheHandler cacheHandler)
    {
      disposeAttempts.incrementAndGet();
      super.disposeCache(cacheHandler);
    }

    private int getDisposeAttempts()
    {
      return disposeAttempts.get();
    }
  }
}
