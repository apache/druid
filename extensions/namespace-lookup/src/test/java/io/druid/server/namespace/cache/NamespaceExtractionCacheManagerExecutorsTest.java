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

package io.druid.server.namespace.cache;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.metamx.common.lifecycle.Lifecycle;
import com.metamx.common.logger.Logger;
import io.druid.data.SearchableVersionedDataFinder;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.query.extraction.namespace.ExtractionNamespace;
import io.druid.query.extraction.namespace.ExtractionNamespaceFunctionFactory;
import io.druid.query.extraction.namespace.URIExtractionNamespace;
import io.druid.query.extraction.namespace.URIExtractionNamespaceTest;
import io.druid.segment.loading.LocalFileTimestampVersionFinder;
import io.druid.server.metrics.NoopServiceEmitter;
import io.druid.server.namespace.URIExtractionNamespaceFunctionFactory;
import org.joda.time.Period;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

/**
 *
 */
public class NamespaceExtractionCacheManagerExecutorsTest
{
  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();
  private static final Logger log = new Logger(NamespaceExtractionCacheManagerExecutorsTest.class);
  private Path tmpDir;
  private Lifecycle lifecycle;
  private NamespaceExtractionCacheManager manager;
  private File tmpFile;
  private URIExtractionNamespaceFunctionFactory factory;
  private final ConcurrentHashMap<String, Function<String, String>> fnCache = new ConcurrentHashMap<String, Function<String, String>>();
  private final Object cacheUpdateAlerter = new Object();
  private final AtomicLong numRuns = new AtomicLong(0L);

  @Before
  public void setUp() throws IOException
  {
    tmpDir = temporaryFolder.newFolder().toPath();
    lifecycle = new Lifecycle();
    factory = new URIExtractionNamespaceFunctionFactory(
        ImmutableMap.<String, SearchableVersionedDataFinder>of("file", new LocalFileTimestampVersionFinder())
    )
    {
      @Override
      public Callable<String> getCachePopulator(
          final URIExtractionNamespace extractionNamespace,
          final String lastVersion,
          final Map<String, String> cache
      )
      {
        final Callable<String> superCallable = super.getCachePopulator(extractionNamespace, lastVersion, cache);
        return new Callable<String>()
        {
          @Override
          public String call() throws Exception
          {
            superCallable.call();
            return String.format("%d", System.currentTimeMillis());
          }
        };
      }
    };
    manager = new OnHeapNamespaceExtractionCacheManager(
        lifecycle, fnCache, new NoopServiceEmitter(),
        ImmutableMap.<Class<? extends ExtractionNamespace>, ExtractionNamespaceFunctionFactory<?>>of(
            URIExtractionNamespace.class,
            factory
        )
    )
    {
      @Override
      protected <T extends ExtractionNamespace> Runnable getPostRunnable(
          final T namespace,
          final ExtractionNamespaceFunctionFactory<T> factory,
          final String cacheId
      )
      {
        final Runnable runnable = super.getPostRunnable(namespace, factory, cacheId);
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
    final ObjectMapper mapper = new DefaultObjectMapper();
    try (OutputStream ostream = new FileOutputStream(tmpFile)) {
      try (OutputStreamWriter out = new OutputStreamWriter(ostream)) {
        out.write(mapper.writeValueAsString(ImmutableMap.<String, String>of("foo", "bar")));
      }
    }
  }

  @After
  public void tearDown()
  {
    lifecycle.stop();
  }


  @Test(timeout = 60_000)
  public void testSimpleSubmission() throws ExecutionException, InterruptedException
  {
    URIExtractionNamespace namespace = new URIExtractionNamespace(
        "ns",
        tmpFile.toURI(),
        new URIExtractionNamespace.ObjectMapperFlatDataParser(
            URIExtractionNamespaceTest.registerTypes(new ObjectMapper())
        ),
        new Period(0),
        null
    );
    NamespaceExtractionCacheManagersTest.waitFor(manager.schedule(namespace));
  }

  @Test(timeout = 60_000)
  public void testRepeatSubmission() throws ExecutionException, InterruptedException
  {
    final int repeatCount = 5;
    final long delay = 5;
    final long totalRunCount;
    final long start;
    try {
      final URIExtractionNamespace namespace = new URIExtractionNamespace(
          "ns",
          tmpFile.toURI(),
          new URIExtractionNamespace.ObjectMapperFlatDataParser(
              URIExtractionNamespaceTest.registerTypes(new ObjectMapper())
          ),
          new Period(delay),
          null
      );

      start = System.currentTimeMillis();
      ListenableFuture<?> future = manager.schedule(namespace);

      Assert.assertFalse(future.isDone());
      Assert.assertFalse(future.isCancelled());

      final long preRunCount;
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
          ), minEnd < end
      );
    }
    finally {
      lifecycle.stop();
    }

    totalRunCount = numRuns.get();
    Thread.sleep(delay * 10);
    Assert.assertEquals(totalRunCount, numRuns.get(), 1);
  }


  @Test(timeout = 60_000)
  public void testConcurrentAddDelete() throws ExecutionException, InterruptedException, TimeoutException
  {
    final int threads = 10;
    ListeningExecutorService executorService = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(threads));
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
                    for (int j = 0; j < 10; ++j) {
                      testDelete(String.format("ns-%d-%d", ii, j));
                    }
                  }
                  catch (InterruptedException e) {
                    throw Throwables.propagate(e);
                  }
                }
              }
          )
      );
    }
    Futures.allAsList(futures).get(30, TimeUnit.SECONDS);
    executorService.shutdown();
    checkNoMoreRunning();
  }

  @Test(timeout = 60_000)
  public void testDelete()
      throws NoSuchFieldException, IllegalAccessException, InterruptedException, ExecutionException
  {
    try {
      testDelete("ns");
      checkNoMoreRunning();
    }
    finally {
      lifecycle.stop();
    }
    checkNoMoreRunning();
  }

  public void testDelete(final String ns)
      throws InterruptedException
  {
    final long start = System.currentTimeMillis();
    final long timeout = 10_000L;
    final long period = 10L;
    final URIExtractionNamespace namespace = new URIExtractionNamespace(
        ns,
        tmpFile.toURI(),
        new URIExtractionNamespace.ObjectMapperFlatDataParser(
            URIExtractionNamespaceTest.registerTypes(new ObjectMapper())
        ),
        new Period(period),
        null
    );
    ListenableFuture<?> future = manager.schedule(namespace);
    Assert.assertFalse(future.isCancelled());
    Assert.assertFalse(future.isDone());

    while (!fnCache.containsKey(ns)) {
      if (System.currentTimeMillis() - start > timeout) {
        throw new RuntimeException(new TimeoutException("Namespace took too long to appear in cache"));
      }
      synchronized (cacheUpdateAlerter) {
        cacheUpdateAlerter.wait();
      }
    }

    Assert.assertTrue(fnCache.containsKey(ns));
    Assert.assertTrue(manager.implData.containsKey(ns));

    Assert.assertTrue(manager.delete(ns));

    Assert.assertFalse(manager.implData.containsKey(ns));
    Assert.assertFalse(fnCache.containsKey(ns));
    Assert.assertTrue(future.isCancelled());
    Assert.assertTrue(future.isDone());
  }

  @Test(timeout = 50_000)
  public void testShutdown()
      throws NoSuchFieldException, IllegalAccessException, InterruptedException, ExecutionException
  {
    final long period = 5L;
    final ListenableFuture future;
    long prior = 0;
    try {

      final URIExtractionNamespace namespace = new URIExtractionNamespace(
          "ns",
          tmpFile.toURI(),
          new URIExtractionNamespace.ObjectMapperFlatDataParser(
              URIExtractionNamespaceTest.registerTypes(new ObjectMapper())
          ),
          new Period(period),
          null
      );

      future = manager.schedule(namespace);

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
    try {
      final URIExtractionNamespace namespace = new URIExtractionNamespace(
          "ns",
          tmpFile.toURI(),
          new URIExtractionNamespace.ObjectMapperFlatDataParser(
              URIExtractionNamespaceTest.registerTypes(new ObjectMapper())
          ),
          new Period(5l),
          null
      );
      future = manager.schedule(namespace);
      Assert.assertFalse(future.isDone());
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
    final long pre;
    synchronized (cacheUpdateAlerter) {
      pre = numRuns.get();
    }
    Thread.sleep(100L);
    Assert.assertEquals(pre, numRuns.get());
  }
}
