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
import org.apache.commons.io.FileUtils;
import org.joda.time.Period;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

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
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 *
 */
public class NamespaceExtractionCacheManagerExecutorsTest
{
  private static final Logger log = new Logger(NamespaceExtractionCacheManagerExecutorsTest.class);
  private static Path tmpDir;
  private Lifecycle lifecycle;
  private NamespaceExtractionCacheManager manager;
  private File tmpFile;
  private URIExtractionNamespaceFunctionFactory factory;
  private final ConcurrentHashMap<String, Function<String, String>> fnCache = new ConcurrentHashMap<String, Function<String, String>>();

  @BeforeClass
  public static void setUpStatic() throws IOException
  {
    tmpDir = Files.createTempDirectory("TestNamespaceExtractionCacheManagerExecutors");
  }

  @AfterClass
  public static void tearDownStatic() throws IOException
  {
    FileUtils.deleteDirectory(tmpDir.toFile());
  }

  @Before
  public void setUp() throws IOException
  {
    lifecycle = new Lifecycle();
    manager = new OnHeapNamespaceExtractionCacheManager(
        lifecycle, fnCache, new NoopServiceEmitter(),
        ImmutableMap.<Class<? extends ExtractionNamespace>, ExtractionNamespaceFunctionFactory<?>>of(
            URIExtractionNamespace.class,
            new URIExtractionNamespaceFunctionFactory(
                ImmutableMap.<String, SearchableVersionedDataFinder>of("file", new LocalFileTimestampVersionFinder())
            )
        )
    );
    tmpFile = Files.createTempFile(tmpDir, "druidTestURIExtractionNS", ".dat").toFile();
    tmpFile.deleteOnExit();
    final ObjectMapper mapper = new DefaultObjectMapper();
    try (OutputStream ostream = new FileOutputStream(tmpFile)) {
      try (OutputStreamWriter out = new OutputStreamWriter(ostream)) {
        out.write(mapper.writeValueAsString(ImmutableMap.<String, String>of("foo", "bar")));
      }
    }
    factory = new URIExtractionNamespaceFunctionFactory(
        ImmutableMap.<String, SearchableVersionedDataFinder>of("file", new LocalFileTimestampVersionFinder())
    )
    {
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
  }


  @Test(timeout = 5_000)
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
    try {
      NamespaceExtractionCacheManagersTest.waitFor(manager.schedule(namespace));
    }
    finally {
      lifecycle.stop();
    }
  }

  @Test(timeout = 5_000)
  public void testRepeatSubmission() throws ExecutionException, InterruptedException
  {
    final int repeatCount = 5;
    final long delay = 5;
    final AtomicLong ranCount = new AtomicLong(0l);
    final long totalRunCount;
    final long start;
    final CountDownLatch latch = new CountDownLatch(repeatCount);
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
      final String cacheId = UUID.randomUUID().toString();
      ListenableFuture<?> future = manager.schedule(
          namespace, factory, new Runnable()
          {
            @Override
            public void run()
            {
              try {
                manager.getPostRunnable(namespace, factory, cacheId).run();
                ranCount.incrementAndGet();
              }
              finally {
                latch.countDown();
              }
            }
          },
          cacheId
      );
      latch.await();
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
    totalRunCount = ranCount.get();
    Thread.sleep(50);
    Assert.assertEquals(totalRunCount, ranCount.get(), 1);
  }


  @Test(timeout = 10_000)
  public void testConcurrentDelete() throws ExecutionException, InterruptedException
  {
    final int threads = 5;
    ListeningExecutorService executorService = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(threads));
    final CountDownLatch latch = new CountDownLatch(threads);
    Collection<ListenableFuture<?>> futures = new ArrayList<>();
    for (int i = 0; i < threads; ++i) {
      final int loopNum = i;
      ListenableFuture<?> future = executorService.submit(
          new Runnable()
          {
            @Override
            public void run()
            {
              try {
                latch.countDown();
                latch.await();
                for (int j = 0; j < 10; ++j) {
                  testDelete(String.format("ns-%d", loopNum));
                }
              }
              catch (InterruptedException e) {
                throw Throwables.propagate(e);
              }
            }
          }
      );
    }
    Futures.allAsList(futures).get();
    executorService.shutdown();
  }

  @Test(timeout = 5_000)
  public void testDelete()
      throws NoSuchFieldException, IllegalAccessException, InterruptedException, ExecutionException
  {
    try {
      testDelete("ns");
    }
    finally {
      lifecycle.stop();
    }
  }

  public void testDelete(final String ns)
      throws InterruptedException
  {
    final CountDownLatch latch = new CountDownLatch(5);
    final CountDownLatch latchMore = new CountDownLatch(10);

    final AtomicLong runs = new AtomicLong(0);
    long prior = 0;
    final URIExtractionNamespace namespace = new URIExtractionNamespace(
        ns,
        tmpFile.toURI(),
        new URIExtractionNamespace.ObjectMapperFlatDataParser(
            URIExtractionNamespaceTest.registerTypes(new ObjectMapper())
        ),
        new Period(1l),
        null
    );
    final String cacheId = UUID.randomUUID().toString();
    final CountDownLatch latchBeforeMore = new CountDownLatch(1);
    ListenableFuture<?> future =
        manager.schedule(
            namespace, factory, new Runnable()
            {
              @Override
              public void run()
              {
                try {
                  if (!Thread.interrupted()) {
                    manager.getPostRunnable(namespace, factory, cacheId).run();
                  } else {
                    Thread.currentThread().interrupt();
                  }
                  if (!Thread.interrupted()) {
                    runs.incrementAndGet();
                  } else {
                    Thread.currentThread().interrupt();
                  }
                }
                finally {
                  latch.countDown();
                  try {
                    if (latch.getCount() == 0) {
                      latchBeforeMore.await();
                    }
                  }
                  catch (InterruptedException e) {
                    log.debug("Interrupted");
                    Thread.currentThread().interrupt();
                  }
                  finally {
                    latchMore.countDown();
                  }
                }
              }
            },
            cacheId
        );
    latch.await();
    prior = runs.get();
    latchBeforeMore.countDown();
    Assert.assertFalse(future.isCancelled());
    Assert.assertFalse(future.isDone());
    Assert.assertTrue(fnCache.containsKey(ns));
    latchMore.await();
    Assert.assertTrue(runs.get() > prior);

    Assert.assertTrue(manager.implData.containsKey(ns));

    manager.delete("ns");
    Assert.assertFalse(manager.implData.containsKey(ns));
    Assert.assertFalse(fnCache.containsKey(ns));
    Assert.assertTrue(future.isCancelled());
    Assert.assertTrue(future.isDone());
    prior = runs.get();
    Thread.sleep(20);
    Assert.assertEquals(prior, runs.get());
  }

  @Test(timeout = 5_000)
  public void testShutdown()
      throws NoSuchFieldException, IllegalAccessException, InterruptedException, ExecutionException
  {
    final CountDownLatch latch = new CountDownLatch(1);
    final ListenableFuture future;
    final AtomicLong runs = new AtomicLong(0);
    long prior = 0;
    try {

      final URIExtractionNamespace namespace = new URIExtractionNamespace(
          "ns",
          tmpFile.toURI(),
          new URIExtractionNamespace.ObjectMapperFlatDataParser(
              URIExtractionNamespaceTest.registerTypes(new ObjectMapper())
          ),
          new Period(1l),
          null
      );
      final String cacheId = UUID.randomUUID().toString();
      final Runnable runnable = manager.getPostRunnable(namespace, factory, cacheId);
      future =
          manager.schedule(
              namespace, factory, new Runnable()
              {
                @Override
                public void run()
                {
                  runnable.run();
                  latch.countDown();
                  runs.incrementAndGet();
                }
              },
              cacheId
          );

      latch.await();
      Assert.assertFalse(future.isCancelled());
      Assert.assertFalse(future.isDone());
      prior = runs.get();
      while (runs.get() <= prior) {
        Thread.sleep(50);
      }
      Assert.assertTrue(runs.get() > prior);
    }
    finally {
      lifecycle.stop();
    }
    manager.waitForServiceToEnd(1_000, TimeUnit.MILLISECONDS);

    prior = runs.get();
    Thread.sleep(50);
    Assert.assertEquals(prior, runs.get());

    Field execField = NamespaceExtractionCacheManager.class.getDeclaredField("listeningScheduledExecutorService");
    execField.setAccessible(true);
    Assert.assertTrue(((ListeningScheduledExecutorService) execField.get(manager)).isShutdown());
    Assert.assertTrue(((ListeningScheduledExecutorService) execField.get(manager)).isTerminated());
  }

  @Test(timeout = 5_000)
  public void testRunCount()
      throws InterruptedException, ExecutionException
  {
    final Lifecycle lifecycle = new Lifecycle();
    final NamespaceExtractionCacheManager onHeap;
    final AtomicLong runCount = new AtomicLong(0);
    final CountDownLatch latch = new CountDownLatch(1);
    try {
      onHeap = new OnHeapNamespaceExtractionCacheManager(
          lifecycle,
          new ConcurrentHashMap<String, Function<String, String>>(),
          new NoopServiceEmitter(),
          ImmutableMap.<Class<? extends ExtractionNamespace>, ExtractionNamespaceFunctionFactory<?>>of(
              URIExtractionNamespace.class,
              new URIExtractionNamespaceFunctionFactory(
                  ImmutableMap.<String, SearchableVersionedDataFinder>of(
                      "file",
                      new LocalFileTimestampVersionFinder()
                  )
              )
          )
      );


      final URIExtractionNamespace namespace = new URIExtractionNamespace(
          "ns",
          tmpFile.toURI(),
          new URIExtractionNamespace.ObjectMapperFlatDataParser(
              URIExtractionNamespaceTest.registerTypes(new ObjectMapper())
          ),
          new Period(1l),
          null
      );
      final String cacheId = UUID.randomUUID().toString();
      ListenableFuture<?> future =
          onHeap.schedule(
              namespace, factory, new Runnable()
              {
                @Override
                public void run()
                {
                  manager.getPostRunnable(namespace, factory, cacheId).run();
                  latch.countDown();
                  runCount.incrementAndGet();
                }
              },
              cacheId
          );
      latch.await();
      Thread.sleep(20);
    }
    finally {
      lifecycle.stop();
    }
    onHeap.waitForServiceToEnd(1_000, TimeUnit.MILLISECONDS);
    Assert.assertTrue(runCount.get() > 5);
  }
}
