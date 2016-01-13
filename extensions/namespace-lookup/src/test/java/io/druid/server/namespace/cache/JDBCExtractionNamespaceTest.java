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

package io.druid.server.namespace.cache;

import com.google.common.base.Function;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import com.google.common.io.Closer;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.metamx.common.lifecycle.Lifecycle;
import com.metamx.common.logger.Logger;
import io.druid.concurrent.Execs;
import io.druid.metadata.TestDerbyConnector;
import io.druid.query.extraction.namespace.ExtractionNamespace;
import io.druid.query.extraction.namespace.ExtractionNamespaceFunctionFactory;
import io.druid.query.extraction.namespace.JDBCExtractionNamespace;
import io.druid.server.metrics.NoopServiceEmitter;
import io.druid.server.namespace.JDBCExtractionNamespaceFunctionFactory;
import org.joda.time.Period;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.skife.jdbi.v2.Handle;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 *
 */
@RunWith(Parameterized.class)
public class JDBCExtractionNamespaceTest
{
  @Rule
  public final TestDerbyConnector.DerbyConnectorRule derbyConnectorRule = new TestDerbyConnector.DerbyConnectorRule();
  private static final Logger log = new Logger(JDBCExtractionNamespaceTest.class);
  private static final String namespace = "testNamespace";
  private static final String tableName = "abstractDbRenameTest";
  private static final String keyName = "keyName";
  private static final String valName = "valName";
  private static final String tsColumn_ = "tsColumn";
  private static final Map<String, String> renames = ImmutableMap.of(
      "foo", "bar",
      "bad", "bar",
      "how about that", "foo",
      "empty string", ""
  );

  @Parameterized.Parameters(name = "{0}")
  public static Collection<Object[]> getParameters()
  {
    return ImmutableList.of(
        new Object[]{"tsColumn"},
        new Object[]{null}
    );
  }

  public JDBCExtractionNamespaceTest(
      String tsColumn
  )
  {
    this.tsColumn = tsColumn;
  }

  private final ConcurrentMap<String, Function<String, String>> fnCache = new ConcurrentHashMap<>();
  private final ConcurrentMap<String, Function<String, List<String>>> reverseFnCache = new ConcurrentHashMap<>();
  private final String tsColumn;
  private OnHeapNamespaceExtractionCacheManager extractionCacheManager;
  private final Lifecycle lifecycle = new Lifecycle();
  private final AtomicLong updates = new AtomicLong(0L);
  private final Lock updateLock = new ReentrantLock(true);
  private final Closer closer = Closer.create();
  private final ListeningExecutorService setupTeardownService =
      MoreExecutors.listeningDecorator(Execs.multiThreaded(2, "JDBCExtractionNamespaceTeardown--%s"));
  private Handle handleRef = null;

  @Before
  public void setup() throws Exception
  {
    final ListenableFuture<Handle> setupFuture = setupTeardownService.submit(
        new Callable<Handle>()
        {
          @Override
          public Handle call()
          {
            final Handle handle = derbyConnectorRule.getConnector().getDBI().open();
            Assert.assertEquals(
                0,
                handle.createStatement(
                    String.format(
                        "CREATE TABLE %s (%s TIMESTAMP, %s VARCHAR(64), %s VARCHAR(64))",
                        tableName,
                        tsColumn_,
                        keyName,
                        valName
                    )
                ).setQueryTimeout(1).execute()
            );
            handle.createStatement(String.format("TRUNCATE TABLE %s", tableName)).setQueryTimeout(1).execute();
            handle.commit();
            closer.register(new Closeable()
            {
              @Override
              public void close() throws IOException
              {
                handle.createStatement("DROP TABLE " + tableName).setQueryTimeout(1).execute();
                final ListenableFuture future = setupTeardownService.submit(new Runnable()
                {
                  @Override
                  public void run()
                  {
                    handle.close();
                  }
                });
                try (Closeable closeable = new Closeable()
                {
                  @Override
                  public void close() throws IOException
                  {
                    future.cancel(true);
                  }
                }) {
                  future.get(10, TimeUnit.SECONDS);
                }
                catch (InterruptedException | ExecutionException | TimeoutException e) {
                  throw new IOException("Error closing handle", e);
                }
              }
            });
            closer.register(new Closeable()
            {
              @Override
              public void close() throws IOException
              {
                if (extractionCacheManager == null) {
                  return;
                }
                final NamespaceExtractionCacheManager.NamespaceImplData implData = extractionCacheManager.implData.get(
                    namespace);
                if (implData != null && implData.future != null) {
                  implData.future.cancel(true);
                  Assert.assertTrue(implData.future.isDone());
                }
              }
            });
            for (Map.Entry<String, String> entry : renames.entrySet()) {
              try {
                insertValues(handle, entry.getKey(), entry.getValue(), "2015-01-01 00:00:00");
              }
              catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw Throwables.propagate(e);
              }
            }

            extractionCacheManager = new OnHeapNamespaceExtractionCacheManager(
                lifecycle,
                fnCache,
                reverseFnCache,
                new NoopServiceEmitter(),
                ImmutableMap.<Class<? extends ExtractionNamespace>, ExtractionNamespaceFunctionFactory<?>>of(
                    JDBCExtractionNamespace.class,
                    new JDBCExtractionNamespaceFunctionFactory()
                    {
                      @Override
                      public Callable<String> getCachePopulator(
                          final JDBCExtractionNamespace namespace,
                          final String lastVersion,
                          final Map<String, String> cache
                      )
                      {
                        final Callable<String> cachePopulator = super.getCachePopulator(namespace, lastVersion, cache);
                        return new Callable<String>()
                        {
                          @Override
                          public String call() throws Exception
                          {
                            updateLock.lockInterruptibly();
                            try {
                              log.debug("Running cache populator");
                              try {
                                return cachePopulator.call();
                              }
                              finally {
                                updates.incrementAndGet();
                              }
                            }
                            finally {
                              updateLock.unlock();
                            }
                          }
                        };
                      }
                    }
                )
            );
            try {
              lifecycle.start();
            }
            catch (Exception e) {
              throw Throwables.propagate(e);
            }
            closer.register(
                new Closeable()
                {
                  @Override
                  public void close() throws IOException
                  {
                    final ListenableFuture future = setupTeardownService.submit(
                        new Runnable()
                        {
                          @Override
                          public void run()
                          {
                            lifecycle.stop();
                          }
                        }
                    );
                    try (final Closeable closeable = new Closeable()
                    {
                      @Override
                      public void close() throws IOException
                      {
                        future.cancel(true);
                      }
                    }) {
                      future.get(30, TimeUnit.SECONDS);
                    }
                    catch (InterruptedException | ExecutionException | TimeoutException e) {
                      throw new IOException("Error stopping lifecycle", e);
                    }
                  }
                }
            );
            return handle;
          }
        }
    );

    try (final Closeable closeable =
             new Closeable()
             {
               @Override
               public void close() throws IOException
               {
                 if (!setupFuture.isDone() && !setupFuture.cancel(true) && !setupFuture.isDone()) {
                   throw new IOException("Unable to stop future");
                 }
               }
             }) {
      handleRef = setupFuture.get(10, TimeUnit.SECONDS);
    }
    Assert.assertNotNull(handleRef);
  }

  @After
  public void tearDown() throws InterruptedException, ExecutionException, TimeoutException, IOException
  {
    final ListenableFuture<?> tearDownFuture = setupTeardownService.submit(
        new Runnable()
        {
          @Override
          public void run()
          {
            try {
              closer.close();
            }
            catch (IOException e) {
              throw Throwables.propagate(e);
            }
          }
        }
    );
    try (final Closeable closeable = new Closeable()
    {
      @Override
      public void close() throws IOException
      {
        setupTeardownService.shutdownNow();
        try {
          if (!setupTeardownService.awaitTermination(60, TimeUnit.SECONDS)) {
            log.error("Tear down service didn't finish");
          }
        }
        catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new IOException("Interrupted", e);
        }
      }
    }) {
      tearDownFuture.get(60, TimeUnit.SECONDS);
    }
    finally {
      if (Thread.interrupted()) {
        log.info("Thread was interrupted. Clearing interrupt and continuing.");
      }
    }
  }

  private void insertValues(final Handle handle, final String key, final String val, final String updateTs)
      throws InterruptedException
  {
    final String query;
    if (tsColumn == null) {
      handle.createStatement(
          String.format("DELETE FROM %s WHERE %s='%s'", tableName, keyName, key)
      ).setQueryTimeout(1).execute();
      query = String.format(
          "INSERT INTO %s (%s, %s) VALUES ('%s', '%s')",
          tableName,
          keyName, valName,
          key, val
      );
    } else {
      query = String.format(
          "INSERT INTO %s (%s, %s, %s) VALUES ('%s', '%s', '%s')",
          tableName,
          tsColumn, keyName, valName,
          updateTs, key, val
      );
    }
    Assert.assertEquals(1, handle.createStatement(query).setQueryTimeout(1).execute());
    handle.commit();
    // Some internals have timing resolution no better than MS. This is to help make sure that checks for timings
    // have elapsed at least to the next ms... 2 is for good measure.
    Thread.sleep(2);
  }

  @Test(timeout = 10_000L)
  public void testMapping()
      throws ClassNotFoundException, NoSuchFieldException, IllegalAccessException, ExecutionException,
             InterruptedException, TimeoutException
  {
    final JDBCExtractionNamespace extractionNamespace = new JDBCExtractionNamespace(
        namespace,
        derbyConnectorRule.getMetadataConnectorConfig(),
        tableName,
        keyName,
        valName,
        tsColumn,
        new Period(0)
    );
    NamespaceExtractionCacheManagersTest.waitFor(extractionCacheManager.schedule(extractionNamespace));
    Function<String, String> extractionFn = fnCache.get(extractionNamespace.getNamespace());

    for (Map.Entry<String, String> entry : renames.entrySet()) {
      String key = entry.getKey();
      String val = entry.getValue();
      Assert.assertEquals("non-null check", Strings.emptyToNull(val), extractionFn.apply(key));
    }
    Assert.assertEquals("null check", null, extractionFn.apply("baz"));
  }

  @Test(timeout = 10_000L)
  public void testReverseLookup() throws InterruptedException
  {
    final JDBCExtractionNamespace extractionNamespace = new JDBCExtractionNamespace(
        namespace,
        derbyConnectorRule.getMetadataConnectorConfig(),
        tableName,
        keyName,
        valName,
        tsColumn,
        new Period(0)
    );
    NamespaceExtractionCacheManagersTest.waitFor(extractionCacheManager.schedule(extractionNamespace));
    Function<String, List<String>> reverseExtractionFn = reverseFnCache.get(extractionNamespace.getNamespace());
    Assert.assertEquals(
        "reverse lookup should match",
        Sets.newHashSet("foo", "bad"),
        Sets.newHashSet(reverseExtractionFn.apply("bar"))
    );
    Assert.assertEquals(
        "reverse lookup should match",
        Sets.newHashSet("how about that"),
        Sets.newHashSet(reverseExtractionFn.apply("foo"))
    );
    Assert.assertEquals(
        "reverse lookup should match",
        Sets.newHashSet("empty string"),
        Sets.newHashSet(reverseExtractionFn.apply(""))
    );
    Assert.assertEquals(
        "null is same as empty string",
        Sets.newHashSet("empty string"),
        Sets.newHashSet(reverseExtractionFn.apply(null))
    );
    Assert.assertEquals(
        "reverse lookup of none existing value should be empty list",
        Collections.EMPTY_LIST,
        reverseExtractionFn.apply("does't exist")
    );
  }

  @Test(timeout = 10_000L)
  public void testSkipOld()
      throws NoSuchFieldException, IllegalAccessException, ExecutionException, InterruptedException
  {
    final JDBCExtractionNamespace extractionNamespace = ensureNamespace();

    assertUpdated(extractionNamespace.getNamespace(), "foo", "bar");

    if (tsColumn != null) {
      insertValues(handleRef, "foo", "baz", "1900-01-01 00:00:00");
    }

    assertUpdated(extractionNamespace.getNamespace(), "foo", "bar");
  }

  @Test(timeout = 60_000L)
  public void testFindNew()
      throws NoSuchFieldException, IllegalAccessException, ExecutionException, InterruptedException
  {
    final JDBCExtractionNamespace extractionNamespace = ensureNamespace();

    assertUpdated(extractionNamespace.getNamespace(), "foo", "bar");

    insertValues(handleRef, "foo", "baz", "2900-01-01 00:00:00");

    assertUpdated(extractionNamespace.getNamespace(), "foo", "baz");
  }

  private JDBCExtractionNamespace ensureNamespace()
      throws NoSuchFieldException, IllegalAccessException, InterruptedException
  {
    final JDBCExtractionNamespace extractionNamespace = new JDBCExtractionNamespace(
        namespace,
        derbyConnectorRule.getMetadataConnectorConfig(),
        tableName,
        keyName,
        valName,
        tsColumn,
        new Period(10)
    );
    extractionCacheManager.schedule(extractionNamespace);

    waitForUpdates(1_000L, 2L);

    Assert.assertEquals(
        "sanity check not correct",
        "bar",
        fnCache.get(extractionNamespace.getNamespace()).apply("foo")
    );
    return extractionNamespace;
  }

  private void waitForUpdates(long timeout, long numUpdates) throws InterruptedException
  {
    long startTime = System.currentTimeMillis();
    long pre = 0L;
    updateLock.lockInterruptibly();
    try {
      pre = updates.get();
    }
    finally {
      updateLock.unlock();
    }
    long post = 0L;
    do {
      // Sleep to spare a few cpu cycles
      Thread.sleep(5);
      log.debug("Waiting for updateLock");
      updateLock.lockInterruptibly();
      try {
        Assert.assertTrue("Failed waiting for update", System.currentTimeMillis() - startTime < timeout);
        post = updates.get();
      }
      finally {
        updateLock.unlock();
      }
    } while (post < pre + numUpdates);
  }

  private void assertUpdated(String namespace, String key, String expected) throws InterruptedException
  {
    waitForUpdates(1_000L, 2L);

    Function<String, String> extractionFn = fnCache.get(namespace);

    // rely on test timeout to break out of this loop
    while (!extractionFn.apply(key).equals(expected)) {
      Thread.sleep(100);
      extractionFn = fnCache.get(namespace);
    }

    Assert.assertEquals(
        "update check",
        expected,
        extractionFn.apply(key)
    );
  }
}
