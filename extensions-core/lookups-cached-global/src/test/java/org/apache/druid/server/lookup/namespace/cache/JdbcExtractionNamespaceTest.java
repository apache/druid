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

import com.fasterxml.jackson.databind.InjectableValues.Std;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.lifecycle.Lifecycle;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.metadata.TestDerbyConnector;
import org.apache.druid.query.lookup.namespace.CacheGenerator;
import org.apache.druid.query.lookup.namespace.ExtractionNamespace;
import org.apache.druid.query.lookup.namespace.JdbcExtractionNamespace;
import org.apache.druid.server.initialization.JdbcAccessSecurityConfig;
import org.apache.druid.server.lookup.namespace.JdbcCacheGenerator;
import org.apache.druid.server.lookup.namespace.NamespaceExtractionConfig;
import org.apache.druid.server.metrics.NoopServiceEmitter;
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
import java.util.Map;
import java.util.Set;
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
public class JdbcExtractionNamespaceTest
{
  static {
    NullHandling.initializeForTests();
  }

  @Rule
  public final TestDerbyConnector.DerbyConnectorRule derbyConnectorRule = new TestDerbyConnector.DerbyConnectorRule();

  private static final Logger log = new Logger(JdbcExtractionNamespaceTest.class);
  private static final String TABLE_NAME = "abstractDbRenameTest";
  private static final String KEY_NAME = "keyName";
  private static final String VAL_NAME = "valName";
  private static final String TS_COLUMN = "tsColumn";
  private static final String FILTER_COLUMN = "filterColumn";
  private static final Map<String, String[]> RENAMES = ImmutableMap.of(
      "foo", new String[]{"bar", "1"},
      "bad", new String[]{"bar", "1"},
      "how about that", new String[]{"foo", "0"},
      "empty string", new String[]{"empty string", "0"}
  );


  @Parameterized.Parameters(name = "{0}")
  public static Collection<Object[]> getParameters()
  {
    return ImmutableList.of(
        new Object[]{"tsColumn"},
        new Object[]{null}
    );
  }

  public JdbcExtractionNamespaceTest(
      String tsColumn
  )
  {
    this.tsColumn = tsColumn;
  }

  private final String tsColumn;
  private CacheScheduler scheduler;
  private Lifecycle lifecycle;
  private AtomicLong updates;
  private Lock updateLock;
  private Closer closer;
  private ListeningExecutorService setupTeardownService;
  private Handle handleRef = null;

  @Before
  public void setup() throws Exception
  {
    lifecycle = new Lifecycle();
    updates = new AtomicLong(0L);
    updateLock = new ReentrantLock(true);
    closer = Closer.create();
    setupTeardownService =
        MoreExecutors.listeningDecorator(Execs.multiThreaded(2, "JDBCExtractionNamespaceTeardown--%s"));
    final ListenableFuture<Handle> setupFuture = setupTeardownService.submit(
        () -> {
          final Handle handle = derbyConnectorRule.getConnector().getDBI().open();
          Assert.assertEquals(
              0,
              handle.createStatement(
                  StringUtils.format(
                      "CREATE TABLE %s (%s TIMESTAMP, %s VARCHAR(64), %s VARCHAR(64), %s VARCHAR(64))",
                      TABLE_NAME,
                      TS_COLUMN,
                      FILTER_COLUMN,
                      KEY_NAME,
                      VAL_NAME
                  )
              ).setQueryTimeout(1).execute()
          );
          handle.createStatement(StringUtils.format("TRUNCATE TABLE %s", TABLE_NAME)).setQueryTimeout(1).execute();
          handle.commit();
          closer.register(() -> {
            handle.createStatement("DROP TABLE " + TABLE_NAME).setQueryTimeout(1).execute();
            final ListenableFuture future = setupTeardownService.submit(new Runnable()
            {
              @Override
              public void run()
              {
                handle.close();
              }
            });
            try (Closeable ignored = () -> future.cancel(true)) {
              future.get(10, TimeUnit.SECONDS);
            }
            catch (InterruptedException | ExecutionException | TimeoutException e) {
              throw new IOException("Error closing handle", e);
            }
          });
          closer.register(() -> {
            if (scheduler == null) {
              return;
            }
            Assert.assertEquals(0, scheduler.getActiveEntries());
          });
          for (Map.Entry<String, String[]> entry : RENAMES.entrySet()) {
            try {
              String key = entry.getKey();
              String value = entry.getValue()[0];
              String filter = entry.getValue()[1];
              insertValues(handle, key, value, filter, "2015-01-01 00:00:00");
            }
            catch (InterruptedException e) {
              Thread.currentThread().interrupt();
              throw new RuntimeException(e);
            }
          }

          NoopServiceEmitter noopServiceEmitter = new NoopServiceEmitter();
          scheduler = new CacheScheduler(
              noopServiceEmitter,
              ImmutableMap.of(
                  JdbcExtractionNamespace.class,
                  new CacheGenerator<JdbcExtractionNamespace>()
                  {
                    private final JdbcCacheGenerator delegate =
                        new JdbcCacheGenerator();

                    @Override
                    public String generateCache(
                        final JdbcExtractionNamespace namespace,
                        final CacheScheduler.EntryImpl<JdbcExtractionNamespace> id,
                        final String lastVersion,
                        final CacheHandler cache
                    ) throws InterruptedException
                    {
                      updateLock.lockInterruptibly();
                      try {
                        log.debug("Running cache generator");
                        try {
                          return delegate.generateCache(namespace, id, lastVersion, cache);
                        }
                        finally {
                          updates.incrementAndGet();
                        }
                      }
                      finally {
                        updateLock.unlock();
                      }
                    }
                  }
              ),
              new OnHeapNamespaceExtractionCacheManager(
                  lifecycle,
                  noopServiceEmitter,
                  new NamespaceExtractionConfig()
              )
          );
          try {
            lifecycle.start();
          }
          catch (Exception e) {
            throw new RuntimeException(e);
          }
          closer.register(
              () -> {
                final ListenableFuture future = setupTeardownService.submit(() -> lifecycle.stop());
                try (final Closeable ignored = () -> future.cancel(true)) {
                  future.get(30, TimeUnit.SECONDS);
                }
                catch (InterruptedException | ExecutionException | TimeoutException e) {
                  throw new IOException("Error stopping lifecycle", e);
                }
              }
          );
          return handle;
        }
    );

    try (final Closeable ignore = () -> setupFuture.cancel(true)) {
      handleRef = setupFuture.get(10, TimeUnit.SECONDS);
    }
    Assert.assertNotNull(handleRef);
  }

  @After
  public void tearDown() throws InterruptedException, ExecutionException, TimeoutException, IOException
  {
    final ListenableFuture<?> tearDownFuture = setupTeardownService.submit(
        () -> {
          try {
            closer.close();
          }
          catch (IOException e) {
            throw new RuntimeException(e);
          }
        }
    );
    try (final Closeable ignored = () -> {
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
    }) {
      tearDownFuture.get(60, TimeUnit.SECONDS);
    }
    finally {
      if (Thread.interrupted()) {
        log.info("Thread was interrupted. Clearing interrupt and continuing.");
      }
    }
  }

  private void insertValues(
      final Handle handle,
      final String key,
      final String val,
      final String filter,
      final String updateTs
  )
      throws InterruptedException
  {
    final String query;
    final String statementVal = val != null ? "'%s'" : "%s";
    if (tsColumn == null) {
      handle.createStatement(
          StringUtils.format("DELETE FROM %s WHERE %s='%s'", TABLE_NAME, KEY_NAME, key)
      ).setQueryTimeout(1).execute();
      query = StringUtils.format(
          "INSERT INTO %s (%s, %s, %s) VALUES ('%s', '%s', " + statementVal + ")",
          TABLE_NAME,
          FILTER_COLUMN, KEY_NAME, VAL_NAME,
          filter, key, val
      );
    } else {
      query = StringUtils.format(
          "INSERT INTO %s (%s, %s, %s, %s) VALUES ('%s', '%s', '%s', " + statementVal + ")",
          TABLE_NAME,
          tsColumn, FILTER_COLUMN, KEY_NAME, VAL_NAME,
          updateTs, filter, key, val
      );
    }
    Assert.assertEquals(1, handle.createStatement(query).setQueryTimeout(1).execute());
    handle.commit();
    // Some internals have timing resolution no better than MS. This is to help make sure that checks for timings
    // have elapsed at least to the next ms... 2 is for good measure.
    Thread.sleep(2);
  }

  @Test(timeout = 60_000L)
  public void testMappingWithoutFilter()
      throws InterruptedException
  {
    final JdbcExtractionNamespace extractionNamespace = new JdbcExtractionNamespace(
        derbyConnectorRule.getMetadataConnectorConfig(),
        TABLE_NAME,
        KEY_NAME,
        VAL_NAME,
        tsColumn,
        null,
        new Period(0),
        null,
        0,
        null,
        new JdbcAccessSecurityConfig()
    );
    try (CacheScheduler.Entry entry = scheduler.schedule(extractionNamespace)) {
      CacheSchedulerTest.waitFor(entry);
      final Map<String, String> map = entry.getCache();

      for (Map.Entry<String, String[]> e : RENAMES.entrySet()) {
        String key = e.getKey();
        String[] val = e.getValue();
        String field = val[0];
        Assert.assertEquals(
            "non-null check",
            NullHandling.emptyToNullIfNeeded(field),
            NullHandling.emptyToNullIfNeeded(map.get(key))
        );
      }
      Assert.assertEquals("null check", null, map.get("baz"));
    }
  }

  @Test(timeout = 60_000L)
  public void testMappingWithFilter()
      throws InterruptedException
  {
    final JdbcExtractionNamespace extractionNamespace = new JdbcExtractionNamespace(
        derbyConnectorRule.getMetadataConnectorConfig(),
        TABLE_NAME,
        KEY_NAME,
        VAL_NAME,
        tsColumn,
        FILTER_COLUMN + "='1'",
        new Period(0),
        null,
        0,
        null,
        new JdbcAccessSecurityConfig()
    );
    try (CacheScheduler.Entry entry = scheduler.schedule(extractionNamespace)) {
      CacheSchedulerTest.waitFor(entry);
      final Map<String, String> map = entry.getCache();

      for (Map.Entry<String, String[]> e : RENAMES.entrySet()) {
        String key = e.getKey();
        String[] val = e.getValue();
        String field = val[0];
        String filterVal = val[1];

        if ("1".equals(filterVal)) {
          Assert.assertEquals(
              "non-null check",
              NullHandling.emptyToNullIfNeeded(field),
              NullHandling.emptyToNullIfNeeded(map.get(key))
          );
        } else {
          Assert.assertEquals("non-null check", null, NullHandling.emptyToNullIfNeeded(map.get(key)));
        }
      }
    }
  }

  @Test(timeout = 60_000L)
  public void testSkipOld()
      throws InterruptedException
  {
    try (final CacheScheduler.Entry entry = ensureEntry()) {
      assertUpdated(entry, "foo", "bar");
      if (tsColumn != null) {
        insertValues(handleRef, "foo", "baz", null, "1900-01-01 00:00:00");
      }
      assertUpdated(entry, "foo", "bar");
    }
  }

  @Test
  public void testRandomJitter()
  {
    JdbcExtractionNamespace extractionNamespace = new JdbcExtractionNamespace(
        derbyConnectorRule.getMetadataConnectorConfig(),
        TABLE_NAME,
        KEY_NAME,
        VAL_NAME,
        tsColumn,
        FILTER_COLUMN + "='1'",
        new Period(0),
        null,
        120,
        null,
        new JdbcAccessSecurityConfig()
    );
    long jitter = extractionNamespace.getJitterMills();
    // jitter will be a random value between 0 and 120 seconds.
    Assert.assertTrue(jitter >= 0 && jitter <= 120000);
  }

  @Test
  public void testRandomJitterNotSpecified()
  {
    JdbcExtractionNamespace extractionNamespace = new JdbcExtractionNamespace(
        derbyConnectorRule.getMetadataConnectorConfig(),
        TABLE_NAME,
        KEY_NAME,
        VAL_NAME,
        tsColumn,
        FILTER_COLUMN + "='1'",
        new Period(0),
        null,
        0,
        null,
        new JdbcAccessSecurityConfig()
    );
    // jitter will be a random value between 0 and 120 seconds.
    Assert.assertEquals(0, extractionNamespace.getJitterMills());
  }

  @Test(timeout = 60_000L)
  public void testFindNew()
      throws InterruptedException
  {
    try (final CacheScheduler.Entry entry = ensureEntry()) {
      assertUpdated(entry, "foo", "bar");
      insertValues(handleRef, "foo", "baz", null, "2900-01-01 00:00:00");
      assertUpdated(entry, "foo", "baz");
    }
  }

  @Test(timeout = 60_000L)
  public void testIgnoresNullValues()
      throws InterruptedException
  {
    try (final CacheScheduler.Entry entry = ensureEntry()) {
      insertValues(handleRef, "fooz", null, null, "2900-01-01 00:00:00");
      waitForUpdates(1_000L, 2L);
      Thread.sleep(100);
      Set set = entry.getCache().keySet();
      Assert.assertFalse(set.contains("fooz"));
    }
  }

  @Test
  public void testSerde() throws IOException
  {
    final JdbcAccessSecurityConfig securityConfig = new JdbcAccessSecurityConfig();
    final JdbcExtractionNamespace extractionNamespace = new JdbcExtractionNamespace(
        derbyConnectorRule.getMetadataConnectorConfig(),
        TABLE_NAME,
        KEY_NAME,
        VAL_NAME,
        tsColumn,
        "some filter",
        new Period(10),
        null,
        0,
        null,
        securityConfig
    );
    final ObjectMapper mapper = new DefaultObjectMapper();
    mapper.setInjectableValues(new Std().addValue(JdbcAccessSecurityConfig.class, securityConfig));

    final ExtractionNamespace extractionNamespace2 = mapper.readValue(
        mapper.writeValueAsBytes(extractionNamespace),
        ExtractionNamespace.class
    );

    Assert.assertEquals(extractionNamespace, extractionNamespace2);
  }

  private CacheScheduler.Entry ensureEntry()
      throws InterruptedException
  {
    final JdbcExtractionNamespace extractionNamespace = new JdbcExtractionNamespace(
        derbyConnectorRule.getMetadataConnectorConfig(),
        TABLE_NAME,
        KEY_NAME,
        VAL_NAME,
        tsColumn,
        null,
        new Period(10),
        null,
        0,
        null,
        new JdbcAccessSecurityConfig()
    );
    CacheScheduler.Entry entry = scheduler.schedule(extractionNamespace);

    waitForUpdates(1_000L, 2L);

    Assert.assertEquals(
        "sanity check not correct",
        "bar",
        entry.getCache().get("foo")
    );
    return entry;
  }

  private void waitForUpdates(long timeout, long numUpdates) throws InterruptedException
  {
    long startTime = System.currentTimeMillis();
    long pre;
    updateLock.lockInterruptibly();
    try {
      pre = updates.get();
    }
    finally {
      updateLock.unlock();
    }
    long post;
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

  private void assertUpdated(CacheScheduler.Entry entry, String key, String expected) throws InterruptedException
  {
    waitForUpdates(1_000L, 2L);

    Map<String, String> map = entry.getCache();

    // rely on test timeout to break out of this loop
    while (!expected.equals(map.get(key))) {
      Thread.sleep(100);
      map = entry.getCache();
    }

    Assert.assertEquals(
        "update check",
        expected,
        map.get(key)
    );
  }
}
