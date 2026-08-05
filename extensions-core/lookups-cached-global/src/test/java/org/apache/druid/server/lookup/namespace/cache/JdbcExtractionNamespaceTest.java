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
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.lifecycle.Lifecycle;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.metadata.MetadataStorageConnectorConfig;
import org.apache.druid.metadata.TestDerbyConnector;
import org.apache.druid.query.lookup.namespace.CacheGenerator;
import org.apache.druid.query.lookup.namespace.ExtractionNamespace;
import org.apache.druid.query.lookup.namespace.JdbcExtractionNamespace;
import org.apache.druid.server.initialization.JdbcAccessSecurityConfig;
import org.apache.druid.server.lookup.namespace.JdbcCacheGenerator;
import org.apache.druid.server.lookup.namespace.NamespaceExtractionConfig;
import org.apache.druid.server.metrics.NoopServiceEmitter;
import org.apache.druid.utils.JvmUtils;
import org.joda.time.Period;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.exceptions.UnableToObtainConnectionException;

import java.io.Closeable;
import java.io.IOException;
import java.sql.SQLException;
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
public class JdbcExtractionNamespaceTest
{
  @RegisterExtension
  public final DerbyConnectorExtension derbyConnectorRule = new DerbyConnectorExtension();

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

  private static class DerbyConnectorExtension implements BeforeEachCallback, AfterEachCallback
  {
    private final TestDerbyConnector connector = new TestDerbyConnector();

    @Override
    public void beforeEach(ExtensionContext context)
    {
      connector.createDatabase();
    }

    @Override
    public void afterEach(ExtensionContext context)
    {
      try {
        new DBI(connector.getJdbcUri() + ";drop=true").open().close();
      }
      catch (UnableToObtainConnectionException e) {
        final SQLException cause = Assertions.assertInstanceOf(
            SQLException.class,
            e.getCause(),
            "Expected Derby shutdown failure to wrap a SQLException"
        );
        Assertions.assertEquals(
            "08006",
            cause.getSQLState(),
            StringUtils.format("Derby not shutdown: [%s]", cause)
        );
      }
    }

    public TestDerbyConnector getConnector()
    {
      return connector;
    }

    public MetadataStorageConnectorConfig getMetadataConnectorConfig()
    {
      return new MetadataStorageConnectorConfig()
      {
        @Override
        public String getConnectURI()
        {
          return connector.getJdbcUri();
        }
      };
    }
  }


  public static Collection<Object[]> getParameters()
  {
    return ImmutableList.of(
        new Object[]{"tsColumn"},
        new Object[]{null}
    );
  }

  public void initJdbcExtractionNamespaceTest(
      String tsColumn
  )
  {
    this.tsColumn = tsColumn;
    this.lifecycle = new Lifecycle();
    try {
      initializeJdbcExtractionNamespaceTest();
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private String tsColumn;
  private CacheScheduler scheduler;
  private Lifecycle lifecycle;
  private AtomicLong updates;
  private Lock updateLock;
  private Closer closer;
  private ListeningExecutorService setupTeardownService;
  private Handle handleRef = null;

  private void initializeJdbcExtractionNamespaceTest() throws Exception
  {
    updates = new AtomicLong(0L);
    updateLock = new ReentrantLock(true);
    closer = Closer.create();
    setupTeardownService =
        MoreExecutors.listeningDecorator(Execs.multiThreaded(2, "JDBCExtractionNamespaceTeardown--%s"));
    final ListenableFuture<Handle> setupFuture = setupTeardownService.submit(
        () -> {
          final Handle handle = derbyConnectorRule.getConnector().getDBI().open();
          Assertions.assertEquals(
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
            Assertions.assertEquals(0, scheduler.getActiveEntries());
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
                        new JdbcCacheGenerator(JvmUtils.getRuntimeInfo());

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
    Assertions.assertNotNull(handleRef);
  }

  @AfterEach
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
    Assertions.assertEquals(1, handle.createStatement(query).setQueryTimeout(1).execute());
    handle.commit();
    // Some internals have timing resolution no better than MS. This is to help make sure that checks for timings
    // have elapsed at least to the next ms... 2 is for good measure.
    Thread.sleep(2);
  }

  @MethodSource("getParameters")
  @ParameterizedTest(name = "{0}")
  @Timeout(value = 60_000L, unit = TimeUnit.MILLISECONDS)
  public void testMappingWithoutFilter(String tsColumn)
      throws InterruptedException
  {
    initJdbcExtractionNamespaceTest(tsColumn);
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
        Assertions.assertEquals(
            field,
            map.get(key),
            "non-null check"
        );
      }
      Assertions.assertEquals(null, map.get("baz"), "null check");
    }
  }

  @MethodSource("getParameters")
  @ParameterizedTest(name = "{0}")
  @Timeout(value = 60_000L, unit = TimeUnit.MILLISECONDS)
  public void testMappingWithFilter(String tsColumn)
      throws InterruptedException
  {
    initJdbcExtractionNamespaceTest(tsColumn);
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
          Assertions.assertEquals(
              field,
              map.get(key),
              "non-null check"
          );
        } else {
          Assertions.assertEquals(null, map.get(key), "non-null check");
        }
      }
    }
  }

  @MethodSource("getParameters")
  @ParameterizedTest(name = "{0}")
  @Timeout(value = 60_000L, unit = TimeUnit.MILLISECONDS)
  public void testEmptyTable(String tsColumn)
      throws InterruptedException
  {
    initJdbcExtractionNamespaceTest(tsColumn);
    // Delete existing rows from table.
    final Handle handle = derbyConnectorRule.getConnector().getDBI().open();
    handle.createStatement(
        StringUtils.format("DELETE FROM %s", TABLE_NAME)
    ).setQueryTimeout(1).execute();

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
      Assertions.assertTrue(map.isEmpty());
    }
  }

  @MethodSource("getParameters")
  @ParameterizedTest(name = "{0}")
  @Timeout(value = 60_000L, unit = TimeUnit.MILLISECONDS)
  public void testSkipOld(String tsColumn)
      throws InterruptedException
  {
    initJdbcExtractionNamespaceTest(tsColumn);
    try (final CacheScheduler.Entry entry = ensureEntry()) {
      assertUpdated(entry, "foo", "bar");
      if (tsColumn != null) {
        insertValues(handleRef, "foo", "baz", null, "1900-01-01 00:00:00");
      }
      assertUpdated(entry, "foo", "bar");
    }
  }

  @MethodSource("getParameters")
  @ParameterizedTest(name = "{0}")
  public void testRandomJitter(String tsColumn)
  {
    initJdbcExtractionNamespaceTest(tsColumn);
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
    Assertions.assertTrue(jitter >= 0 && jitter <= 120000);
  }

  @MethodSource("getParameters")
  @ParameterizedTest(name = "{0}")
  public void testRandomJitterNotSpecified(String tsColumn)
  {
    initJdbcExtractionNamespaceTest(tsColumn);
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
    Assertions.assertEquals(0, extractionNamespace.getJitterMills());
  }

  @MethodSource("getParameters")
  @ParameterizedTest(name = "{0}")
  @Timeout(value = 60_000L, unit = TimeUnit.MILLISECONDS)
  public void testFindNew(String tsColumn)
      throws InterruptedException
  {
    initJdbcExtractionNamespaceTest(tsColumn);
    try (final CacheScheduler.Entry entry = ensureEntry()) {
      assertUpdated(entry, "foo", "bar");
      insertValues(handleRef, "foo", "baz", null, "2900-01-01 00:00:00");
      assertUpdated(entry, "foo", "baz");
    }
  }

  @MethodSource("getParameters")
  @ParameterizedTest(name = "{0}")
  @Timeout(value = 60_000L, unit = TimeUnit.MILLISECONDS)
  public void testIgnoresNullValues(String tsColumn)
      throws InterruptedException
  {
    initJdbcExtractionNamespaceTest(tsColumn);
    try (final CacheScheduler.Entry entry = ensureEntry()) {
      insertValues(handleRef, "fooz", null, null, "2900-01-01 00:00:00");
      waitForUpdates(1_000L, 2L);
      Thread.sleep(100);
      Set set = entry.getCache().keySet();
      Assertions.assertFalse(set.contains("fooz"));
    }
  }

  @MethodSource("getParameters")
  @ParameterizedTest(name = "{0}")
  public void testSerde(String tsColumn) throws IOException
  {
    initJdbcExtractionNamespaceTest(tsColumn);
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

    Assertions.assertEquals(extractionNamespace, extractionNamespace2);
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

    Assertions.assertEquals(
        "bar",
        entry.getCache().get("foo"),
        "sanity check not correct"
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
        Assertions.assertTrue(System.currentTimeMillis() - startTime < timeout, "Failed waiting for update");
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

    Assertions.assertEquals(
        expected,
        map.get(key),
        "update check"
    );
  }
}
