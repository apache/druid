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

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.metamx.common.ISE;
import com.metamx.common.lifecycle.Lifecycle;
import io.druid.metadata.MetadataStorageConnectorConfig;
import io.druid.query.extraction.namespace.ExtractionNamespace;
import io.druid.query.extraction.namespace.ExtractionNamespaceFunctionFactory;
import io.druid.query.extraction.namespace.JDBCExtractionNamespace;
import io.druid.server.metrics.NoopServiceEmitter;
import io.druid.server.namespace.JDBCExtractionNamespaceFunctionFactory;
import org.apache.commons.dbcp2.BasicDataSource;
import org.joda.time.Period;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.tweak.HandleCallback;

import java.lang.reflect.Field;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;

/**
 *
 */
@RunWith(Parameterized.class)
public class JDBCExtractionNamespaceTest
{
  private static final String namespace = "testNamespace";
  private static final String tableName = "abstractDbRenameTest";
  private static final String keyName = "keyName";
  private static final String valName = "valName";
  private static final String tsColumn_ = "tsColumn";
  private static final Map<String, String> renames = ImmutableMap.of(
      "foo", "bar",
      "bad", "bar",
      "how about that", "foo"
  );
  private static final String connectionURI = "jdbc:derby:memory:druid;create=true";
  private static DBI dbi;

  @BeforeClass
  public static final void createTables()
  {
    final BasicDataSource datasource = new BasicDataSource();
    datasource.setUrl(connectionURI);
    datasource.setDriverClassLoader(JDBCExtractionNamespaceTest.class.getClassLoader());
    datasource.setDriverClassName("org.apache.derby.jdbc.EmbeddedDriver");
    dbi = new DBI(datasource);
    dbi.withHandle(
        new HandleCallback<Void>()
        {
          @Override
          public Void withHandle(Handle handle) throws Exception
          {
            handle
                .createStatement(
                    String.format(
                        "CREATE TABLE %s (%s TIMESTAMP, %s VARCHAR(64), %s VARCHAR(64))",
                        tableName,
                        tsColumn_,
                        keyName,
                        valName
                    )
                )
                .execute();
            return null;
          }
        }
    );
  }


  @Parameterized.Parameters(name = "{0}")
  public static Collection<Object[]> getParameters()
  {
    return ImmutableList.<Object[]>of(
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
  private final String tsColumn;
  private NamespaceExtractionCacheManager extractionCacheManager;
  private final Lifecycle lifecycle = new Lifecycle();

  @Before
  public void setup()
  {
    dbi.withHandle(
        new HandleCallback<Void>()
        {
          @Override
          public Void withHandle(Handle handle) throws Exception
          {
            handle.createStatement(String.format("TRUNCATE TABLE %s", tableName)).execute();
            handle.commit();
            return null;
          }
        }
    );
    for (Map.Entry<String, String> entry : renames.entrySet()) {
      insertValues(entry.getKey(), entry.getValue(), "2015-01-01 00:00:00");
    }
    final Map<Class<? extends ExtractionNamespace>, ExtractionNamespaceFunctionFactory<?>> factoryMap = new HashMap<>();
    extractionCacheManager = new OnHeapNamespaceExtractionCacheManager(
        lifecycle,
        fnCache,
        new NoopServiceEmitter(),
        ImmutableMap.<Class<? extends ExtractionNamespace>, ExtractionNamespaceFunctionFactory<?>>of(
            JDBCExtractionNamespace.class, new JDBCExtractionNamespaceFunctionFactory()
        )
    );
  }

  @After
  public void tearDown()
  {
    lifecycle.stop();
  }

  private void insertValues(final String key, final String val, final String updateTs)
  {
    dbi.withHandle(
        new HandleCallback<Void>()
        {
          @Override
          public Void withHandle(Handle handle) throws Exception
          {
            final String query;
            if (tsColumn == null) {
              handle.createStatement(
                  String.format("DELETE FROM %s WHERE %s='%s'", tableName, keyName, key)
              ).execute();
              handle.commit();
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
            if (1 != handle.createStatement(query).execute()) {
              throw new ISE("Did not return the correct number of rows");
            }
            handle.commit();
            return null;
          }
        }
    );
  }

  @Test(timeout = 500)
  public void testMapping()
      throws ClassNotFoundException, NoSuchFieldException, IllegalAccessException, ExecutionException,
             InterruptedException
  {
    MetadataStorageConnectorConfig config = new MetadataStorageConnectorConfig();
    Field uriField = MetadataStorageConnectorConfig.class.getDeclaredField("connectURI");
    uriField.setAccessible(true);
    uriField.set(config, connectionURI);

    final JDBCExtractionNamespace extractionNamespace = new JDBCExtractionNamespace(
        namespace,
        config,
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
      Assert.assertEquals(
          val,
          String.format(val, extractionFn.apply(key))
      );
    }
    Assert.assertEquals(
        null,
        extractionFn.apply("baz")
    );
  }


  @Test(timeout = 500)
  public void testSkipOld()
      throws NoSuchFieldException, IllegalAccessException, ExecutionException, InterruptedException
  {
    MetadataStorageConnectorConfig config = new MetadataStorageConnectorConfig();
    Field uriField = MetadataStorageConnectorConfig.class.getDeclaredField("connectURI");
    uriField.setAccessible(true);
    uriField.set(config, connectionURI);
    final JDBCExtractionNamespace extractionNamespace = new JDBCExtractionNamespace(
        namespace,
        config,
        tableName,
        keyName,
        valName,
        tsColumn,
        new Period(1)
    );
    extractionCacheManager.schedule(extractionNamespace);
    while (!fnCache.containsKey(extractionNamespace.getNamespace())) {
      Thread.sleep(1);
    }
    Assert.assertEquals(
        "bar",
        fnCache.get(extractionNamespace.getNamespace()).apply("foo")
    );
    if (tsColumn != null) {
      insertValues("foo", "baz", "1900-01-01 00:00:00");
    }

    Thread.sleep(10);

    Assert.assertEquals(
        "bar",
        fnCache.get(extractionNamespace.getNamespace()).apply("foo")
    );
    extractionCacheManager.delete(namespace);
  }

  @Test(timeout = 500)
  public void testFindNew()
      throws NoSuchFieldException, IllegalAccessException, ExecutionException, InterruptedException
  {
    MetadataStorageConnectorConfig config = new MetadataStorageConnectorConfig();
    Field uriField = MetadataStorageConnectorConfig.class.getDeclaredField("connectURI");
    uriField.setAccessible(true);
    uriField.set(config, connectionURI);
    final JDBCExtractionNamespace extractionNamespace = new JDBCExtractionNamespace(
        namespace,
        config,
        tableName,
        keyName,
        valName,
        tsColumn,
        new Period(1)
    );
    extractionCacheManager.schedule(extractionNamespace);
    while (!fnCache.containsKey(extractionNamespace.getNamespace())) {
      Thread.sleep(1);
    }
    Function<String, String> extractionFn = fnCache.get(extractionNamespace.getNamespace());
    Assert.assertEquals(
        "bar",
        extractionFn.apply("foo")
    );

    insertValues("foo", "baz", "2900-01-01 00:00:00");
    Thread.sleep(100);
    extractionFn = fnCache.get(extractionNamespace.getNamespace());
    Assert.assertEquals(
        "baz",
        extractionFn.apply("foo")
    );
  }
}
