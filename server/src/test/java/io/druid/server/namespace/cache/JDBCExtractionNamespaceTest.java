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
import io.druid.query.extraction.namespace.JDBCExtractionNamespace;
import io.druid.server.namespace.JDBCExtractionNamespaceFunctionFactory;
import org.apache.commons.dbcp2.BasicDataSource;
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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

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


  @Parameterized.Parameters
  public static Collection<Object[]> getParameters()
  {
    return ImmutableList.<Object[]>of(
        new Object[]{null, "tsColumn"},
        new Object[]{new ConcurrentHashMap<String, String>(), "tsColumn"},
        new Object[]{new ConcurrentHashMap<String, String>(), null},
        new Object[]{null, null}
    );
  }

  public JDBCExtractionNamespaceTest(
      ConcurrentMap<String, String> cache,
      String tsColumn
  )
  {
    this.cache = cache;
    this.tsColumn = tsColumn;
  }

  private final ConcurrentMap<String, String> cache;
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
    if (cache != null) {
      cache.clear();
    }
    extractionCacheManager = new NamespaceExtractionCacheManager(lifecycle)
    {
      @Override
      public ConcurrentMap<String, String> getCacheMap(String namespace)
      {
        return cache;
      }

      @Override
      public Collection<String> getKnownNamespaces()
      {
        return ImmutableList.of();
      }
    };
  }
  @After
  public void tearDown(){
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

  @Test
  public void testMapping() throws ClassNotFoundException, NoSuchFieldException, IllegalAccessException
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
        tsColumn
    );
    JDBCExtractionNamespaceFunctionFactory factory = new JDBCExtractionNamespaceFunctionFactory(extractionCacheManager);
    Runnable populateCache = factory.getCachePopulator(extractionNamespace);
    Function<String, String> extractionFn = factory.build(extractionNamespace);
    if (cache != null) {
      populateCache.run();
    }
    for (Map.Entry<String, String> entry : renames.entrySet()) {
      String key = entry.getKey();
      String val = entry.getValue();
      Assert.assertEquals(
          String.format("Failed on key [%s] with cache [%s] ts [%s]", key, cache, tsColumn),
          val,
          String.format(val, extractionFn.apply(key))
      );
    }
    Assert.assertEquals(
        String.format("Failed with cache [%s] ts [%s]", cache, tsColumn),
        null,
        extractionFn.apply("baz")
    );
  }


  @Test
  public void testSkipOld() throws NoSuchFieldException, IllegalAccessException
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
        tsColumn
    );
    final JDBCExtractionNamespaceFunctionFactory factory = new JDBCExtractionNamespaceFunctionFactory(extractionCacheManager);
    final Runnable populateCache = factory.getCachePopulator(extractionNamespace);
    final Function<String, String> extractionFn = factory.build(extractionNamespace);
    if (cache != null) {
      populateCache.run();
    }
    if (tsColumn != null) {
      insertValues("foo", "baz", "1900-01-01 00:00:00");
    }
    if (cache != null) {
      populateCache.run();
    }
    Assert.assertEquals(
        String.format("Failed with cache [%s] ts [%s]", cache, tsColumn),
        "bar",
        extractionFn.apply("foo")
    );
  }

  @Test
  public void testFindNew() throws NoSuchFieldException, IllegalAccessException
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
        tsColumn
    );
    final JDBCExtractionNamespaceFunctionFactory factory = new JDBCExtractionNamespaceFunctionFactory(extractionCacheManager);
    final Runnable populateCache = factory.getCachePopulator(extractionNamespace);
    final Function<String, String> extractionFn = factory.build(extractionNamespace);
    if (cache != null) {
      populateCache.run();
    }
    insertValues("foo", "baz", "2900-01-01 00:00:00");
    if (cache != null) {
      populateCache.run();
    }
    Assert.assertEquals(
        String.format("Failed with cache [%s] ts [%s]", cache, tsColumn),
        "baz",
        extractionFn.apply("foo")
    );
  }
}
