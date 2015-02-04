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

package io.druid.server.namespace;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import com.metamx.common.Pair;
import io.druid.query.extraction.namespace.ExtractionNamespaceFunctionFactory;
import io.druid.query.extraction.namespace.JDBCExtractionNamespace;
import io.druid.server.namespace.cache.NamespaceExtractionCacheManager;
import org.joda.time.DateTime;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.FoldController;
import org.skife.jdbi.v2.Folder3;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.tweak.HandleCallback;
import org.skife.jdbi.v2.tweak.ResultSetMapper;
import org.skife.jdbi.v2.util.StringMapper;

import javax.annotation.Nullable;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 *
 */
public class JDBCExtractionNamespaceFunctionFactory
    implements ExtractionNamespaceFunctionFactory<JDBCExtractionNamespace>
{
  private final ConcurrentMap<String, DBI> dbiCache = new ConcurrentHashMap<>();
  private final ConcurrentMap<String, AtomicLong> lastUpdates = new ConcurrentHashMap<>();
  private final NamespaceExtractionCacheManager extractionCacheManager;

  @Inject
  public JDBCExtractionNamespaceFunctionFactory(
      NamespaceExtractionCacheManager extractionCacheManager
  )
  {
    this.extractionCacheManager = extractionCacheManager;
  }

  @Override
  public Function<String, String> build(final JDBCExtractionNamespace namespace)
  {
    final ConcurrentMap<String, String> cache = extractionCacheManager.getCacheMap(namespace.getNamespace());
    final String tsColumn = namespace.getTsColumn();
    final String table = namespace.getTable();
    final String valueColumn = namespace.getValueColumn();
    final String keyColumn = namespace.getKeyColumn();
    return new Function<String, String>()
    {
      @Nullable
      @Override
      public String apply(final @Nullable String input)
      {
        final String retval;
        if (cache == null) {
          retval = ensureDBI(namespace).withHandle(
              new HandleCallback<String>()
              {
                @Override
                public String withHandle(Handle handle) throws Exception
                {
                  final String query;
                  if (tsColumn == null) {
                    query = String.format(
                        "SELECT %s FROM %s WHERE %s='%s'",
                        valueColumn,
                        table,
                        keyColumn,
                        input
                    );
                  } else {
                    query = String.format(
                        "SELECT %s FROM %s WHERE %s='%s' ORDER BY %s DESC",
                        valueColumn,
                        table,
                        keyColumn,
                        input,
                        tsColumn
                    );
                  }
                  return handle
                      .createQuery(query)
                      .map(StringMapper.FIRST)
                      .first();
                }
              }
          );
        } else {
          retval = cache.get(input);
        }
        return retval;
      }
    };
  }

  @Override
  public Runnable getCachePopulator(final JDBCExtractionNamespace namespace)
  {
    lastUpdates.putIfAbsent(namespace.getNamespace(), new AtomicLong());
    return new Runnable()
    {
      @Override
      public void run()
      {
        final ConcurrentMap<String, String> cache = extractionCacheManager.getCacheMap(namespace.getNamespace());
        Preconditions.checkNotNull(cache, "Must call setCache first");
        final DBI dbi = ensureDBI(namespace);
        final String tsColumn = namespace.getTsColumn();
        final String table = namespace.getTable();
        final String valueColumn = namespace.getValueColumn();
        final String keyColumn = namespace.getKeyColumn();
        final AtomicLong lastCheck = lastUpdates.get(namespace.getNamespace());
        List<Pair<String, String>> pairs = dbi.withHandle(
            new HandleCallback<List<Pair<String, String>>>()
            {
              @Override
              public List<Pair<String, String>> withHandle(Handle handle) throws Exception
              {
                final String query;
                if (tsColumn == null) {
                  query = String.format(
                      "SELECT %s, %s FROM %s",
                      keyColumn,
                      valueColumn,
                      table
                  );
                } else {
                  query = String.format(
                      "SELECT %s, %s FROM %s WHERE %s > '%s' ORDER BY %s ASC",
                      keyColumn, valueColumn,
                      table,
                      tsColumn,
                      new DateTime(lastCheck.get()).toString("yyyy-MM-dd hh:mm:ss"),
                      tsColumn
                  );
                  lastCheck.set(System.currentTimeMillis());
                }
                return handle
                    .createQuery(
                        query
                    ).map(
                        new ResultSetMapper<Pair<String, String>>()
                        {

                          @Override
                          public Pair<String, String> map(
                              final int index,
                              final ResultSet r,
                              final StatementContext ctx
                          ) throws SQLException
                          {
                            return new Pair<String, String>(r.getString(keyColumn), r.getString(valueColumn));
                          }
                        }
                    ).fold(
                        new LinkedList<Pair<String, String>>(),
                        new Folder3<LinkedList<Pair<String, String>>, Pair<String, String>>()
                        {
                          @Override
                          public LinkedList<Pair<String, String>> fold(
                              LinkedList<Pair<String, String>> accumulator,
                              Pair<String, String> rs,
                              FoldController control,
                              StatementContext ctx
                          ) throws SQLException
                          {
                            accumulator.add(rs);
                            return accumulator;
                          }
                        }
                    );
              }
            }
        );
        for (Pair<String, String> pair : pairs) {
          cache.put(pair.lhs, pair.rhs);
        }
      }
    };
  }

  private DBI ensureDBI(JDBCExtractionNamespace namespace)
  {
    final String key = namespace.getNamespace();
    DBI dbi = null;
    if (dbiCache.containsKey(key)) {
      dbi = dbiCache.get(key);
    }
    if (dbi == null) {
      final DBI newDbi = new DBI(
          namespace.getConnectorConfig().getConnectURI(),
          namespace.getConnectorConfig().getUser(),
          namespace.getConnectorConfig().getPassword()
      );
      dbiCache.putIfAbsent(key, newDbi);
      dbi = dbiCache.get(key);
    }
    return dbi;
  }

}
