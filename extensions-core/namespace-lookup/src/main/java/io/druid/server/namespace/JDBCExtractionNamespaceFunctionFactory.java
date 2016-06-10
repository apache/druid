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

package io.druid.server.namespace;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.metamx.common.Pair;
import io.druid.common.utils.JodaUtils;
import io.druid.query.extraction.namespace.ExtractionNamespaceFunctionFactory;
import io.druid.query.extraction.namespace.JDBCExtractionNamespace;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.tweak.HandleCallback;
import org.skife.jdbi.v2.tweak.ResultSetMapper;
import org.skife.jdbi.v2.util.TimestampMapper;

import javax.annotation.Nullable;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 *
 */
public class JDBCExtractionNamespaceFunctionFactory
    implements ExtractionNamespaceFunctionFactory<JDBCExtractionNamespace>
{
  private final ConcurrentMap<String, DBI> dbiCache = new ConcurrentHashMap<>();

  @Override
  public Function<String, String> buildFn(JDBCExtractionNamespace extractionNamespace, final Map<String, String> cache)
  {
    return new Function<String, String>()
    {
      @Nullable
      @Override
      public String apply(String input)
      {
        if (Strings.isNullOrEmpty(input)) {
          return null;
        }
        return Strings.emptyToNull(cache.get(input));
      }
    };
  }

  @Override
  public Function<String, List<String>> buildReverseFn(
      JDBCExtractionNamespace extractionNamespace, final Map<String, String> cache
  )
  {
    return new Function<String, List<String>>()
    {
      @Nullable
      @Override
      public List<String> apply(@Nullable final String value)
      {
        return Lists.newArrayList(Maps.filterKeys(cache, new Predicate<String>()
        {
          @Override public boolean apply(@Nullable String key)
          {
            return cache.get(key).equals(Strings.nullToEmpty(value));
          }
        }).keySet());
      }
    };
  }

  @Override
  public Callable<String> getCachePopulator(
      final JDBCExtractionNamespace namespace,
      final String lastVersion,
      final Map<String, String> cache
  )
  {
    final long lastCheck = lastVersion == null ? JodaUtils.MIN_INSTANT : Long.parseLong(lastVersion);
    final Long lastDBUpdate = lastUpdates(namespace);
    if (lastDBUpdate != null && lastDBUpdate <= lastCheck) {
      return new Callable<String>()
      {
        @Override
        public String call() throws Exception
        {
          return lastVersion;
        }
      };
    }
    return new Callable<String>()
    {
      @Override
      public String call()
      {
        final DBI dbi = ensureDBI(namespace);
        final String table = namespace.getTable();
        final String valueColumn = namespace.getValueColumn();
        final String keyColumn = namespace.getKeyColumn();

        final List<Pair<String, String>> pairs = dbi.withHandle(
            new HandleCallback<List<Pair<String, String>>>()
            {
              @Override
              public List<Pair<String, String>> withHandle(Handle handle) throws Exception
              {
                final String query;
                query = String.format(
                    "SELECT %s, %s FROM %s",
                    keyColumn,
                    valueColumn,
                    table
                );
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
                    ).list();
              }
            }
        );
        for (Pair<String, String> pair : pairs) {
          cache.put(pair.lhs, pair.rhs);
        }
        return String.format("%d", System.currentTimeMillis());
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

  private Long lastUpdates(JDBCExtractionNamespace namespace)
  {
    final DBI dbi = ensureDBI(namespace);
    final String table = namespace.getTable();
    final String tsColumn = namespace.getTsColumn();
    if (tsColumn == null) {
      return null;
    }
    final Timestamp update = dbi.withHandle(
        new HandleCallback<Timestamp>()
        {

          @Override
          public Timestamp withHandle(Handle handle) throws Exception
          {
            final String query = String.format(
                "SELECT MAX(%s) FROM %s",
                tsColumn, table
            );
            return handle
                .createQuery(query)
                .map(TimestampMapper.FIRST)
                .first();
          }
        }
    );
    return update.getTime();
  }

}
