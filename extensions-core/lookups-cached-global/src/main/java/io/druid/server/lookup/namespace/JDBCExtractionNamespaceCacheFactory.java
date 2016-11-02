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

package io.druid.server.lookup.namespace;

import com.google.common.base.Function;
import io.druid.common.utils.JodaUtils;
import io.druid.java.util.common.logger.Logger;
import io.druid.query.lookup.namespace.ExtractionNamespaceCacheFactory;
import io.druid.query.lookup.namespace.JDBCExtractionNamespace;
import io.druid.query.lookup.namespace.KeyValueMap;
import org.apache.commons.collections.keyvalue.MultiKey;
import org.apache.commons.lang.StringUtils;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.tweak.HandleCallback;
import org.skife.jdbi.v2.tweak.ResultSetMapper;
import org.skife.jdbi.v2.util.TimestampMapper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 *
 */
public class JDBCExtractionNamespaceCacheFactory
    extends ExtractionNamespaceCacheFactory<JDBCExtractionNamespace>
{
  private static final Logger LOG = new Logger(JDBCExtractionNamespaceCacheFactory.class);
  private final ConcurrentMap<String, DBI> dbiCache = new ConcurrentHashMap<>();

  @Override
  public String populateCache(
      final String id,
      final JDBCExtractionNamespace namespace,
      final String lastVersion,
      final ConcurrentMap<MultiKey, Map<String, String>> cache,
      final Function<MultiKey, Map<String, String>> mapAllocator
  ) throws Exception
  {
    final long lastCheck = lastVersion == null ? JodaUtils.MIN_INSTANT : Long.parseLong(lastVersion);
    final Long lastDBUpdate = lastUpdates(id, namespace);
    if (lastDBUpdate != null && lastDBUpdate <= lastCheck) {
      return lastVersion;
    }
    final long dbQueryStart = System.currentTimeMillis();
    final DBI dbi = ensureDBI(id, namespace);
    final String table = namespace.getTable();

    final List<String> requiredFields = KeyValueMap.getRequiredFields(namespace.getMaps());
    final Map<String, Integer> fieldMap = new HashMap<>();
    final int numFields = requiredFields.size();
    for (int idx = 0; idx < numFields; idx++)
    {
      fieldMap.put(requiredFields.get(idx), idx);
    }

    LOG.debug("Updating [%s]", namespace.toString());
    final List<String[]> columns = dbi.withHandle(
        new HandleCallback<List<String[]>>()
        {
          @Override
          public List<String[]> withHandle(Handle handle) throws Exception
          {
            final String query;
            query = String.format(
                makeQueryString(requiredFields),
                table
            );
            return handle
                .createQuery(
                    query
                ).map(
                    new ResultSetMapper<String[]>()
                    {

                      @Override
                      public String[] map(
                          final int index,
                          final ResultSet r,
                          final StatementContext ctx
                      ) throws SQLException
                      {
                        String[] values = new String[numFields];
                        int idx = 0;

                        for (String field: requiredFields)
                        {
                          values[idx++] = r.getString(field);
                        }
                        return values;
                      }
                    }
                ).list();
          }
        }
    );
    for (String[] values : columns) {
      for (KeyValueMap keyValueMap: namespace.getMaps())
      {
        String mapName = keyValueMap.getMapName();
        MultiKey key = new MultiKey(id, mapName);
        Map<String, String> innerMap = cache.get(key);
        if (innerMap == null)
        {
          innerMap = mapAllocator.apply(key);
          cache.put(key, innerMap);
        }
        innerMap.put(values[fieldMap.get(keyValueMap.getKeyColumn())], values[fieldMap.get(keyValueMap.getValueColumn())]);
      }
    }
    LOG.info("Finished loading %d values for namespace[%s]", cache.size(), id);
    if (lastDBUpdate != null) {
      return lastDBUpdate.toString();
    } else {
      return String.format("%d", dbQueryStart);
    }
  }

  private String makeQueryString(List<String> requiredFields)
  {
    String query = "SELECT \"";
    query += StringUtils.join(requiredFields, "\", \"");
    query += "\" from %s";

    return query;
  }

  private DBI ensureDBI(String id, JDBCExtractionNamespace namespace)
  {
    final String key = id;
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

  private Long lastUpdates(String id, JDBCExtractionNamespace namespace)
  {
    final DBI dbi = ensureDBI(id, namespace);
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
                "SELECT MAX(\"%s\") FROM %s",
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
