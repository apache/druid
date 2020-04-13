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

package org.apache.druid.server.lookup.namespace;

import com.google.common.base.Strings;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.JodaUtils;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.lookup.namespace.CacheGenerator;
import org.apache.druid.query.lookup.namespace.JdbcExtractionNamespace;
import org.apache.druid.server.lookup.namespace.cache.CacheScheduler;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.exceptions.UnableToObtainConnectionException;
import org.skife.jdbi.v2.util.TimestampMapper;

import javax.annotation.Nullable;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 *
 */
public final class JdbcCacheGenerator implements CacheGenerator<JdbcExtractionNamespace>
{
  private static final Logger LOG = new Logger(JdbcCacheGenerator.class);
  private final ConcurrentMap<CacheScheduler.EntryImpl<JdbcExtractionNamespace>, DBI> dbiCache =
      new ConcurrentHashMap<>();

  @Override
  @Nullable
  public CacheScheduler.VersionedCache generateCache(
      final JdbcExtractionNamespace namespace,
      final CacheScheduler.EntryImpl<JdbcExtractionNamespace> entryId,
      final String lastVersion,
      final CacheScheduler scheduler
  )
  {
    final long lastCheck = lastVersion == null ? JodaUtils.MIN_INSTANT : Long.parseLong(lastVersion);
    final Long lastDBUpdate;
    final List<Pair<String, String>> pairs;
    final long dbQueryStart;

    try {
      lastDBUpdate = lastUpdates(entryId, namespace);
      if (lastDBUpdate != null && lastDBUpdate <= lastCheck) {
        return null;
      }
      dbQueryStart = System.currentTimeMillis();

      LOG.debug("Updating %s", entryId);
      pairs = getLookupPairs(entryId, namespace);
    }
    catch (UnableToObtainConnectionException e) {
      if (e.getMessage().contains("No suitable driver found")) {
        throw new ISE(
            e,
            "JDBC driver JAR files missing from extensions/druid-lookups-cached-global directory"
        );
      } else {
        throw e;
      }
    }

    final String newVersion;
    if (lastDBUpdate != null) {
      newVersion = lastDBUpdate.toString();
    } else {
      newVersion = StringUtils.format("%d", dbQueryStart);
    }
    final CacheScheduler.VersionedCache versionedCache = scheduler.createVersionedCache(entryId, newVersion);
    try {
      final Map<String, String> cache = versionedCache.getCache();
      for (Pair<String, String> pair : pairs) {
        cache.put(pair.lhs, pair.rhs);
      }
      LOG.info("Finished loading %d values for %s", cache.size(), entryId);
      return versionedCache;
    }
    catch (Throwable t) {
      try {
        versionedCache.close();
      }
      catch (Exception e) {
        t.addSuppressed(e);
      }
      throw t;
    }
  }

  private List<Pair<String, String>> getLookupPairs(
      final CacheScheduler.EntryImpl<JdbcExtractionNamespace> key,
      final JdbcExtractionNamespace namespace
  )
  {
    final DBI dbi = ensureDBI(key, namespace);
    final String table = namespace.getTable();
    final String filter = namespace.getFilter();
    final String valueColumn = namespace.getValueColumn();
    final String keyColumn = namespace.getKeyColumn();

    return dbi.withHandle(
        handle -> handle
            .createQuery(buildLookupQuery(table, filter, keyColumn, valueColumn))
            .map((index, r, ctx) -> new Pair<>(r.getString(keyColumn), r.getString(valueColumn)))
            .list()
    );
  }

  private static String buildLookupQuery(String table, String filter, String keyColumn, String valueColumn)
  {
    if (Strings.isNullOrEmpty(filter)) {
      return StringUtils.format(
          "SELECT %s, %s FROM %s WHERE %s IS NOT NULL",
          keyColumn,
          valueColumn,
          table,
          valueColumn
      );
    }

    return StringUtils.format(
        "SELECT %s, %s FROM %s WHERE %s AND %s IS NOT NULL",
        keyColumn,
        valueColumn,
        table,
        filter,
        valueColumn
    );
  }

  private DBI ensureDBI(CacheScheduler.EntryImpl<JdbcExtractionNamespace> key, JdbcExtractionNamespace namespace)
  {
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

  @Nullable
  private Long lastUpdates(CacheScheduler.EntryImpl<JdbcExtractionNamespace> key, JdbcExtractionNamespace namespace)
  {
    final DBI dbi = ensureDBI(key, namespace);
    final String table = namespace.getTable();
    final String tsColumn = namespace.getTsColumn();
    if (tsColumn == null) {
      return null;
    }
    final Timestamp update = dbi.withHandle(
        handle -> {
          final String query = StringUtils.format(
              "SELECT MAX(%s) FROM %s",
              tsColumn, table
          );
          return handle
              .createQuery(query)
              .map(TimestampMapper.FIRST)
              .first();
        }
    );
    return update.getTime();
  }
}
