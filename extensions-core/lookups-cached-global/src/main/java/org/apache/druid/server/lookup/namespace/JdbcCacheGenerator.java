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
import org.joda.time.Period;
import org.joda.time.format.PeriodFormat;
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
    final List<Pair<String, String>> newCacheEntries;
    final long dbQueryStart;
    final boolean doIncrementalLoad;

    try {
      lastDBUpdate = lastUpdates(entryId, namespace);
      if (lastDBUpdate != null && lastDBUpdate <= lastCheck) {
        return null;
      }
      dbQueryStart = System.currentTimeMillis();
      doIncrementalLoad = lastDBUpdate != null && !Strings.isNullOrEmpty(namespace.getTsColumn())
          && lastVersion != null;
      LOG.debug("Updating %s", entryId);
      newCacheEntries = getLookupEntries(entryId, namespace, doIncrementalLoad, lastCheck);
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
    if (doIncrementalLoad) {
      newVersion = StringUtils.format("%d", lastDBUpdate);
      CacheScheduler.VersionedCache versionedCache = entryId.createFromExistingCache(entryId, newVersion, newCacheEntries);
      LOG.info("Finished loading %d new incremental values in last %s for %s ",
          newCacheEntries.size(),
          PeriodFormat.getDefault().print(new Period(lastCheck, lastDBUpdate)),
          entryId
      );
      // In case of exceptions while loading, we do not close versionedCache here,
      // its preferable to have stale entries for incremental load rather than closing the cache altogether.
      return versionedCache;
    } else {
      CacheScheduler.VersionedCacheBuilder versionedCacheBuilder = null;
      try {
        LOG.debug("Not doing incremental load because either " +
                "namespace.getTsColumn() is not set or this is the first load." +
                " lastDBUpdate: %s, namespace.getTsColumn(): %s, lastVersion: %s",
            lastDBUpdate,
            namespace.getTsColumn(),
            lastVersion);
        if (lastDBUpdate != null) {
          // Setting newVersion to lastDBUpdate will ensure that during next load
          // we read the keys that were modified after lastDBUpdate.
          // See how lastCheck is being set in the beginning of generateCache().
          newVersion = StringUtils.format("%d", lastDBUpdate);
        } else {
          newVersion = StringUtils.format("%d", dbQueryStart);
        }
        versionedCacheBuilder = scheduler.createVersionedCache(entryId, newVersion, null);
        CacheScheduler.VersionedCache versionedCache = versionedCacheBuilder.getVersionedCache();
        final Map<String, String> cache = versionedCache.getCache();
        for (Pair<String, String> pair : newCacheEntries) {
          cache.put(pair.lhs, pair.rhs);
        }
        LOG.info("Finished loading %d values for %s", cache.size(), entryId);
        return versionedCache;
      }
      catch (Throwable t) {
        try {
          if (versionedCacheBuilder != null) {
            versionedCacheBuilder.close();
          }
        }
        catch (Exception e) {
          t.addSuppressed(e);
        }
        throw t;
      }
    }
  }

  private List<Pair<String, String>> getLookupEntries(
      final CacheScheduler.EntryImpl<JdbcExtractionNamespace> key,
      final JdbcExtractionNamespace namespace,
      boolean doIncrementalLoad,
      long lastCheck
  )
  {
    final DBI dbi = ensureDBI(key, namespace);
    final String table = namespace.getTable();
    final String filter = namespace.getFilter();
    final String valueColumn = namespace.getValueColumn();
    final String keyColumn = namespace.getKeyColumn();

    String sqlQuery = doIncrementalLoad ?
        buildIncrementalLookupQuery(key, namespace, namespace.getTsColumn(), lastCheck) :
        buildLookupQuery(table, filter, keyColumn, valueColumn);


    return dbi.withHandle(
        handle -> handle
            .createQuery(sqlQuery)
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

  private String buildIncrementalLookupQuery(
      final CacheScheduler.EntryImpl<JdbcExtractionNamespace> key,
      final JdbcExtractionNamespace namespace,
      String tsColumn, Long lastLoadTs)
  {
    final DBI dbi = ensureDBI(key, namespace);
    final String table = namespace.getTable();
    final String filter = namespace.getFilter();
    final String valueColumn = namespace.getValueColumn();
    final String keyColumn = namespace.getKeyColumn();

    if (Strings.isNullOrEmpty(filter)) {
      return StringUtils.format(
          "SELECT %s, %s FROM %s WHERE %s >= '%s' AND %s IS NOT NULL",
          keyColumn,
          valueColumn,
          table,
          tsColumn,
          new Timestamp(lastLoadTs).toString(),
          valueColumn
      );
    }
    return StringUtils.format(
        "SELECT %s, %s FROM %s WHERE %s%s IS NOT NULL",
        keyColumn,
        valueColumn,
        table,
        Strings.isNullOrEmpty(filter) ? "" : filter + " AND ",
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
