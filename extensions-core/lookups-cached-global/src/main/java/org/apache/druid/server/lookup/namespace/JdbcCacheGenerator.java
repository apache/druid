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
import org.apache.druid.data.input.MapPopulator;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.JodaUtils;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.lookup.namespace.CacheGenerator;
import org.apache.druid.query.lookup.namespace.JdbcExtractionNamespace;
import org.apache.druid.server.lookup.namespace.cache.CacheHandler;
import org.apache.druid.server.lookup.namespace.cache.CacheScheduler;
import org.apache.druid.utils.JvmUtils;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.ResultIterator;
import org.skife.jdbi.v2.exceptions.UnableToObtainConnectionException;
import org.skife.jdbi.v2.util.TimestampMapper;

import javax.annotation.Nullable;
import java.sql.Timestamp;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 *
 */
public final class JdbcCacheGenerator implements CacheGenerator<JdbcExtractionNamespace>
{
  private static final Logger LOG = new Logger(JdbcCacheGenerator.class);
  private static final String NO_SUITABLE_DRIVER_FOUND_ERROR = "No suitable driver found";
  private static final String JDBC_DRIVER_JAR_FILES_MISSING_ERROR =
      "JDBC driver JAR files missing from extensions/druid-lookups-cached-global directory";
  private static final long MAX_MEMORY = JvmUtils.getRuntimeInfo().getMaxHeapSizeBytes();
  private final ConcurrentMap<CacheScheduler.EntryImpl<JdbcExtractionNamespace>, DBI> dbiCache =
      new ConcurrentHashMap<>();

  @Override
  @Nullable
  public String generateCache(
      final JdbcExtractionNamespace namespace,
      final CacheScheduler.EntryImpl<JdbcExtractionNamespace> entryId,
      final String lastVersion,
      final CacheHandler cache
  )
  {
    final long lastCheck = lastVersion == null ? JodaUtils.MIN_INSTANT : Long.parseLong(lastVersion);
    final Long lastDBUpdate;
    final long dbQueryStart;

    try {
      lastDBUpdate = lastUpdates(entryId, namespace);
      if (lastDBUpdate != null && lastDBUpdate <= lastCheck) {
        return null;
      }
    }
    catch (UnableToObtainConnectionException e) {
      if (e.getMessage().contains(NO_SUITABLE_DRIVER_FOUND_ERROR)) {
        throw new ISE(e, JDBC_DRIVER_JAR_FILES_MISSING_ERROR);
      } else {
        throw e;
      }
    }
    dbQueryStart = System.currentTimeMillis();

    LOG.debug("Updating %s", entryId);

    final String newVersion;
    if (lastDBUpdate != null) {
      newVersion = lastDBUpdate.toString();
    } else {
      newVersion = StringUtils.format("%d", dbQueryStart);
    }

    final long startNs = System.nanoTime();
    try (
        Handle handle = getHandle(entryId, namespace);
        ResultIterator<Pair<String, String>> pairs = getLookupPairs(handle, namespace)
    ) {
      final MapPopulator.PopulateResult populateResult = MapPopulator.populateAndWarnAtByteLimit(
          pairs,
          cache.getCache(),
          (long) (MAX_MEMORY * namespace.getMaxHeapPercentage() / 100.0),
          null == entryId ? null : entryId.toString()
      );
      final long duration = System.nanoTime() - startNs;
      LOG.info(
          "Finished loading %d values (%d bytes) for [%s] in %d ns",
          populateResult.getEntries(),
          populateResult.getBytes(),
          entryId,
          duration
      );
      return newVersion;
    }
    catch (UnableToObtainConnectionException e) {
      if (e.getMessage().contains(NO_SUITABLE_DRIVER_FOUND_ERROR)) {
        throw new ISE(e, JDBC_DRIVER_JAR_FILES_MISSING_ERROR);
      } else {
        throw e;
      }
    }
    catch (Throwable t) {
      try {
        cache.close();
      }
      catch (Exception e) {
        t.addSuppressed(e);
      }
      throw t;
    }
  }

  private Handle getHandle(
      final CacheScheduler.EntryImpl<JdbcExtractionNamespace> key,
      final JdbcExtractionNamespace namespace
  )
  {
    final DBI dbi = ensureDBI(key, namespace);
    return dbi.open();
  }

  private ResultIterator<Pair<String, String>> getLookupPairs(
      final Handle handle,
      final JdbcExtractionNamespace namespace
  )
  {
    final String table = namespace.getTable();
    final String filter = namespace.getFilter();
    final String valueColumn = namespace.getValueColumn();
    final String keyColumn = namespace.getKeyColumn();

    return handle.createQuery(buildLookupQuery(table, filter, keyColumn, valueColumn))
            .map((index1, r1, ctx1) -> new Pair<>(r1.getString(1), r1.getString(2)))
            .iterator();
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
