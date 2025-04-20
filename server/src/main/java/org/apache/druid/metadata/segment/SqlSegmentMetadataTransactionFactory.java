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

package org.apache.druid.metadata.segment;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import org.apache.druid.client.indexing.IndexingService;
import org.apache.druid.discovery.DruidLeaderSelector;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.metadata.MetadataStorageTablesConfig;
import org.apache.druid.metadata.SQLMetadataConnector;
import org.apache.druid.metadata.segment.cache.Metric;
import org.apache.druid.metadata.segment.cache.SegmentMetadataCache;
import org.apache.druid.query.DruidMetrics;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.TransactionStatus;

/**
 * Factory for {@link SegmentMetadataTransaction}s. If the
 * {@link SegmentMetadataCache} is enabled and ready, the transaction may
 * read/write from the cache as applicable.
 * <p>
 * This class serves as a wrapper over the {@link SQLMetadataConnector} to
 * perform transactions specific to segment metadata.
 */
public class SqlSegmentMetadataTransactionFactory implements SegmentMetadataTransactionFactory
{
  private static final Logger log = new Logger(SqlSegmentMetadataTransactionFactory.class);

  private static final int QUIET_RETRIES = 2;
  private static final int MAX_RETRIES = 3;

  private final ObjectMapper jsonMapper;
  private final MetadataStorageTablesConfig tablesConfig;
  private final SQLMetadataConnector connector;
  private final DruidLeaderSelector leaderSelector;
  private final SegmentMetadataCache segmentMetadataCache;
  private final ServiceEmitter emitter;

  @Inject
  public SqlSegmentMetadataTransactionFactory(
      ObjectMapper jsonMapper,
      MetadataStorageTablesConfig tablesConfig,
      SQLMetadataConnector connector,
      @IndexingService DruidLeaderSelector leaderSelector,
      SegmentMetadataCache segmentMetadataCache,
      ServiceEmitter emitter
  )
  {
    this.jsonMapper = jsonMapper;
    this.tablesConfig = tablesConfig;
    this.connector = connector;
    this.leaderSelector = leaderSelector;
    this.segmentMetadataCache = segmentMetadataCache;
    this.emitter = emitter;
  }

  public int getMaxRetries()
  {
    return MAX_RETRIES;
  }

  @Override
  public <T> T inReadOnlyDatasourceTransaction(
      String dataSource,
      SegmentMetadataReadTransaction.Callback<T> callback
  )
  {
    return connector.retryReadOnlyTransaction(
        (handle, status) -> {
          final SegmentMetadataTransaction sqlTransaction
              = createSqlTransaction(dataSource, handle, status);

          // For read-only transactions, use cache only if it is already synced
          if (segmentMetadataCache.isSyncedForRead()) {
            emitTransactionCount(Metric.READ_ONLY_TRANSACTIONS, dataSource);
            return segmentMetadataCache.readCacheForDataSource(dataSource, dataSourceCache -> {
              final SegmentMetadataReadTransaction cachedTransaction
                  = new CachedSegmentMetadataTransaction(sqlTransaction, dataSourceCache, leaderSelector, true);
              return executeReadAndClose(cachedTransaction, callback);
            });
          } else {
            return executeReadAndClose(sqlTransaction, callback);
          }
        },
        QUIET_RETRIES,
        getMaxRetries()
    );
  }

  @Override
  public <T> T inReadWriteDatasourceTransaction(
      String dataSource,
      SegmentMetadataTransaction.Callback<T> callback
  )
  {
    return connector.retryTransaction(
        (handle, status) -> {
          final SegmentMetadataTransaction sqlTransaction
              = createSqlTransaction(dataSource, handle, status);

          if (segmentMetadataCache.isEnabled()) {
            final boolean isSynced = segmentMetadataCache.isSyncedForRead();

            if (isSynced) {
              emitTransactionCount(Metric.READ_WRITE_TRANSACTIONS, dataSource);
            } else {
              log.warn(
                  "Starting read-write transaction for datasource[%s]. Reads will be done"
                  + " directly from metadata store since cache is not synced yet.",
                  dataSource
              );
              emitTransactionCount(Metric.WRITE_ONLY_TRANSACTIONS, dataSource);
            }

            return segmentMetadataCache.writeCacheForDataSource(dataSource, dataSourceCache -> {
              final SegmentMetadataTransaction cachedTransaction =
                  new CachedSegmentMetadataTransaction(sqlTransaction, dataSourceCache, leaderSelector, isSynced);
              return executeWriteAndClose(cachedTransaction, callback);
            });
          } else {
            return executeWriteAndClose(sqlTransaction, callback);
          }
        },
        QUIET_RETRIES,
        getMaxRetries()
    );
  }

  private SegmentMetadataTransaction createSqlTransaction(
      String dataSource,
      Handle handle,
      TransactionStatus transactionStatus
  )
  {
    return new SqlSegmentMetadataTransaction(
        dataSource,
        handle, transactionStatus, connector, tablesConfig, jsonMapper
    );
  }

  private <T> T executeWriteAndClose(
      SegmentMetadataTransaction transaction,
      SegmentMetadataTransaction.Callback<T> callback
  ) throws Exception
  {
    try {
      return callback.inTransaction(transaction);
    }
    catch (Throwable e) {
      transaction.setRollbackOnly();
      throw e;
    }
    finally {
      transaction.close();
    }
  }

  private <T> T executeReadAndClose(
      SegmentMetadataReadTransaction transaction,
      SegmentMetadataReadTransaction.Callback<T> callback
  ) throws Exception
  {
    try (transaction) {
      return callback.inTransaction(transaction);
    }
  }

  private void emitTransactionCount(String metricName, String datasource)
  {
    emitter.emit(
        ServiceMetricEvent.builder()
                          .setDimension(DruidMetrics.DATASOURCE, datasource)
                          .setMetric(metricName, 1L)
    );
  }

}
