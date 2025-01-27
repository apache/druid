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
import org.apache.druid.discovery.DruidLeaderSelector;
import org.apache.druid.metadata.MetadataStorageTablesConfig;
import org.apache.druid.metadata.SQLMetadataConnector;
import org.apache.druid.metadata.segment.cache.SegmentsMetadataCache;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.TransactionStatus;

/**
 * Factory for {@link SegmentsMetadataTransaction}s. If the
 * {@link SegmentsMetadataCache} is enabled and ready, the transaction may
 * read/write from the cache as applicable.
 * <p>
 * This class serves as a wrapper over the {@link SQLMetadataConnector} to
 * perform transactions specific to segment metadata.
 */
public class SqlSegmentsMetadataTransactionFactory
{
  private static final int QUIET_RETRIES = 3;
  private static final int MAX_RETRIES = 10;

  private final ObjectMapper jsonMapper;
  private final MetadataStorageTablesConfig tablesConfig;
  private final SQLMetadataConnector connector;
  private final DruidLeaderSelector leaderSelector;
  private final SegmentsMetadataCache segmentsMetadataCache;

  @Inject
  public SqlSegmentsMetadataTransactionFactory(
      ObjectMapper jsonMapper,
      MetadataStorageTablesConfig tablesConfig,
      SQLMetadataConnector connector,
      DruidLeaderSelector leaderSelector,
      SegmentsMetadataCache segmentsMetadataCache
  )
  {
    this.jsonMapper = jsonMapper;
    this.tablesConfig = tablesConfig;
    this.connector = connector;
    this.leaderSelector = leaderSelector;
    this.segmentsMetadataCache = segmentsMetadataCache;
  }

  public int getMaxRetries()
  {
    return MAX_RETRIES;
  }

  public <T> T inReadOnlyDatasourceTransaction(
      String dataSource,
      SegmentsMetadataReadTransaction.Callback<T> callback
  )
  {
    return connector.inReadOnlyTransaction((handle, status) -> {
      final SegmentsMetadataTransaction sqlTransaction
          = createSqlTransaction(dataSource, handle, status);

      if (segmentsMetadataCache.isReady()) {
        final SegmentsMetadataCache.DataSource datasourceCache
            = segmentsMetadataCache.getDatasource(dataSource);
        final SegmentsMetadataReadTransaction cachedTransaction
            = new SqlSegmentsMetadataCachedTransaction(sqlTransaction, datasourceCache, leaderSelector);

        return datasourceCache.withReadLock(dc -> executeRead(cachedTransaction, callback));
      } else {
        return executeRead(createSqlTransaction(dataSource, handle, status), callback);
      }
    });
  }

  public <T> T retryDatasourceTransaction(
      String dataSource,
      SegmentsMetadataTransaction.Callback<T> callback
  )
  {
    return connector.retryTransaction(
        (handle, status) -> {
          final SegmentsMetadataTransaction sqlTransaction
              = createSqlTransaction(dataSource, handle, status);

          if (segmentsMetadataCache.isReady()) {
            final SegmentsMetadataCache.DataSource datasourceCache
                = segmentsMetadataCache.getDatasource(dataSource);
            final SegmentsMetadataTransaction cachedTransaction
                = new SqlSegmentsMetadataCachedTransaction(sqlTransaction, datasourceCache, leaderSelector);

            return datasourceCache.withWriteLock(dc -> executeWrite(cachedTransaction, callback));
          } else {
            return executeWrite(sqlTransaction, callback);
          }
        },
        QUIET_RETRIES,
        getMaxRetries()
    );
  }

  private SegmentsMetadataTransaction createSqlTransaction(
      String dataSource,
      Handle handle,
      TransactionStatus transactionStatus
  )
  {
    return new SqlSegmentsMetadataTransaction(
        dataSource,
        handle,
        transactionStatus,
        connector,
        tablesConfig,
        jsonMapper
    );
  }

  private <T> T executeWrite(
      SegmentsMetadataTransaction transaction,
      SegmentsMetadataTransaction.Callback<T> callback
  ) throws Exception
  {
    try {
      return callback.inTransaction(transaction);
    }
    catch (Exception e) {
      transaction.setRollbackOnly();
      throw e;
    }
    finally {
      transaction.close();
    }
  }

  private <T> T executeRead(
      SegmentsMetadataReadTransaction transaction,
      SegmentsMetadataReadTransaction.Callback<T> callback
  ) throws Exception
  {
    try (transaction) {
      return callback.inTransaction(transaction);
    }
  }

}
