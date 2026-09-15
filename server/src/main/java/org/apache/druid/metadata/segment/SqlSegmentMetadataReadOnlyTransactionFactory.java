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
import org.apache.druid.error.DruidException;
import org.apache.druid.metadata.MetadataStorageTablesConfig;
import org.apache.druid.metadata.SQLMetadataConnector;
import org.apache.druid.metadata.SegmentsMetadataManagerConfig;
import org.apache.druid.metadata.SqlSegmentsMetadataQuery;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.TransactionStatus;

import java.util.function.Function;

/**
 * Factory for read-only {@link SegmentMetadataTransaction}s that always read
 * directly from the metadata store and never from the {@code SegmentMetadataCache}.
 *
 * @see SqlSegmentMetadataTransactionFactory
 */
public class SqlSegmentMetadataReadOnlyTransactionFactory implements SegmentMetadataTransactionFactory
{
  private static final int QUIET_RETRIES = 2;
  private static final int MAX_RETRIES = 3;

  private final ObjectMapper jsonMapper;
  private final MetadataStorageTablesConfig tablesConfig;
  private final SegmentsMetadataManagerConfig managerConfig;
  private final SQLMetadataConnector connector;

  @Inject
  public SqlSegmentMetadataReadOnlyTransactionFactory(
      ObjectMapper jsonMapper,
      MetadataStorageTablesConfig tablesConfig,
      SegmentsMetadataManagerConfig managerConfig,
      SQLMetadataConnector connector
  )
  {
    this.jsonMapper = jsonMapper;
    this.tablesConfig = tablesConfig;
    this.managerConfig = managerConfig;
    this.connector = connector;
  }

  public int getMaxRetries()
  {
    return MAX_RETRIES;
  }

  public int getQuietRetries()
  {
    return QUIET_RETRIES;
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
          return executeReadAndClose(sqlTransaction, callback);
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
    throw DruidException.defensive("Only Overlord can perform write transactions on segment metadata.");
  }

  @Override
  public <T> T inReadOnlyNoCacheTransaction(Function<SqlSegmentsMetadataQuery, T> sqlQuery)
  {
    return connector.retryReadOnlyTransaction(
        (handle, status) -> sqlQuery.apply(createSqlQueryForTransaction(handle)),
        getQuietRetries(),
        getMaxRetries()
    );
  }

  @Override
  public <T> T inReadWriteNoCacheTransaction(Function<SqlSegmentsMetadataQuery, T> sqlUpdate)
  {
    throw DruidException.defensive("Only Overlord can perform write transactions on segment metadata.");
  }

  protected SegmentMetadataTransaction createSqlTransaction(
      String dataSource,
      Handle handle,
      TransactionStatus transactionStatus
  )
  {
    return new SqlSegmentMetadataTransaction(
        dataSource,
        handle,
        createSqlQueryForTransaction(handle),
        transactionStatus, connector, tablesConfig, jsonMapper
    );
  }

  protected <T> T executeReadAndClose(
      SegmentMetadataReadTransaction transaction,
      SegmentMetadataReadTransaction.Callback<T> callback
  ) throws Exception
  {
    try (transaction) {
      return callback.inTransaction(transaction);
    }
  }

  protected SqlSegmentsMetadataQuery createSqlQueryForTransaction(Handle handle)
  {
    return SqlSegmentsMetadataQuery.forHandle(handle, connector, tablesConfig, managerConfig, jsonMapper);
  }
}
