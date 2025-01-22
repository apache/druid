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
 */
public class SqlSegmentsMetadataTransactionFactory
{
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

  public SegmentsMetadataTransaction createTransactionForDatasource(
      String dataSource,
      Handle handle,
      TransactionStatus transactionStatus
  )
  {
    final SegmentsMetadataTransaction metadataTransaction = new SqlSegmentsMetadataTransaction(
        dataSource,
        handle,
        transactionStatus,
        connector,
        tablesConfig,
        jsonMapper
    );

    return
        segmentsMetadataCache.isReady()
        ? new SqlSegmentsMetadataCachedTransaction(
            dataSource,
            metadataTransaction,
            segmentsMetadataCache,
            leaderSelector
        )
        : metadataTransaction;
  }

}
