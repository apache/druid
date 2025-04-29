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

package org.apache.druid.testing.cluster.overlord;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import org.apache.druid.java.util.common.lifecycle.LifecycleStart;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.metadata.IndexerSQLMetadataStorageCoordinator;
import org.apache.druid.metadata.MetadataStorageTablesConfig;
import org.apache.druid.metadata.SQLMetadataConnector;
import org.apache.druid.metadata.segment.SegmentMetadataTransactionFactory;
import org.apache.druid.segment.metadata.CentralizedDatasourceSchemaConfig;
import org.apache.druid.segment.metadata.SegmentSchemaManager;

public class FaultyMetadataStorageCoordinator extends IndexerSQLMetadataStorageCoordinator
{
  private static final Logger log = new Logger(FaultyMetadataStorageCoordinator.class);

  @Inject
  public FaultyMetadataStorageCoordinator(
      SegmentMetadataTransactionFactory transactionFactory,
      ObjectMapper jsonMapper,
      MetadataStorageTablesConfig dbTables,
      SQLMetadataConnector connector,
      SegmentSchemaManager segmentSchemaManager,
      CentralizedDatasourceSchemaConfig centralizedDatasourceSchemaConfig
  )
  {
    super(
        transactionFactory,
        jsonMapper,
        dbTables,
        connector,
        segmentSchemaManager,
        centralizedDatasourceSchemaConfig
    );
    log.info("Creating a faulty metadata storage coordinator");
  }

  @Override
  @LifecycleStart
  public void start()
  {
    log.info("Starting the faulty metadata storage coordinator");
    super.start();
  }

  @Override
  public int deletePendingSegments(String dataSource)
  {
    return super.deletePendingSegments(dataSource);
  }

  @Override
  public int deletePendingSegmentsForTaskAllocatorId(String datasource, String taskAllocatorId)
  {
    log.info("Deleting pending segments with a bit of faulty behaviour");
    return super.deletePendingSegmentsForTaskAllocatorId(datasource, taskAllocatorId);
  }
}
