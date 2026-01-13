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

package org.apache.druid.guice;

import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Module;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.guice.annotations.Self;
import org.apache.druid.indexing.overlord.IndexerMetadataStorageCoordinator;
import org.apache.druid.metadata.IndexerSQLMetadataStorageCoordinator;
import org.apache.druid.metadata.MetadataRuleManager;
import org.apache.druid.metadata.MetadataRuleManagerConfig;
import org.apache.druid.metadata.MetadataRuleManagerProvider;
import org.apache.druid.metadata.MetadataSupervisorManager;
import org.apache.druid.metadata.SQLMetadataRuleManagerProvider;
import org.apache.druid.metadata.SQLMetadataSupervisorManager;
import org.apache.druid.metadata.SegmentsMetadataManager;
import org.apache.druid.metadata.SegmentsMetadataManagerConfig;
import org.apache.druid.metadata.SegmentsMetadataManagerProvider;
import org.apache.druid.metadata.SqlSegmentsMetadataManagerProvider;
import org.apache.druid.metadata.segment.SegmentMetadataTransactionFactory;
import org.apache.druid.metadata.segment.SqlSegmentMetadataReadOnlyTransactionFactory;
import org.apache.druid.metadata.segment.SqlSegmentMetadataTransactionFactory;
import org.apache.druid.metadata.segment.cache.HeapMemorySegmentMetadataCache;
import org.apache.druid.metadata.segment.cache.SegmentMetadataCache;
import org.apache.druid.segment.metadata.CompactionStateCache;
import org.apache.druid.segment.metadata.CompactionStateStorage;
import org.apache.druid.segment.metadata.NoopSegmentSchemaCache;
import org.apache.druid.segment.metadata.SegmentSchemaCache;
import org.apache.druid.segment.metadata.SqlCompactionStateStorage;
import org.apache.druid.server.coordinator.CoordinatorConfigManager;
import org.apache.druid.server.coordinator.MetadataManager;

import java.util.Properties;
import java.util.Set;

/**
 * Module used by Overlord and Coordinator to bind the following metadata managers:
 * <ul>
 * <li>{@link MetadataManager} - Coordinator only</li>
 * <li>{@link MetadataRuleManager} - Coordinator only</li>
 * <li>{@link MetadataSupervisorManager}</li>
 * <li>{@link SegmentsMetadataManager}</li>
 * <li>{@link IndexerMetadataStorageCoordinator}</li>
 * <li>{@link CoordinatorConfigManager}</li>
 * <li>{@link SegmentMetadataCache}</li>
 * <li>{@link CompactionStateCache} - Overlord only</li>
 * <li>{@link SegmentSchemaCache} - Coordinator only</li>
 * <li>{@link SqlCompactionStateStorage}</li>
 * </ul>
 */
public class MetadataManagerModule implements Module
{
  private Set<NodeRole> nodeRoles;
  private boolean isSchemaCacheEnabled;

  @Inject
  public void configure(
      Properties properties,
      @Self Set<NodeRole> nodeRoles
  )
  {
    this.nodeRoles = nodeRoles;
    this.isSchemaCacheEnabled = MetadataConfigModule.isSegmentSchemaCacheEnabled(properties);
  }

  @Override
  public void configure(Binder binder)
  {
    // Common dependencies
    binder.bind(CoordinatorConfigManager.class).in(LazySingleton.class);

    binder.bind(MetadataSupervisorManager.class)
          .to(SQLMetadataSupervisorManager.class)
          .in(LazySingleton.class);

    JsonConfigProvider.bind(binder, "druid.manager.segments", SegmentsMetadataManagerConfig.class);
    binder.bind(SegmentsMetadataManagerProvider.class)
          .to(SqlSegmentsMetadataManagerProvider.class)
          .in(LazySingleton.class);
    binder.bind(SegmentsMetadataManager.class)
          .toProvider(SegmentsMetadataManagerProvider.class)
          .in(ManageLifecycle.class);

    binder.bind(IndexerMetadataStorageCoordinator.class)
          .to(IndexerSQLMetadataStorageCoordinator.class)
          .in(ManageLifecycle.class);
    binder.bind(SegmentMetadataCache.class)
          .to(HeapMemorySegmentMetadataCache.class)
          .in(LazySingleton.class);

    // Coordinator-only dependencies
    if (nodeRoles.contains(NodeRole.COORDINATOR)) {
      JsonConfigProvider.bind(binder, "druid.manager.rules", MetadataRuleManagerConfig.class);
      binder.bind(MetadataRuleManagerProvider.class)
            .to(SQLMetadataRuleManagerProvider.class)
            .in(LazySingleton.class);
      binder.bind(MetadataRuleManager.class)
            .toProvider(MetadataRuleManagerProvider.class)
            .in(ManageLifecycle.class);

      binder.bind(MetadataManager.class).in(LazySingleton.class);
    }

    if (nodeRoles.contains(NodeRole.COORDINATOR) && isSchemaCacheEnabled) {
      binder.bind(SegmentSchemaCache.class).in(LazySingleton.class);
    } else {
      binder.bind(SegmentSchemaCache.class)
            .to(NoopSegmentSchemaCache.class)
            .in(LazySingleton.class);
    }

    // Overlord-only dependencies
    if (nodeRoles.contains(NodeRole.OVERLORD)) {
      binder.bind(SegmentMetadataTransactionFactory.class)
            .to(SqlSegmentMetadataTransactionFactory.class)
            .in(LazySingleton.class);
      binder.bind(CompactionStateCache.class).in(LazySingleton.class);
      binder.bind(CompactionStateStorage.class)
            .to(SqlCompactionStateStorage.class)
            .in(ManageLifecycle.class);
    } else {
      binder.bind(SegmentMetadataTransactionFactory.class)
            .to(SqlSegmentMetadataReadOnlyTransactionFactory.class)
            .in(LazySingleton.class);
    }
  }
}
