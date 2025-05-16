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
import org.apache.druid.metadata.MetadataRuleManagerProvider;
import org.apache.druid.metadata.MetadataSupervisorManager;
import org.apache.druid.metadata.SQLMetadataRuleManagerProvider;
import org.apache.druid.metadata.SQLMetadataSupervisorManager;
import org.apache.druid.metadata.SegmentsMetadataManager;
import org.apache.druid.metadata.SegmentsMetadataManagerProvider;
import org.apache.druid.metadata.SqlSegmentsMetadataManagerProvider;
import org.apache.druid.metadata.segment.SegmentMetadataTransactionFactory;
import org.apache.druid.metadata.segment.SqlSegmentMetadataTransactionFactory;
import org.apache.druid.metadata.segment.cache.HeapMemorySegmentMetadataCache;
import org.apache.druid.metadata.segment.cache.SegmentMetadataCache;
import org.apache.druid.segment.metadata.NoopSegmentSchemaCache;
import org.apache.druid.segment.metadata.SegmentSchemaCache;
import org.apache.druid.server.coordinator.CoordinatorConfigManager;
import org.apache.druid.server.coordinator.MetadataManager;

import java.util.Set;

/**
 * Module used by Overlord and Coordinator to bind various metadata managers.
 */
public class MetadataManagerModule implements Module
{
  private Set<NodeRole> nodeRoles;

  @Inject
  public void configure(
      @Self Set<NodeRole> nodeRoles
  )
  {
    this.nodeRoles = nodeRoles;
  }

  @Override
  public void configure(Binder binder)
  {
    if (nodeRoles.contains(NodeRole.COORDINATOR)) {
      binder.bind(MetadataRuleManagerProvider.class)
            .to(SQLMetadataRuleManagerProvider.class)
            .in(LazySingleton.class);
      binder.bind(MetadataRuleManager.class)
            .toProvider(MetadataRuleManagerProvider.class)
            .in(ManageLifecycle.class);

      binder.bind(MetadataManager.class).in(LazySingleton.class);
    }

    binder.bind(CoordinatorConfigManager.class).in(LazySingleton.class);

    binder.bind(MetadataSupervisorManager.class)
          .to(SQLMetadataSupervisorManager.class)
          .in(LazySingleton.class);

    binder.bind(SegmentsMetadataManagerProvider.class)
          .to(SqlSegmentsMetadataManagerProvider.class)
          .in(LazySingleton.class);
    binder.bind(SegmentsMetadataManager.class)
          .toProvider(SegmentsMetadataManagerProvider.class)
          .in(ManageLifecycle.class);

    binder.bind(SegmentMetadataTransactionFactory.class)
          .to(SqlSegmentMetadataTransactionFactory.class)
          .in(LazySingleton.class);
    binder.bind(IndexerMetadataStorageCoordinator.class)
          .to(IndexerSQLMetadataStorageCoordinator.class)
          .in(ManageLifecycle.class);
    binder.bind(SegmentMetadataCache.class)
          .to(HeapMemorySegmentMetadataCache.class)
          .in(LazySingleton.class);

    if (nodeRoles.contains(NodeRole.COORDINATOR)) {
      binder.bind(SegmentSchemaCache.class).in(LazySingleton.class);
    } else {
      binder.bind(SegmentSchemaCache.class)
            .to(NoopSegmentSchemaCache.class)
            .in(LazySingleton.class);
    }
  }
}
