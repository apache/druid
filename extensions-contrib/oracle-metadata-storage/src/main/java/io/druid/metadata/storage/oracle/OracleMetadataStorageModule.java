/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.metadata.storage.oracle;

import com.fasterxml.jackson.databind.Module;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Key;
import io.druid.audit.AuditManager;
import io.druid.guice.JsonConfigProvider;
import io.druid.guice.LazySingleton;
import io.druid.guice.ManageLifecycle;
import io.druid.guice.PolyBind;
import io.druid.guice.SQLMetadataStorageDruidModule;
import io.druid.indexer.MetadataStorageUpdaterJobHandler;
import io.druid.indexer.OracleMetadataStorageUpdaterJobHandler;
import io.druid.indexing.overlord.IndexerMetadataStorageCoordinator;
import io.druid.initialization.DruidModule;
import io.druid.metadata.IndexerOracleMetadataStorageCoordinator;
import io.druid.metadata.MetadataRuleManager;
import io.druid.metadata.MetadataRuleManagerProvider;
import io.druid.metadata.MetadataSegmentManager;
import io.druid.metadata.MetadataSegmentManagerProvider;
import io.druid.metadata.MetadataSegmentPublisher;
import io.druid.metadata.MetadataSegmentPublisherProvider;
import io.druid.metadata.MetadataStorageActionHandlerFactory;
import io.druid.metadata.MetadataStorageConnector;
import io.druid.metadata.MetadataStorageProvider;
import io.druid.metadata.MetadataSupervisorManager;
import io.druid.metadata.NoopMetadataStorageProvider;
import io.druid.metadata.OracleMetadataConnector;
import io.druid.metadata.OracleMetadataRuleManager;
import io.druid.metadata.OracleMetadataRuleManagerProvider;
import io.druid.metadata.OracleMetadataSegmentManager;
import io.druid.metadata.OracleMetadataSegmentManagerProvider;
import io.druid.metadata.OracleMetadataSegmentPublisher;
import io.druid.metadata.OracleMetadataSegmentPublisherProvider;
import io.druid.metadata.OracleMetadataStorageActionHandlerFactory;
import io.druid.metadata.OracleMetadataSupervisorManager;
import io.druid.metadata.SQLMetadataConnector;
import io.druid.server.audit.AuditManagerProvider;
import io.druid.server.audit.OracleAuditManager;
import io.druid.server.audit.OracleAuditManagerProvider;
import io.druid.server.audit.SQLAuditManagerConfig;

import java.util.List;

public class OracleMetadataStorageModule extends SQLMetadataStorageDruidModule implements DruidModule
{

  public static final String type = "oracle";

  public OracleMetadataStorageModule()
  {
    super(type);
  }

  @Override
  public void configure(Binder binder)
  {

    PolyBind.optionBinder(binder, Key.get(MetadataRuleManager.class))
            .addBinding(type)
            .to(OracleMetadataRuleManager.class)
            .in(LazySingleton.class);

    PolyBind.optionBinder(binder, Key.get(MetadataRuleManagerProvider.class))
            .addBinding(type)
            .to(OracleMetadataRuleManagerProvider.class)
            .in(LazySingleton.class);

    PolyBind.optionBinder(binder, Key.get(MetadataSegmentPublisher.class))
            .addBinding(type)
            .to(OracleMetadataSegmentPublisher.class)
            .in(LazySingleton.class);

    PolyBind.optionBinder(binder, Key.get(MetadataSegmentPublisherProvider.class))
            .addBinding(type)
            .to(OracleMetadataSegmentPublisherProvider.class)
            .in(LazySingleton.class);

    PolyBind.optionBinder(binder, Key.get(MetadataStorageActionHandlerFactory.class))
            .addBinding(type)
            .to(OracleMetadataStorageActionHandlerFactory.class)
            .in(LazySingleton.class);

    PolyBind.optionBinder(binder, Key.get(IndexerMetadataStorageCoordinator.class))
            .addBinding(type)
            .to(IndexerOracleMetadataStorageCoordinator.class)
            .in(ManageLifecycle.class);

    PolyBind.optionBinder(binder, Key.get(MetadataStorageUpdaterJobHandler.class))
            .addBinding(type)
            .to(OracleMetadataStorageUpdaterJobHandler.class)
            .in(LazySingleton.class);

    JsonConfigProvider.bind(binder, "druid.audit.manager", SQLAuditManagerConfig.class);

    PolyBind.optionBinder(binder, Key.get(AuditManager.class))
            .addBinding(type)
            .to(OracleAuditManager.class)
            .in(LazySingleton.class);

    PolyBind.optionBinder(binder, Key.get(AuditManagerProvider.class))
            .addBinding(type)
            .to(OracleAuditManagerProvider.class)
            .in(LazySingleton.class);

    PolyBind.optionBinder(binder, Key.get(MetadataSupervisorManager.class))
            .addBinding(type)
            .to(OracleMetadataSupervisorManager.class)
            .in(LazySingleton.class);

    PolyBind.optionBinder(binder, Key.get(MetadataStorageProvider.class))
            .addBinding(type)
            .to(NoopMetadataStorageProvider.class)
            .in(LazySingleton.class);

    PolyBind.optionBinder(binder, Key.get(MetadataStorageConnector.class))
            .addBinding(type)
            .to(OracleMetadataConnector.class)
            .in(LazySingleton.class);

    PolyBind.optionBinder(binder, Key.get(SQLMetadataConnector.class))
            .addBinding(type)
            .to(OracleMetadataConnector.class)
            .in(LazySingleton.class);

    PolyBind.optionBinder(binder, Key.get(MetadataSegmentManager.class))
            .addBinding(type)
            .to(OracleMetadataSegmentManager.class)
            .in(LazySingleton.class);

    PolyBind.optionBinder(binder, Key.get(MetadataSegmentManagerProvider.class))
            .addBinding(type)
            .to(OracleMetadataSegmentManagerProvider.class)
            .in(LazySingleton.class);
  }

  @Override
  public List<? extends Module> getJacksonModules()
  {
    return ImmutableList.of();
  }
}
