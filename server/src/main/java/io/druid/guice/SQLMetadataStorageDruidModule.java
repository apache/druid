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

package io.druid.guice;

import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.Module;
import io.druid.audit.AuditManager;
import io.druid.indexer.MetadataStorageUpdaterJobHandler;
import io.druid.indexer.SQLMetadataStorageUpdaterJobHandler;
import io.druid.indexing.overlord.IndexerMetadataStorageCoordinator;
import io.druid.metadata.IndexerSQLMetadataStorageCoordinator;
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
import io.druid.metadata.SQLMetadataConnector;
import io.druid.metadata.SQLMetadataRuleManager;
import io.druid.metadata.SQLMetadataRuleManagerProvider;
import io.druid.metadata.SQLMetadataSegmentManager;
import io.druid.metadata.SQLMetadataSegmentManagerProvider;
import io.druid.metadata.SQLMetadataSegmentPublisher;
import io.druid.metadata.SQLMetadataSegmentPublisherProvider;
import io.druid.metadata.SQLMetadataStorageActionHandlerFactory;
import io.druid.metadata.SQLMetadataSupervisorManager;
import io.druid.server.audit.AuditManagerProvider;
import io.druid.server.audit.SQLAuditManager;
import io.druid.server.audit.SQLAuditManagerConfig;
import io.druid.server.audit.SQLAuditManagerProvider;

public class SQLMetadataStorageDruidModule implements Module
{
  public static final String PROPERTY = "druid.metadata.storage.type";
  final String type;

  public SQLMetadataStorageDruidModule(String type)
  {
    this.type = type;
  }

  /**
   * This function only needs to be called by the default SQL metadata storage module
   * Other modules should default to calling super.configure(...) alone
   *
   * @param defaultValue default property value
   */
  public void createBindingChoices(Binder binder, String defaultValue)
  {
    String prop = PROPERTY;
    PolyBind.createChoiceWithDefault(binder, prop, Key.get(MetadataStorageConnector.class), defaultValue);
    PolyBind.createChoiceWithDefault(binder, prop, Key.get(MetadataStorageProvider.class), defaultValue);
    PolyBind.createChoiceWithDefault(binder, prop, Key.get(SQLMetadataConnector.class), defaultValue);

    PolyBind.createChoiceWithDefault(binder, prop, Key.get(MetadataSegmentManager.class), defaultValue);
    PolyBind.createChoiceWithDefault(binder, prop, Key.get(MetadataSegmentManagerProvider.class), defaultValue);
    PolyBind.createChoiceWithDefault(binder, prop, Key.get(MetadataRuleManager.class), defaultValue);
    PolyBind.createChoiceWithDefault(binder, prop, Key.get(MetadataRuleManagerProvider.class), defaultValue);
    PolyBind.createChoiceWithDefault(binder, prop, Key.get(MetadataSegmentPublisher.class), defaultValue);
    PolyBind.createChoiceWithDefault(binder, prop, Key.get(MetadataSegmentPublisherProvider.class), defaultValue);
    PolyBind.createChoiceWithDefault(binder, prop, Key.get(IndexerMetadataStorageCoordinator.class), defaultValue);
    PolyBind.createChoiceWithDefault(binder, prop, Key.get(MetadataStorageActionHandlerFactory.class), defaultValue);
    PolyBind.createChoiceWithDefault(binder, prop, Key.get(MetadataStorageUpdaterJobHandler.class), defaultValue);
    PolyBind.createChoiceWithDefault(binder, prop, Key.get(AuditManager.class), defaultValue);
    PolyBind.createChoiceWithDefault(binder, prop, Key.get(AuditManagerProvider.class), defaultValue);
    PolyBind.createChoiceWithDefault(binder, prop, Key.get(MetadataSupervisorManager.class), defaultValue);
  }

  @Override
  public void configure(Binder binder)
  {
    PolyBind.optionBinder(binder, Key.get(MetadataSegmentManager.class))
            .addBinding(type)
            .to(SQLMetadataSegmentManager.class)
            .in(LazySingleton.class);

    PolyBind.optionBinder(binder, Key.get(MetadataSegmentManagerProvider.class))
            .addBinding(type)
            .to(SQLMetadataSegmentManagerProvider.class)
            .in(LazySingleton.class);

    PolyBind.optionBinder(binder, Key.get(MetadataRuleManager.class))
            .addBinding(type)
            .to(SQLMetadataRuleManager.class)
            .in(LazySingleton.class);

    PolyBind.optionBinder(binder, Key.get(MetadataRuleManagerProvider.class))
            .addBinding(type)
            .to(SQLMetadataRuleManagerProvider.class)
            .in(LazySingleton.class);

    PolyBind.optionBinder(binder, Key.get(MetadataSegmentPublisher.class))
            .addBinding(type)
            .to(SQLMetadataSegmentPublisher.class)
            .in(LazySingleton.class);

    PolyBind.optionBinder(binder, Key.get(MetadataSegmentPublisherProvider.class))
            .addBinding(type)
            .to(SQLMetadataSegmentPublisherProvider.class)
            .in(LazySingleton.class);

    PolyBind.optionBinder(binder, Key.get(MetadataStorageActionHandlerFactory.class))
            .addBinding(type)
            .to(SQLMetadataStorageActionHandlerFactory.class)
            .in(LazySingleton.class);

    PolyBind.optionBinder(binder, Key.get(IndexerMetadataStorageCoordinator.class))
            .addBinding(type)
            .to(IndexerSQLMetadataStorageCoordinator.class)
            .in(ManageLifecycle.class);

    PolyBind.optionBinder(binder, Key.get(MetadataStorageUpdaterJobHandler.class))
            .addBinding(type)
            .to(SQLMetadataStorageUpdaterJobHandler.class)
            .in(LazySingleton.class);

    JsonConfigProvider.bind(binder, "druid.audit.manager", SQLAuditManagerConfig.class);

    PolyBind.optionBinder(binder, Key.get(AuditManager.class))
            .addBinding(type)
            .to(SQLAuditManager.class)
            .in(LazySingleton.class);

    PolyBind.optionBinder(binder, Key.get(AuditManagerProvider.class))
            .addBinding(type)
            .to(SQLAuditManagerProvider.class)
            .in(LazySingleton.class);

    PolyBind.optionBinder(binder, Key.get(MetadataSupervisorManager.class))
            .addBinding(type)
            .to(SQLMetadataSupervisorManager.class)
            .in(LazySingleton.class);
  }
}
