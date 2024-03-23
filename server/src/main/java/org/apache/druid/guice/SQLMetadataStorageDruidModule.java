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
import com.google.inject.Key;
import com.google.inject.Module;
import org.apache.druid.audit.AuditManager;
import org.apache.druid.indexer.MetadataStorageUpdaterJobHandler;
import org.apache.druid.indexer.SQLMetadataStorageUpdaterJobHandler;
import org.apache.druid.indexing.overlord.IndexerMetadataStorageCoordinator;
import org.apache.druid.metadata.IndexerSQLMetadataStorageCoordinator;
import org.apache.druid.metadata.MetadataRuleManager;
import org.apache.druid.metadata.MetadataRuleManagerProvider;
import org.apache.druid.metadata.MetadataSegmentPublisher;
import org.apache.druid.metadata.MetadataSegmentPublisherProvider;
import org.apache.druid.metadata.MetadataStorageActionHandlerFactory;
import org.apache.druid.metadata.MetadataStorageConnector;
import org.apache.druid.metadata.MetadataStorageProvider;
import org.apache.druid.metadata.MetadataSupervisorManager;
import org.apache.druid.metadata.SQLMetadataConnector;
import org.apache.druid.metadata.SQLMetadataRuleManager;
import org.apache.druid.metadata.SQLMetadataRuleManagerProvider;
import org.apache.druid.metadata.SQLMetadataSegmentPublisher;
import org.apache.druid.metadata.SQLMetadataSegmentPublisherProvider;
import org.apache.druid.metadata.SQLMetadataSupervisorManager;
import org.apache.druid.metadata.SegmentsMetadataManager;
import org.apache.druid.metadata.SegmentsMetadataManagerProvider;
import org.apache.druid.metadata.SqlSegmentsMetadataManager;
import org.apache.druid.metadata.SqlSegmentsMetadataManagerProvider;
import org.apache.druid.server.audit.AuditManagerConfig;
import org.apache.druid.server.audit.AuditSerdeHelper;
import org.apache.druid.server.audit.SQLAuditManager;

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

    PolyBind.createChoiceWithDefault(binder, prop, Key.get(SegmentsMetadataManager.class), defaultValue);
    PolyBind.createChoiceWithDefault(binder, prop, Key.get(SegmentsMetadataManagerProvider.class), defaultValue);
    PolyBind.createChoiceWithDefault(binder, prop, Key.get(MetadataRuleManager.class), defaultValue);
    PolyBind.createChoiceWithDefault(binder, prop, Key.get(MetadataRuleManagerProvider.class), defaultValue);
    PolyBind.createChoiceWithDefault(binder, prop, Key.get(MetadataSegmentPublisher.class), defaultValue);
    PolyBind.createChoiceWithDefault(binder, prop, Key.get(MetadataSegmentPublisherProvider.class), defaultValue);
    PolyBind.createChoiceWithDefault(binder, prop, Key.get(IndexerMetadataStorageCoordinator.class), defaultValue);
    PolyBind.createChoiceWithDefault(binder, prop, Key.get(MetadataStorageActionHandlerFactory.class), defaultValue);
    PolyBind.createChoiceWithDefault(binder, prop, Key.get(MetadataStorageUpdaterJobHandler.class), defaultValue);
    PolyBind.createChoiceWithDefault(binder, prop, Key.get(MetadataSupervisorManager.class), defaultValue);

    configureAuditManager(binder);
  }

  @Override
  public void configure(Binder binder)
  {
    PolyBind.optionBinder(binder, Key.get(SegmentsMetadataManager.class))
            .addBinding(type)
            .to(SqlSegmentsMetadataManager.class)
            .in(LazySingleton.class);

    PolyBind.optionBinder(binder, Key.get(SegmentsMetadataManagerProvider.class))
            .addBinding(type)
            .to(SqlSegmentsMetadataManagerProvider.class)
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

    PolyBind.optionBinder(binder, Key.get(IndexerMetadataStorageCoordinator.class))
            .addBinding(type)
            .to(IndexerSQLMetadataStorageCoordinator.class)
            .in(ManageLifecycle.class);

    PolyBind.optionBinder(binder, Key.get(MetadataStorageUpdaterJobHandler.class))
            .addBinding(type)
            .to(SQLMetadataStorageUpdaterJobHandler.class)
            .in(LazySingleton.class);

    PolyBind.optionBinder(binder, Key.get(MetadataSupervisorManager.class))
            .addBinding(type)
            .to(SQLMetadataSupervisorManager.class)
            .in(LazySingleton.class);
  }

  private void configureAuditManager(Binder binder)
  {
    JsonConfigProvider.bind(binder, "druid.audit.manager", AuditManagerConfig.class);

    PolyBind.createChoice(
        binder,
        "druid.audit.manager.type",
        Key.get(AuditManager.class),
        Key.get(SQLAuditManager.class)
    );
    PolyBind.optionBinder(binder, Key.get(AuditManager.class))
        .addBinding("sql")
        .to(SQLAuditManager.class)
        .in(LazySingleton.class);

    binder.bind(AuditSerdeHelper.class).in(LazySingleton.class);
  }
}
