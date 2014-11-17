/*
 * Druid - a distributed column store.
 * Copyright (C) 2014  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.guice;

import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.Module;
import io.druid.indexer.MetadataStorageUpdaterJobHandler;
import io.druid.indexer.SQLMetadataStorageUpdaterJobHandler;
import io.druid.indexing.overlord.IndexerMetadataStorageCoordinator;
import io.druid.metadata.MetadataStorageActionHandlerFactory;
import io.druid.metadata.IndexerSQLMetadataStorageCoordinator;
import io.druid.metadata.MetadataRuleManager;
import io.druid.metadata.MetadataRuleManagerProvider;
import io.druid.metadata.MetadataSegmentManager;
import io.druid.metadata.MetadataSegmentManagerProvider;
import io.druid.metadata.MetadataSegmentPublisher;
import io.druid.metadata.MetadataSegmentPublisherProvider;
import io.druid.metadata.MetadataStorageConnector;
import io.druid.metadata.MetadataStorageProvider;
import io.druid.metadata.NoopMetadataStorageProvider;
import io.druid.metadata.SQLMetadataConnector;
import io.druid.metadata.SQLMetadataRuleManager;
import io.druid.metadata.SQLMetadataRuleManagerProvider;
import io.druid.metadata.SQLMetadataSegmentManager;
import io.druid.metadata.SQLMetadataSegmentManagerProvider;
import io.druid.metadata.SQLMetadataSegmentPublisher;
import io.druid.metadata.SQLMetadataSegmentPublisherProvider;
import io.druid.metadata.SQLMetadataStorageActionHandlerFactory;

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
   */
  public void createBindingChoices(Binder binder, String defaultPropertyValue)
  {
    PolyBind.createChoiceWithDefault(
        binder, PROPERTY, Key.get(MetadataStorageConnector.class), null, defaultPropertyValue
    );
    PolyBind.createChoiceWithDefault(
        binder,
        PROPERTY,
        Key.get(MetadataStorageProvider.class),
        Key.get(NoopMetadataStorageProvider.class),
        defaultPropertyValue
    );
    PolyBind.createChoiceWithDefault(
        binder, PROPERTY, Key.get(SQLMetadataConnector.class), null, defaultPropertyValue
    );
    PolyBind.createChoiceWithDefault(
        binder,
        PROPERTY,
        Key.get(MetadataSegmentManager.class),
        Key.get(SQLMetadataSegmentManager.class),
        defaultPropertyValue
    );
    PolyBind.createChoiceWithDefault(
        binder,
        PROPERTY,
        Key.get(MetadataSegmentManagerProvider.class),
        Key.get(SQLMetadataSegmentManagerProvider.class),
        defaultPropertyValue
    );
    PolyBind.createChoiceWithDefault(
        binder,
        PROPERTY,
        Key.get(MetadataRuleManager.class),
        Key.get(SQLMetadataRuleManager.class),
        defaultPropertyValue
    );
    PolyBind.createChoiceWithDefault(
        binder,
        PROPERTY,
        Key.get(MetadataRuleManagerProvider.class),
        Key.get(SQLMetadataRuleManagerProvider.class),
        defaultPropertyValue
    );
    PolyBind.createChoiceWithDefault(
        binder,
        PROPERTY,
        Key.get(MetadataSegmentPublisher.class),
        Key.get(SQLMetadataSegmentPublisher.class),
        defaultPropertyValue
    );
    PolyBind.createChoiceWithDefault(
        binder,
        PROPERTY,
        Key.get(MetadataSegmentPublisherProvider.class),
        Key.get(SQLMetadataSegmentPublisherProvider.class),
        defaultPropertyValue
    );
    PolyBind.createChoiceWithDefault(
        binder,
        PROPERTY,
        Key.get(IndexerMetadataStorageCoordinator.class),
        Key.get(IndexerSQLMetadataStorageCoordinator.class),
        defaultPropertyValue
    );
    PolyBind.createChoiceWithDefault(
        binder,
        PROPERTY,
        Key.get(MetadataStorageActionHandlerFactory.class),
        Key.get(SQLMetadataStorageActionHandlerFactory.class),
        defaultPropertyValue
    );
    PolyBind.createChoiceWithDefault(
        binder,
        PROPERTY,
        Key.get(MetadataStorageUpdaterJobHandler.class),
        Key.get(SQLMetadataStorageUpdaterJobHandler.class),
        defaultPropertyValue
    );
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
            .in(LazySingleton.class);

    PolyBind.optionBinder(binder, Key.get(MetadataStorageUpdaterJobHandler.class))
            .addBinding(type)
            .to(SQLMetadataStorageUpdaterJobHandler.class)
            .in(LazySingleton.class);
  }
}
