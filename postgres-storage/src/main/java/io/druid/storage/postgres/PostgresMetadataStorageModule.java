/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
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

package io.druid.storage.postgres;

import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.Provides;
import io.druid.db.IndexerSQLMetadataStorageCoordinator;
import io.druid.db.MetadataRuleManager;
import io.druid.db.MetadataRuleManagerProvider;
import io.druid.db.MetadataSegmentManager;
import io.druid.db.MetadataSegmentManagerProvider;
import io.druid.db.MetadataSegmentPublisherProvider;
import io.druid.db.MetadataStorageConnector;
import io.druid.db.MetadataStorageConnectorConfig;
import io.druid.db.MetadataStorageTablesConfig;
import io.druid.db.SQLMetadataConnector;
import io.druid.db.SQLMetadataRuleManager;
import io.druid.db.SQLMetadataRuleManagerProvider;
import io.druid.db.SQLMetadataSegmentManager;
import io.druid.db.SQLMetadataSegmentManagerProvider;
import io.druid.db.SQLMetadataSegmentPublisher;
import io.druid.db.SQLMetadataSegmentPublisherProvider;
import io.druid.db.SQLMetadataStorageActionHandler;
import io.druid.db.SQLMetadataStorageActionHandlerFactory;
import io.druid.guice.JsonConfigProvider;
import io.druid.guice.LazySingleton;
import io.druid.guice.PolyBind;
import io.druid.indexer.MetadataStorageUpdaterJobHandler;
import io.druid.indexer.SQLMetadataStorageUpdaterJobHandler;
import io.druid.indexing.overlord.IndexerMetadataStorageCoordinator;
import io.druid.indexing.overlord.MetadataStorageActionHandler;
import io.druid.indexing.overlord.MetadataStorageActionHandlerFactory;
import io.druid.initialization.DruidModule;
import io.druid.segment.realtime.SegmentPublisher;
import org.skife.jdbi.v2.IDBI;

import java.util.List;

public class PostgresMetadataStorageModule implements DruidModule
{
  @Override
  public List<? extends com.fasterxml.jackson.databind.Module> getJacksonModules()
  {
    return ImmutableList.of();
  }

  @Override
  public void configure(Binder binder)
  {
    bindPostgres(binder);
    JsonConfigProvider.bind(binder, "druid.db.tables", MetadataStorageTablesConfig.class);
    JsonConfigProvider.bind(binder, "druid.db.connector", MetadataStorageConnectorConfig.class);
  }

  private void bindPostgres(Binder binder) {
    PolyBind.optionBinder(binder, Key.get(MetadataStorageConnector.class))
            .addBinding("postgresql")
            .to(PostgreSQLConnector.class)
            .in(LazySingleton.class);

    PolyBind.optionBinder(binder, Key.get(SQLMetadataConnector.class))
            .addBinding("postgresql")
            .to(PostgreSQLConnector.class)
            .in(LazySingleton.class);

    PolyBind.optionBinder(binder, Key.get(MetadataSegmentManager.class))
            .addBinding("postgresql")
            .to(SQLMetadataSegmentManager.class)
            .in(LazySingleton.class);

    PolyBind.optionBinder(binder, Key.get(MetadataSegmentManagerProvider.class))
            .addBinding("postgresql")
            .to(SQLMetadataSegmentManagerProvider.class)
            .in(LazySingleton.class);

    PolyBind.optionBinder(binder, Key.get(MetadataRuleManager.class))
            .addBinding("postgresql")
            .to(SQLMetadataRuleManager.class)
            .in(LazySingleton.class);

    PolyBind.optionBinder(binder, Key.get(MetadataRuleManagerProvider.class))
            .addBinding("postgresql")
            .to(SQLMetadataRuleManagerProvider.class)
            .in(LazySingleton.class);

    PolyBind.optionBinder(binder, Key.get(SegmentPublisher.class))
            .addBinding("postgresql")
            .to(SQLMetadataSegmentPublisher.class)
            .in(LazySingleton.class);

    PolyBind.optionBinder(binder, Key.get(MetadataStorageActionHandlerFactory.class))
            .addBinding("postgresql")
            .to(SQLMetadataStorageActionHandlerFactory.class)
            .in(LazySingleton.class);

    PolyBind.optionBinder(binder, Key.get(MetadataSegmentPublisherProvider.class))
            .addBinding("postgresql")
            .to(SQLMetadataSegmentPublisherProvider.class)
            .in(LazySingleton.class);

    PolyBind.optionBinder(binder, Key.get(MetadataStorageUpdaterJobHandler.class))
            .addBinding("postgresql")
            .to(SQLMetadataStorageUpdaterJobHandler.class)
            .in(LazySingleton.class);

    PolyBind.optionBinder(binder, Key.get(IndexerMetadataStorageCoordinator.class))
            .addBinding("postgresql")
            .to(IndexerSQLMetadataStorageCoordinator.class)
            .in(LazySingleton.class);
  }

  @Provides
  @LazySingleton
  public IDBI getDbi(final PostgreSQLConnector dbConnector)
  {
    return dbConnector.getDBI();
  }
}
