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

package io.druid.storage.jdbc.postgresql;

import com.fasterxml.jackson.databind.Module;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.Provides;
import io.druid.db.IndexerSQLMetadataCoordinator;
import io.druid.db.MetadataRuleManager;
import io.druid.db.MetadataRuleManagerProvider;
import io.druid.db.MetadataSegmentManager;
import io.druid.db.MetadataSegmentManagerProvider;
import io.druid.db.MetadataDbConnector;
import io.druid.db.MetadataSegmentPublisherProvider;
import io.druid.db.DerbyConnector;
import io.druid.db.SQLMetadataRuleManager;
import io.druid.db.SQLMetadataRuleManagerProvider;
import io.druid.db.SQLMetadataSegmentManager;
import io.druid.db.SQLMetadataSegmentManagerProvider;
import io.druid.db.SQLMetadataSegmentPublisher;
import io.druid.guice.LazySingleton;
import io.druid.guice.PolyBind;
import io.druid.indexing.overlord.IndexerMetadataCoordinator;
import io.druid.initialization.DruidModule;
import io.druid.segment.realtime.DbSegmentPublisher;
import io.druid.db.SQLMetadataSegmentPublisherProvider;
import org.skife.jdbi.v2.IDBI;

import java.util.List;

public class PostgreSQLStorageDruidModule implements DruidModule
{
  @Override
  public List<? extends Module> getJacksonModules()
  {
    return ImmutableList.of();
  }

  @Override
  public void configure(Binder binder)
  {
    bindDataBasePostgreSQL(binder);

    // TODO: Change default to Derby
    PolyBind.createChoice(
        binder, "druid.db.type", Key.get(MetadataDbConnector.class), Key.get(DerbyConnector.class)
    );
  }

  private static void bindDataBasePostgreSQL(Binder binder)
  {
    PolyBind.optionBinder(binder, Key.get(MetadataDbConnector.class))
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

    PolyBind.optionBinder(binder, Key.get(DbSegmentPublisher.class))
            .addBinding("postgresql")
            .to(SQLMetadataSegmentPublisher.class)
            .in(LazySingleton.class);

    // TODO: Bind DbActionHandler

    PolyBind.optionBinder(binder, Key.get(MetadataSegmentPublisherProvider.class))
            .addBinding("postgresql")
            .to(SQLMetadataSegmentPublisherProvider.class)
            .in(LazySingleton.class);

    // TODO: Bind DbUpdaterJobHandler

    PolyBind.optionBinder(binder, Key.get(IndexerMetadataCoordinator.class))
            .addBinding("postgresql")
            .to(IndexerSQLMetadataCoordinator.class)
            .in(LazySingleton.class);
  }

  @Provides
  @LazySingleton
  public IDBI getDbi(final PostgreSQLConnector dbConnector)
  {
    return dbConnector.getDBI();
  }
}
