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

package io.druid.guice;

import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.Provides;
import io.druid.db.DerbyMetadataRuleManager;
import io.druid.db.DerbyMetadataRuleManagerProvider;
import io.druid.db.DerbyMetadataSegmentManager;
import io.druid.db.DerbyMetadataSegmentManagerProvider;
import io.druid.db.IndexerSQLMetadataCoordinator;
import io.druid.db.MetadataRuleManager;
import io.druid.db.MetadataSegmentManager;
import io.druid.db.MetadataSegmentManagerProvider;
import io.druid.db.MetadataDbConnector;
import io.druid.db.MetadataSegmentPublisherProvider;
import io.druid.db.MetadataRuleManagerProvider;
import io.druid.db.DerbyConnector;
import io.druid.db.SQLMetadataActionHandler;
import io.druid.db.SQLMetadataSegmentPublisher;
import io.druid.db.SQLMetadataSegmentPublisherProvider;
import io.druid.indexer.SQLMetadataUpdaterJobHandler;
import io.druid.indexer.MetadataUpdaterJobHandler;
import io.druid.indexing.overlord.IndexerMetadataCoordinator;
import io.druid.indexing.overlord.MetadataActionHandler;
import io.druid.segment.realtime.DbSegmentPublisher;
import org.skife.jdbi.v2.IDBI;

public class DerbyStorageDruidModule implements Module
{

  @Override
  public void configure(Binder binder)
  {
    bindDataBaseDerby(binder);
    PolyBind.createChoice(
        binder, "druid.db.type", Key.get(MetadataDbConnector.class), Key.get(DerbyConnector.class)
    );
    PolyBind.createChoice(
        binder, "druid.db.type", Key.get(MetadataSegmentManager.class), Key.get(DerbyMetadataSegmentManager.class)
    );
    PolyBind.createChoice(
        binder, "druid.db.type", Key.get(MetadataSegmentManagerProvider.class), Key.get(DerbyMetadataSegmentManagerProvider.class)
    );
    PolyBind.createChoice(
        binder, "druid.db.type", Key.get(MetadataRuleManager.class), Key.get(DerbyMetadataRuleManager.class)
    );
    PolyBind.createChoice(
        binder, "druid.db.type", Key.get(MetadataRuleManagerProvider.class), Key.get(DerbyMetadataRuleManagerProvider.class)
    );
    PolyBind.createChoice(
        binder, "druid.db.type", Key.get(DbSegmentPublisher.class), Key.get(SQLMetadataSegmentPublisher.class)
    );
    PolyBind.createChoice(
        binder, "druid.db.type", Key.get(MetadataSegmentPublisherProvider.class), Key.get(SQLMetadataSegmentPublisherProvider.class)
    );
    PolyBind.createChoice(
        binder, "druid.db.type", Key.get(IndexerMetadataCoordinator.class), Key.get(IndexerSQLMetadataCoordinator.class)
    );
    PolyBind.createChoice(
        binder, "druid.db.type", Key.get(MetadataActionHandler.class), Key.get(SQLMetadataActionHandler.class)
    );
    PolyBind.createChoice(
        binder, "druid.db.type", Key.get(MetadataUpdaterJobHandler.class), Key.get(SQLMetadataUpdaterJobHandler.class)
    );

  }

  private static void bindDataBaseDerby(Binder binder)
  {
    PolyBind.optionBinder(binder, Key.get(MetadataDbConnector.class))
            .addBinding("derby")
            .to(DerbyConnector.class)
            .in(LazySingleton.class);

    PolyBind.optionBinder(binder, Key.get(MetadataSegmentManager.class))
            .addBinding("derby")
            .to(DerbyMetadataSegmentManager.class)
            .in(LazySingleton.class);

    PolyBind.optionBinder(binder, Key.get(MetadataSegmentManagerProvider.class))
            .addBinding("derby")
            .to(DerbyMetadataSegmentManagerProvider.class)
            .in(LazySingleton.class);

    PolyBind.optionBinder(binder, Key.get(MetadataRuleManager.class))
            .addBinding("derby")
            .to(DerbyMetadataRuleManager.class)
            .in(LazySingleton.class);

    PolyBind.optionBinder(binder, Key.get(MetadataRuleManagerProvider.class))
            .addBinding("derby")
            .to(DerbyMetadataRuleManagerProvider.class)
            .in(LazySingleton.class);

    PolyBind.optionBinder(binder, Key.get(DbSegmentPublisher.class))
            .addBinding("derby")
            .to(SQLMetadataSegmentPublisher.class)
            .in(LazySingleton.class);

    PolyBind.optionBinder(binder, Key.get(MetadataSegmentPublisherProvider.class))
            .addBinding("derby")
            .to(SQLMetadataSegmentPublisherProvider.class)
            .in(LazySingleton.class);

    PolyBind.optionBinder(binder, Key.get(IndexerMetadataCoordinator.class))
            .addBinding("derby")
            .to(IndexerSQLMetadataCoordinator.class)
            .in(LazySingleton.class);

    PolyBind.optionBinder(binder, Key.get(MetadataActionHandler.class))
            .addBinding("derby")
            .to(SQLMetadataActionHandler.class)
            .in(LazySingleton.class);

    PolyBind.optionBinder(binder, Key.get(MetadataUpdaterJobHandler.class))
            .addBinding("derby")
            .to(SQLMetadataUpdaterJobHandler.class)
            .in(LazySingleton.class);

  }

  @Provides
  @LazySingleton
  public IDBI getDbi(final DerbyConnector dbConnector)
  {
    return dbConnector.getDBI();
  }
}
