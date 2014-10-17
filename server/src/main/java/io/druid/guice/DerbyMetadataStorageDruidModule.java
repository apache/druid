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
import io.druid.metadata.DerbyMetadataRuleManager;
import io.druid.metadata.DerbyMetadataRuleManagerProvider;
import io.druid.metadata.SQLMetadataRuleManagerProvider;
import io.druid.metadata.DerbyMetadataSegmentManager;
import io.druid.metadata.SQLMetadataSegmentManagerProvider;
import io.druid.metadata.IndexerSQLMetadataStorageCoordinator;
import io.druid.metadata.MetadataRuleManager;
import io.druid.metadata.MetadataSegmentManager;
import io.druid.metadata.MetadataSegmentManagerProvider;
import io.druid.metadata.MetadataStorageConnector;
import io.druid.metadata.MetadataSegmentPublisherProvider;
import io.druid.metadata.MetadataRuleManagerProvider;
import io.druid.metadata.DerbyConnector;
import io.druid.metadata.SQLMetadataStorageActionHandler;
import io.druid.metadata.SQLMetadataSegmentPublisher;
import io.druid.metadata.SQLMetadataSegmentPublisherProvider;
import io.druid.indexer.SQLMetadataStorageUpdaterJobHandler;
import io.druid.indexer.MetadataStorageUpdaterJobHandler;
import io.druid.indexing.overlord.IndexerMetadataStorageCoordinator;
import io.druid.indexing.overlord.MetadataStorageActionHandler;
import io.druid.segment.realtime.SegmentPublisher;
import org.skife.jdbi.v2.IDBI;

public class DerbyMetadataStorageDruidModule implements Module
{

  @Override
  public void configure(Binder binder)
  {
    bindDataBaseDerby(binder);
    PolyBind.createChoice(
        binder, "druid.db.type", Key.get(MetadataStorageConnector.class), Key.get(DerbyConnector.class)
    );
    PolyBind.createChoice(
        binder, "druid.db.type", Key.get(MetadataSegmentManager.class), Key.get(DerbyMetadataSegmentManager.class)
    );
    PolyBind.createChoice(
        binder, "druid.db.type", Key.get(MetadataSegmentManagerProvider.class), Key.get(SQLMetadataSegmentManagerProvider.class)
    );
    PolyBind.createChoice(
        binder, "druid.db.type", Key.get(MetadataRuleManager.class), Key.get(DerbyMetadataRuleManager.class)
    );
    PolyBind.createChoice(
        binder, "druid.db.type", Key.get(MetadataRuleManagerProvider.class), Key.get(DerbyMetadataRuleManagerProvider.class)
    );
    PolyBind.createChoice(
        binder, "druid.db.type", Key.get(SegmentPublisher.class), Key.get(SQLMetadataSegmentPublisher.class)
    );
    PolyBind.createChoice(
        binder, "druid.db.type", Key.get(MetadataSegmentPublisherProvider.class), Key.get(SQLMetadataSegmentPublisherProvider.class)
    );
    PolyBind.createChoice(
        binder, "druid.db.type", Key.get(IndexerMetadataStorageCoordinator.class), Key.get(IndexerSQLMetadataStorageCoordinator.class)
    );
    PolyBind.createChoice(
        binder, "druid.db.type", Key.get(MetadataStorageActionHandler.class), Key.get(SQLMetadataStorageActionHandler.class)
    );
    PolyBind.createChoice(
        binder, "druid.db.type", Key.get(MetadataStorageUpdaterJobHandler.class), Key.get(SQLMetadataStorageUpdaterJobHandler.class)
    );

  }

  private static void bindDataBaseDerby(Binder binder)
  {
    PolyBind.optionBinder(binder, Key.get(MetadataStorageConnector.class))
            .addBinding("derby")
            .to(DerbyConnector.class)
            .in(LazySingleton.class);

    PolyBind.optionBinder(binder, Key.get(MetadataSegmentManager.class))
            .addBinding("derby")
            .to(DerbyMetadataSegmentManager.class)
            .in(LazySingleton.class);

    PolyBind.optionBinder(binder, Key.get(MetadataSegmentManagerProvider.class))
            .addBinding("derby")
            .to(SQLMetadataSegmentManagerProvider.class)
            .in(LazySingleton.class);

    PolyBind.optionBinder(binder, Key.get(MetadataRuleManager.class))
            .addBinding("derby")
            .to(DerbyMetadataRuleManager.class)
            .in(LazySingleton.class);

    PolyBind.optionBinder(binder, Key.get(MetadataRuleManagerProvider.class))
            .addBinding("derby")
            .to(DerbyMetadataRuleManagerProvider.class)
            .in(LazySingleton.class);

    PolyBind.optionBinder(binder, Key.get(SegmentPublisher.class))
            .addBinding("derby")
            .to(SQLMetadataSegmentPublisher.class)
            .in(LazySingleton.class);

    PolyBind.optionBinder(binder, Key.get(MetadataSegmentPublisherProvider.class))
            .addBinding("derby")
            .to(SQLMetadataSegmentPublisherProvider.class)
            .in(LazySingleton.class);

    PolyBind.optionBinder(binder, Key.get(IndexerMetadataStorageCoordinator.class))
            .addBinding("derby")
            .to(IndexerSQLMetadataStorageCoordinator.class)
            .in(LazySingleton.class);

    PolyBind.optionBinder(binder, Key.get(MetadataStorageActionHandler.class))
            .addBinding("derby")
            .to(SQLMetadataStorageActionHandler.class)
            .in(LazySingleton.class);

    PolyBind.optionBinder(binder, Key.get(MetadataStorageUpdaterJobHandler.class))
            .addBinding("derby")
            .to(SQLMetadataStorageUpdaterJobHandler.class)
            .in(LazySingleton.class);

  }

  @Provides
  @LazySingleton
  public IDBI getDbi(final DerbyConnector dbConnector)
  {
    return dbConnector.getDBI();
  }
}
