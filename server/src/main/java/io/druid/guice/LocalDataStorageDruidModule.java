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
import io.druid.segment.loading.DataSegmentPusher;
import io.druid.segment.loading.DataSegmentKiller;
import io.druid.segment.loading.LocalDataSegmentPuller;
import io.druid.segment.loading.LocalDataSegmentPusher;
import io.druid.segment.loading.LocalDataSegmentPusherConfig;
import io.druid.segment.loading.LocalDataSegmentKiller;
import io.druid.segment.loading.OmniSegmentLoader;
import io.druid.segment.loading.SegmentLoader;

/**
 */
public class LocalDataStorageDruidModule implements Module
{
  @Override
  public void configure(Binder binder)
  {
    binder.bind(SegmentLoader.class).to(OmniSegmentLoader.class).in(LazySingleton.class);

    bindDeepStorageLocal(binder);

    PolyBind.createChoice(
        binder, "druid.storage.type", Key.get(DataSegmentPusher.class), Key.get(LocalDataSegmentPusher.class)
    );
  }

  private static void bindDeepStorageLocal(Binder binder)
  {
    Binders.dataSegmentPullerBinder(binder)
                .addBinding("local")
                .to(LocalDataSegmentPuller.class)
                .in(LazySingleton.class);

    PolyBind.optionBinder(binder, Key.get(DataSegmentKiller.class))
        .addBinding("local")
        .to(LocalDataSegmentKiller.class)
        .in(LazySingleton.class);

    PolyBind.optionBinder(binder, Key.get(DataSegmentPusher.class))
            .addBinding("local")
            .to(LocalDataSegmentPusher.class)
            .in(LazySingleton.class);
    JsonConfigProvider.bind(binder, "druid.storage", LocalDataSegmentPusherConfig.class);
  }
}
