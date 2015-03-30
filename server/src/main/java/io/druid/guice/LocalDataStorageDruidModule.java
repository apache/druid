/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.guice;

import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.Module;
import io.druid.segment.loading.DataSegmentKiller;
import io.druid.segment.loading.DataSegmentPusher;
import io.druid.segment.loading.LocalDataSegmentKiller;
import io.druid.segment.loading.LocalDataSegmentPuller;
import io.druid.segment.loading.LocalDataSegmentPusher;
import io.druid.segment.loading.LocalDataSegmentPusherConfig;
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
