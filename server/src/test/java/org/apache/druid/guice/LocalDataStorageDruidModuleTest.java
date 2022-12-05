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

import com.google.common.collect.ImmutableList;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.TypeLiteral;
import org.apache.druid.segment.column.ColumnConfig;
import org.apache.druid.segment.loading.OmniDataSegmentKiller;
import org.apache.druid.segment.loading.RandomStorageLocationSelectorStrategy;
import org.apache.druid.segment.loading.StorageLocation;
import org.apache.druid.segment.loading.StorageLocationSelectorStrategy;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class LocalDataStorageDruidModuleTest
{
  @Test
  public void testSegmentKillerBoundSingleton()
  {
    Injector injector = createInjector();
    OmniDataSegmentKiller killer = injector.getInstance(OmniDataSegmentKiller.class);
    Assert.assertTrue(killer.getKillers().containsKey(LocalDataStorageDruidModule.SCHEME));
    Assert.assertSame(
        killer.getKillers().get(LocalDataStorageDruidModule.SCHEME).get(),
        killer.getKillers().get(LocalDataStorageDruidModule.SCHEME).get()
    );
  }

  private static Injector createInjector()
  {
    return GuiceInjectors.makeStartupInjectorWithModules(
        ImmutableList.of(
            new LocalDataStorageDruidModule(),
            (Module) binder -> {
              binder.bind(new TypeLiteral<List<StorageLocation>>(){}).toInstance(ImmutableList.of());
              binder.bind(ColumnConfig.class).toInstance(() -> 0);
              binder.bind(StorageLocationSelectorStrategy.class)
                    .toInstance(new RandomStorageLocationSelectorStrategy(ImmutableList.of()));
            }
        )
    );
  }
}
