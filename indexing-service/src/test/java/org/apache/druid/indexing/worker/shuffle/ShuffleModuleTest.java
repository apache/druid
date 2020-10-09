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

package org.apache.druid.indexing.worker.shuffle;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Scopes;
import com.google.inject.TypeLiteral;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.java.util.metrics.MonitorScheduler;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import java.util.Optional;

public class ShuffleModuleTest
{
  private ShuffleModule shuffleModule;

  @Before
  public void setup()
  {
    shuffleModule = new ShuffleModule();
  }

  @Test
  public void testGetShuffleMetricsWhenShuffleMonitorExists()
  {
    final ShuffleMonitor shuffleMonitor = new ShuffleMonitor();
    final MonitorScheduler monitorScheduler = Mockito.mock(MonitorScheduler.class);
    Mockito.when(monitorScheduler.findMonitor(ShuffleMonitor.class))
           .thenReturn(Optional.of(shuffleMonitor));
    final Injector injector = createInjector(monitorScheduler);
    final Optional<ShuffleMetrics> optional = injector.getInstance(
        Key.get(new TypeLiteral<Optional<ShuffleMetrics>>() {})
    );
    Assert.assertTrue(optional.isPresent());
  }

  @Test
  public void testGetShuffleMetricsWithNoShuffleMonitor()
  {
    final MonitorScheduler monitorScheduler = Mockito.mock(MonitorScheduler.class);
    Mockito.when(monitorScheduler.findMonitor(ArgumentMatchers.eq(ShuffleMonitor.class)))
           .thenReturn(Optional.empty());
    final Injector injector = createInjector(monitorScheduler);
    final Optional<ShuffleMetrics> optional = injector.getInstance(
        Key.get(new TypeLiteral<Optional<ShuffleMetrics>>() {})
    );
    Assert.assertFalse(optional.isPresent());
  }

  private Injector createInjector(MonitorScheduler monitorScheduler)
  {
    return Guice.createInjector(
        binder -> {
          binder.bindScope(LazySingleton.class, Scopes.SINGLETON);
          binder.bind(MonitorScheduler.class).toInstance(monitorScheduler);
          binder.bind(IntermediaryDataManager.class).toInstance(Mockito.mock(IntermediaryDataManager.class));
        },
        shuffleModule
    );
  }
}
