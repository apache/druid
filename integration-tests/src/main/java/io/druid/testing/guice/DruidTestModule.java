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

package io.druid.testing.guice;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Supplier;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.metamx.emitter.core.LoggingEmitter;
import com.metamx.emitter.core.LoggingEmitterConfig;
import com.metamx.emitter.service.ServiceEmitter;
import io.druid.guice.JsonConfigProvider;
import io.druid.guice.LazySingleton;
import io.druid.guice.ManageLifecycle;
import io.druid.testing.IntegrationTestingConfig;
import io.druid.testing.IntegrationTestingConfigProvider;

/**
 */
public class DruidTestModule implements Module
{
  @Override
  public void configure(Binder binder)
  {
    binder.bind(IntegrationTestingConfig.class).toProvider(IntegrationTestingConfigProvider.class).in(ManageLifecycle.class);
    JsonConfigProvider.bind(binder, "druid.test.config", IntegrationTestingConfigProvider.class);
  }

  @Provides
  @LazySingleton
  public ServiceEmitter getServiceEmitter(Supplier<LoggingEmitterConfig> config, ObjectMapper jsonMapper)
  {
    return new ServiceEmitter("", "", new LoggingEmitter(config.get(), jsonMapper));
  }
}
