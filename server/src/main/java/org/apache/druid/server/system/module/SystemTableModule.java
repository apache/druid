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

package org.apache.druid.server.system.module;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.multibindings.MapBinder;
import org.apache.druid.guice.DruidBinders;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.query.SystemTableDataSource;
import org.apache.druid.server.system.table.ServerPropertiesTableDataProvider;
import org.apache.druid.server.system.table.ServerPropertiesTableDescriptor;
import org.apache.druid.server.system.table.SystemTableDataProvider;
import org.apache.druid.server.system.table.SystemTableDescriptor;
import org.apache.druid.server.system.table.SystemTableSegmentWrangler;

import java.util.Properties;

/**
 * Registers the segment wrangler that reads system tables, and the {@code sys.server_properties} table.
 *
 * <p>Table-specific integrations contribute their own entries to the native system-table multibinders.</p>
 */
public class SystemTableModule implements Module
{
  /**
   * Server property that enables running system-table queries on the Druid query engine. When it is off, servers don't
   * bind system tables or the endpoints that serve them, and the {@code useDruidEngineForSystemTables} query
   * context parameter has no effect.
   */
  public static final String ENABLE_DRUID_ENGINE_FOR_SYSTEM_TABLES_PROPERTY =
      "druid.query.enableDruidEngineForSystemTables";

  public static boolean isEnabled(final Properties properties)
  {
    return Boolean.parseBoolean(properties.getProperty(ENABLE_DRUID_ENGINE_FOR_SYSTEM_TABLES_PROPERTY, "false"));
  }

  @Override
  public void configure(final Binder binder)
  {
    DruidBinders.segmentWranglerBinder(binder)
                .addBinding(SystemTableDataSource.class)
                .to(SystemTableSegmentWrangler.class);
    binder.bind(SystemTableSegmentWrangler.class).in(LazySingleton.class);

    final MapBinder<String, SystemTableDescriptor> descriptorBinder = DruidBinders.systemTableDescriptorBinder(binder);
    descriptorBinder.addBinding(ServerPropertiesTableDescriptor.TABLE_NAME)
                    .toInstance(new ServerPropertiesTableDescriptor());
    final MapBinder<String, SystemTableDataProvider> dataProviderBinder =
        MapBinder.newMapBinder(binder, String.class, SystemTableDataProvider.class);
    dataProviderBinder.addBinding(ServerPropertiesTableDescriptor.TABLE_NAME)
                      .to(ServerPropertiesTableDataProvider.class)
                      .in(LazySingleton.class);
  }
}
