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
import org.apache.druid.server.system.table.TaskTableDescriptor;

/**
 * Registers native system-table routing and the node-local server-properties supplier.
 *
 * <p>Table-specific integrations, such as the task supplier in indexing-service, contribute their own entries to the
 * native system-table multibinders.</p>
 */
public class SystemTableModule implements Module
{
  @Override
  public void configure(final Binder binder)
  {
    DruidBinders.dataSourceQueryHandlerBinder(binder)
                .addBinding(SystemTableDataSource.class)
                .toProvider(SystemTableQueryHandlerProvider.class)
                .in(LazySingleton.class);

    final MapBinder<String, SystemTableDescriptor> descriptorBinder = MapBinder.newMapBinder(binder, String.class, SystemTableDescriptor.class);
    descriptorBinder.addBinding(ServerPropertiesTableDescriptor.TABLE_NAME)
                    .toInstance(new ServerPropertiesTableDescriptor());
    descriptorBinder.addBinding(TaskTableDescriptor.TABLE_NAME)
                    .toInstance(new TaskTableDescriptor());

    final MapBinder<String, SystemTableDataProvider> dataProviderBinder = MapBinder.newMapBinder(binder, String.class, SystemTableDataProvider.class);
    dataProviderBinder.addBinding(ServerPropertiesTableDescriptor.TABLE_NAME)
                      .to(ServerPropertiesTableDataProvider.class)
                      .in(LazySingleton.class);
  }
}
