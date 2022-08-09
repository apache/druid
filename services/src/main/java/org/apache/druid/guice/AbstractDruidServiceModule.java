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

import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.TypeLiteral;
import com.google.inject.multibindings.MapBinder;
import com.google.inject.multibindings.MultibindingsScanner;
import org.apache.druid.discovery.DruidService;
import org.apache.druid.discovery.NodeRole;

import java.util.Set;

/**
 * An abstract module for dynamic registration of {@link DruidService}.
 * DruidServices are bound to a set which is mapped to a certain {@link NodeRole}.
 * See {@link org.apache.druid.initialization.ServerInjectorBuilder#registerNodeRoleModule}
 * for how the map is bound.
 * <p>
 * To register a DruidService, create a class something like below:
 *
 * <pre>
 *   public class MyModule extends AbstractDruidServiceModule
 *   {
 *     @ProvidesIntoSet
 *     @Named("jsonNameOfNodeRole")
 *     public Class<? extends DruidService> getDataNodeService()
 *     {
 *       return DataNodeService.class;
 *     }
 *   }
 * </pre>
 *
 * and add it in {@link org.apache.druid.cli.ServerRunnable#getModules}.
 * The key of Named annotation should be the {@link NodeRole#jsonName}.
 */
public abstract class AbstractDruidServiceModule implements Module
{
  protected abstract NodeRole getNodeRoleKey();

  @Override
  public void configure(Binder binder)
  {
    configure(binder, getNodeRoleKey());
  }

  /**
   * A helper method for extensions which do not implement Module directly.
   */
  public static void configure(Binder binder, NodeRole role)
  {
    binder.install(MultibindingsScanner.asModule());
    MapBinder<NodeRole, Set<Class<? extends DruidService>>> serviceBinder = MapBinder.newMapBinder(
        binder,
        new TypeLiteral<NodeRole>(){},
        new TypeLiteral<Set<Class<? extends DruidService>>>(){}
    );
    serviceBinder
        .addBinding(role)
        .to(Key.get(new TypeLiteral<Set<Class<? extends DruidService>>>(){}, role.getDruidServiceInjectName()));
  }
}
