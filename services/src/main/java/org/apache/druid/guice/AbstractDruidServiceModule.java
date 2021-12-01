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
import com.google.inject.Module;
import com.google.inject.multibindings.MultibindingsScanner;

/**
 * An abstract module for dynamic registration of {@link org.apache.druid.discovery.DruidService}.
 * DruidServices are bound to a set which is mapped to a certain {@link org.apache.druid.discovery.NodeRole}.
 * See {@link org.apache.druid.cli.ServerRunnable#bindDruidServiceType} for how the map is bound.
 *
 * To register a DruidService, create a class something like below:
 *
 * <pre>
 *   public class MyModule extends AbstractDruidServiceModule
 *   {
 *     @ProvidesIntoSet
 *     @Named("myNodeTypeKey")
 *     public Class<? extends DruidService> getDataNodeService()
 *     {
 *       return DataNodeService.class;
 *     }
 *   }
 * </pre>
 *
 * and add it in {@link org.apache.druid.cli.ServerRunnable#getModules}.
 */
public abstract class AbstractDruidServiceModule implements Module
{
  @Override
  public void configure(Binder binder)
  {
    binder.install(MultibindingsScanner.asModule());
  }
}
