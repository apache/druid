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

package org.apache.druid.sql.calcite.util;

import com.fasterxml.jackson.databind.Module;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import org.apache.druid.initialization.DruidModule;

import java.util.List;

public class CompositeDruidModule implements DruidModule
{
  protected final DruidModule[] modules;

  public CompositeDruidModule(DruidModule... modules)
  {
    this.modules = modules;
  }

  @Override
  public List<? extends Module> getJacksonModules()
  {
    ImmutableList.Builder<Module> builder = ImmutableList.builder();
    for (DruidModule druidModule : modules) {
      builder.addAll(druidModule.getJacksonModules());
    }
    return builder.build();
  }

  @Override
  public void configure(Binder binder)
  {
    for (DruidModule druidModule : modules) {
      binder.install(druidModule);
    }
  }
}
