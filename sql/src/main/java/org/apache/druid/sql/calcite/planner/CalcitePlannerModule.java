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

package org.apache.druid.sql.calcite.planner;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.multibindings.Multibinder;
import org.apache.druid.guice.JsonConfigProvider;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.sql.calcite.rule.ExtensionCalciteRuleProvider;

/**
 * The module responsible for provide bindings for the Calcite Planner.
 */
public class CalcitePlannerModule implements Module
{
  @Override
  public void configure(Binder binder)
  {
    // PlannerConfig previously held the segment metadata cache config,
    // so both configs are bound to the same property prefix.
    // It turns out that the order of the arguments above is misleading.
    // We're actually binding the class to the config prefix, not the other way around.
    JsonConfigProvider.bind(binder, "druid.sql.planner", PlannerConfig.class);
    JsonConfigProvider.bind(binder, "druid.sql.planner", SegmentMetadataCacheConfig.class);
    binder.bind(PlannerFactory.class).in(LazySingleton.class);
    binder.bind(DruidOperatorTable.class).in(LazySingleton.class);
    Multibinder.newSetBinder(binder, ExtensionCalciteRuleProvider.class);
  }
}
