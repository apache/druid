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
import com.google.common.collect.ImmutableMap;
import com.google.inject.Binder;
import org.apache.druid.guice.ExpressionModule;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.query.expression.LookupEnabledTestExprMacroTable;
import org.apache.druid.query.expression.LookupExprMacro;
import org.apache.druid.query.lookup.LookupExtractorFactoryContainerProvider;
import org.apache.druid.query.lookup.LookupSerdeModule;
import org.apache.druid.sql.calcite.expression.builtin.QueryLookupOperatorConversion;
import org.apache.druid.sql.guice.SqlBindings;
import org.apache.druid.timeline.DataSegment;

import java.util.List;

/**
 * Provides a lookup called {@link LookupEnabledTestExprMacroTable#LOOKYLOO}, provides the SQL {@code LOOKUP}
 * function, and provides the native expression function {@code lookup}.
 */
public class LookylooModule implements DruidModule
{
  @Override
  public void configure(Binder binder)
  {
    final LookupExtractorFactoryContainerProvider lookupProvider =
        LookupEnabledTestExprMacroTable.createTestLookupProvider(
            ImmutableMap.of(
                "a", "xa",
                "abc", "xabc",
                "nosuchkey", "mysteryvalue",
                "6", "x6"
            )
        );

    binder.bind(DataSegment.PruneSpecsHolder.class).toInstance(DataSegment.PruneSpecsHolder.DEFAULT);
    binder.bind(LookupExtractorFactoryContainerProvider.class).toInstance(lookupProvider);
    SqlBindings.addOperatorConversion(binder, QueryLookupOperatorConversion.class);
    ExpressionModule.addExprMacro(binder, LookupExprMacro.class);
  }

  @Override
  public List<? extends Module> getJacksonModules()
  {
    return new LookupSerdeModule().getJacksonModules();
  }
}
