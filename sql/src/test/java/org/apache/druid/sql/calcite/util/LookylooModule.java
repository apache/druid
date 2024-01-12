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
import com.google.inject.multibindings.MapBinder;
import org.apache.druid.guice.ExpressionModule;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.query.expression.LookupEnabledTestExprMacroTable;
import org.apache.druid.query.expression.LookupExprMacro;
import org.apache.druid.query.lookup.ImmutableLookupMap;
import org.apache.druid.query.lookup.LookupExtractor;
import org.apache.druid.query.lookup.LookupExtractorFactoryContainerProvider;
import org.apache.druid.query.lookup.LookupSerdeModule;
import org.apache.druid.sql.calcite.expression.builtin.QueryLookupOperatorConversion;
import org.apache.druid.sql.guice.SqlBindings;
import org.apache.druid.timeline.DataSegment;

import java.util.List;

/**
 * Provides a lookup called {@link LookupEnabledTestExprMacroTable#LOOKYLOO}, a one-to-one lookup on "dim1" from
 * {@link TestDataBuilder#FOO_SCHEMA} called {@link #LOOKYLOO_INJECTIVE}, a lookup chainable on lookyloo called
 * {@link #LOOKYLOO_CHAINED}. Also adds the SQL {@code LOOKUP} function and the native expression
 * function {@code lookup}.
 */
public class LookylooModule implements DruidModule
{
  private static final String LOOKYLOO_INJECTIVE = "lookyloo121";
  private static final String LOOKYLOO_CHAINED = "lookyloo-chain";

  @Override
  public void configure(Binder binder)
  {
    // Allows SqlBenchmark to add additional lookup tables.
    final MapBinder<String, LookupExtractor> lookupBinder =
        MapBinder.newMapBinder(binder, String.class, LookupExtractor.class);

    lookupBinder.addBinding(LookupEnabledTestExprMacroTable.LOOKYLOO).toInstance(
        ImmutableLookupMap.fromMap(
            ImmutableMap.<String, String>builder()
                        .put("a", "xa")
                        .put("abc", "xabc")
                        .put("nosuchkey", "mysteryvalue")
                        .put("6", "x6")
                        .build()
        ).asLookupExtractor(false, () -> new byte[0])
    );

    lookupBinder.addBinding(LOOKYLOO_CHAINED).toInstance(
        ImmutableLookupMap.fromMap(
            ImmutableMap.<String, String>builder()
                        .put("xa", "za")
                        .put("xabc", "zabc")
                        .put("x6", "z6")
                        .build()
        ).asLookupExtractor(false, () -> new byte[0])
    );

    lookupBinder.addBinding(LOOKYLOO_INJECTIVE).toInstance(
        ImmutableLookupMap.fromMap(
            ImmutableMap.<String, String>builder()
                        .put("", "x")
                        .put("10.1", "x10.1")
                        .put("2", "x2")
                        .put("1", "x1")
                        .put("def", "xdef")
                        .put("abc", "xabc")
                        .build()
        ).asLookupExtractor(true, () -> new byte[0])
    );

    binder.bind(DataSegment.PruneSpecsHolder.class).toInstance(DataSegment.PruneSpecsHolder.DEFAULT);
    binder.bind(LookupExtractorFactoryContainerProvider.class).to(TestLookupProvider.class);
    SqlBindings.addOperatorConversion(binder, QueryLookupOperatorConversion.class);
    ExpressionModule.addExprMacro(binder, LookupExprMacro.class);
  }

  @Override
  public List<? extends Module> getJacksonModules()
  {
    return new LookupSerdeModule().getJacksonModules();
  }
}
