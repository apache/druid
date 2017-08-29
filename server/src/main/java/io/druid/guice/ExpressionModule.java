/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.guice;

import com.fasterxml.jackson.databind.Module;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.multibindings.Multibinder;
import io.druid.initialization.DruidModule;
import io.druid.math.expr.ExprMacroTable;
import io.druid.query.expression.GuiceExprMacroTable;
import io.druid.query.expression.LikeExprMacro;
import io.druid.query.expression.RegexpExtractExprMacro;
import io.druid.query.expression.TimestampCeilExprMacro;
import io.druid.query.expression.TimestampExtractExprMacro;
import io.druid.query.expression.TimestampFloorExprMacro;
import io.druid.query.expression.TimestampFormatExprMacro;
import io.druid.query.expression.TimestampParseExprMacro;
import io.druid.query.expression.TimestampShiftExprMacro;

import java.util.List;

/**
 */
public class ExpressionModule implements DruidModule
{
  public static final List<Class<? extends ExprMacroTable.ExprMacro>> EXPR_MACROS =
      ImmutableList.<Class<? extends ExprMacroTable.ExprMacro>>builder()
          .add(LikeExprMacro.class)
          .add(RegexpExtractExprMacro.class)
          .add(TimestampCeilExprMacro.class)
          .add(TimestampExtractExprMacro.class)
          .add(TimestampFloorExprMacro.class)
          .add(TimestampFormatExprMacro.class)
          .add(TimestampParseExprMacro.class)
          .add(TimestampShiftExprMacro.class)
          .build();

  @Override
  public void configure(Binder binder)
  {
    binder.bind(ExprMacroTable.class).to(GuiceExprMacroTable.class).in(LazySingleton.class);
    for (Class<? extends ExprMacroTable.ExprMacro> exprMacroClass : EXPR_MACROS) {
      addExprMacro(binder, exprMacroClass);
    }
  }

  @Override
  public List<? extends Module> getJacksonModules()
  {
    return ImmutableList.of();
  }

  public static void addExprMacro(final Binder binder, final Class<? extends ExprMacroTable.ExprMacro> clazz)
  {
    Multibinder.newSetBinder(binder, ExprMacroTable.ExprMacro.class)
               .addBinding()
               .to(clazz);
  }
}
