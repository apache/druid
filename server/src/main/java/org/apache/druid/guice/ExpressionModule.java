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

import com.fasterxml.jackson.databind.Module;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.multibindings.Multibinder;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.expression.GuiceExprMacroTable;
import org.apache.druid.query.expression.IPv4AddressMatchExprMacro;
import org.apache.druid.query.expression.IPv4AddressParseExprMacro;
import org.apache.druid.query.expression.IPv4AddressStringifyExprMacro;
import org.apache.druid.query.expression.LikeExprMacro;
import org.apache.druid.query.expression.RegexpExtractExprMacro;
import org.apache.druid.query.expression.TimestampCeilExprMacro;
import org.apache.druid.query.expression.TimestampExtractExprMacro;
import org.apache.druid.query.expression.TimestampFloorExprMacro;
import org.apache.druid.query.expression.TimestampFormatExprMacro;
import org.apache.druid.query.expression.TimestampParseExprMacro;
import org.apache.druid.query.expression.TimestampShiftExprMacro;
import org.apache.druid.query.expression.TrimExprMacro;

import java.util.List;

/**
 */
public class ExpressionModule implements DruidModule
{
  public static final List<Class<? extends ExprMacroTable.ExprMacro>> EXPR_MACROS =
      ImmutableList.<Class<? extends ExprMacroTable.ExprMacro>>builder()
          .add(IPv4AddressMatchExprMacro.class)
          .add(IPv4AddressParseExprMacro.class)
          .add(IPv4AddressStringifyExprMacro.class)
          .add(LikeExprMacro.class)
          .add(RegexpExtractExprMacro.class)
          .add(TimestampCeilExprMacro.class)
          .add(TimestampExtractExprMacro.class)
          .add(TimestampFloorExprMacro.class)
          .add(TimestampFormatExprMacro.class)
          .add(TimestampParseExprMacro.class)
          .add(TimestampShiftExprMacro.class)
          .add(TrimExprMacro.BothTrimExprMacro.class)
          .add(TrimExprMacro.LeftTrimExprMacro.class)
          .add(TrimExprMacro.RightTrimExprMacro.class)
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
