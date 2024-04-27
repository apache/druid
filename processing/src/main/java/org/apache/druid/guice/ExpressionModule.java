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

import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.multibindings.Multibinder;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.expression.ArrayQuantileExprMacro;
import org.apache.druid.query.expression.CaseInsensitiveContainsExprMacro;
import org.apache.druid.query.expression.ContainsExprMacro;
import org.apache.druid.query.expression.GuiceExprMacroTable;
import org.apache.druid.query.expression.HyperUniqueExpressions;
import org.apache.druid.query.expression.IPv4AddressMatchExprMacro;
import org.apache.druid.query.expression.IPv4AddressParseExprMacro;
import org.apache.druid.query.expression.IPv4AddressStringifyExprMacro;
import org.apache.druid.query.expression.IPv6AddressMatchExprMacro;
import org.apache.druid.query.expression.LikeExprMacro;
import org.apache.druid.query.expression.NestedDataExpressions;
import org.apache.druid.query.expression.RegexpExtractExprMacro;
import org.apache.druid.query.expression.RegexpLikeExprMacro;
import org.apache.druid.query.expression.RegexpReplaceExprMacro;
import org.apache.druid.query.expression.TimestampCeilExprMacro;
import org.apache.druid.query.expression.TimestampExtractExprMacro;
import org.apache.druid.query.expression.TimestampFloorExprMacro;
import org.apache.druid.query.expression.TimestampFormatExprMacro;
import org.apache.druid.query.expression.TimestampParseExprMacro;
import org.apache.druid.query.expression.TimestampShiftExprMacro;
import org.apache.druid.query.expression.TrimExprMacro;

import java.util.List;

/**
 * Module that binds {@link ExprMacroTable} to {@link GuiceExprMacroTable} and configures a starter set of
 * {@link ExprMacroTable.ExprMacro} for all macros defined in the "druid-processing" module.
 */
public class ExpressionModule implements Module
{
  public static final List<Class<? extends ExprMacroTable.ExprMacro>> EXPR_MACROS =
      ImmutableList.<Class<? extends ExprMacroTable.ExprMacro>>builder()
                   .add(ArrayQuantileExprMacro.class)
                   .add(IPv4AddressMatchExprMacro.class)
                   .add(IPv4AddressParseExprMacro.class)
                   .add(IPv4AddressStringifyExprMacro.class)
                   .add(IPv6AddressMatchExprMacro.class)
                   .add(LikeExprMacro.class)
                   .add(RegexpExtractExprMacro.class)
                   .add(RegexpLikeExprMacro.class)
                   .add(RegexpReplaceExprMacro.class)
                   .add(ContainsExprMacro.class)
                   .add(CaseInsensitiveContainsExprMacro.class)
                   .add(TimestampCeilExprMacro.class)
                   .add(TimestampExtractExprMacro.class)
                   .add(TimestampFloorExprMacro.class)
                   .add(TimestampFormatExprMacro.class)
                   .add(TimestampParseExprMacro.class)
                   .add(TimestampShiftExprMacro.class)
                   .add(TrimExprMacro.BothTrimExprMacro.class)
                   .add(TrimExprMacro.LeftTrimExprMacro.class)
                   .add(TrimExprMacro.RightTrimExprMacro.class)
                   .add(HyperUniqueExpressions.HllCreateExprMacro.class)
                   .add(HyperUniqueExpressions.HllAddExprMacro.class)
                   .add(HyperUniqueExpressions.HllEstimateExprMacro.class)
                   .add(HyperUniqueExpressions.HllRoundEstimateExprMacro.class)
                   .add(NestedDataExpressions.JsonObjectExprMacro.class)
                   .add(NestedDataExpressions.JsonKeysExprMacro.class)
                   .add(NestedDataExpressions.JsonPathsExprMacro.class)
                   .add(NestedDataExpressions.JsonValueExprMacro.class)
                   .add(NestedDataExpressions.JsonQueryExprMacro.class)
                   .add(NestedDataExpressions.JsonQueryArrayExprMacro.class)
                   .add(NestedDataExpressions.ToJsonStringExprMacro.class)
                   .add(NestedDataExpressions.ParseJsonExprMacro.class)
                   .add(NestedDataExpressions.TryParseJsonExprMacro.class)
                   .build();

  @Override
  public void configure(Binder binder)
  {
    binder.bind(ExprMacroTable.class).to(GuiceExprMacroTable.class).in(LazySingleton.class);
    for (Class<? extends ExprMacroTable.ExprMacro> exprMacroClass : EXPR_MACROS) {
      addExprMacro(binder, exprMacroClass);
    }
  }

  public static void addExprMacro(final Binder binder, final Class<? extends ExprMacroTable.ExprMacro> clazz)
  {
    Multibinder.newSetBinder(binder, ExprMacroTable.ExprMacro.class)
               .addBinding()
               .to(clazz);
  }
}
