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

package org.apache.druid.query.expression;

import com.google.common.collect.ImmutableList;
import org.apache.druid.math.expr.ExprMacroTable;

public class TestExprMacroTable extends ExprMacroTable
{
  public static final ExprMacroTable INSTANCE = new TestExprMacroTable();

  private TestExprMacroTable()
  {
    super(
        ImmutableList.of(
            new IPv4AddressMatchExprMacro(),
            new IPv4AddressParseExprMacro(),
            new IPv4AddressStringifyExprMacro(),
            new LikeExprMacro(),
            new RegexpExtractExprMacro(),
            new TimestampCeilExprMacro(),
            new TimestampExtractExprMacro(),
            new TimestampFloorExprMacro(),
            new TimestampFormatExprMacro(),
            new TimestampParseExprMacro(),
            new TimestampShiftExprMacro(),
            new TrimExprMacro.BothTrimExprMacro(),
            new TrimExprMacro.LeftTrimExprMacro(),
            new TrimExprMacro.RightTrimExprMacro()
        )
    );
  }
}
