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

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.fun.SqlAbstractTimeFunction;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * Used for functions like CURRENT_TIMESTAMP and LOCALTIME.
 *
 * Similar to {@link SqlAbstractTimeFunction}, but default precision is
 * {@link DruidTypeSystem#DEFAULT_TIMESTAMP_PRECISION} instead of 0.
 */
public class CurrentTimestampSqlFunction extends SqlAbstractTimeFunction
{
  private final SqlTypeName typeName;

  public CurrentTimestampSqlFunction(final String name, final SqlTypeName typeName)
  {
    super(name, typeName);
    this.typeName = typeName;
  }

  @Override
  public RelDataType inferReturnType(SqlOperatorBinding opBinding)
  {
    if (opBinding.getOperandCount() == 0) {
      return opBinding.getTypeFactory().createSqlType(typeName, DruidTypeSystem.DEFAULT_TIMESTAMP_PRECISION);
    } else {
      return super.inferReturnType(opBinding);
    }
  }
}
