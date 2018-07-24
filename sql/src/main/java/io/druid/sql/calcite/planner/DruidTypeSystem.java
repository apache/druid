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

package io.druid.sql.calcite.planner;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.type.SqlTypeName;

public class DruidTypeSystem implements RelDataTypeSystem
{
  public static final DruidTypeSystem INSTANCE = new DruidTypeSystem();

  private DruidTypeSystem()
  {
    // Singleton.
  }

  @Override
  public int getMaxScale(final SqlTypeName typeName)
  {
    return RelDataTypeSystem.DEFAULT.getMaxScale(typeName);
  }

  @Override
  public int getDefaultPrecision(final SqlTypeName typeName)
  {
    return RelDataTypeSystem.DEFAULT.getDefaultPrecision(typeName);
  }

  @Override
  public int getMaxPrecision(final SqlTypeName typeName)
  {
    return RelDataTypeSystem.DEFAULT.getMaxPrecision(typeName);
  }

  @Override
  public int getMaxNumericScale()
  {
    return RelDataTypeSystem.DEFAULT.getMaxNumericScale();
  }

  @Override
  public int getMaxNumericPrecision()
  {
    return RelDataTypeSystem.DEFAULT.getMaxNumericPrecision();
  }

  @Override
  public String getLiteral(final SqlTypeName typeName, final boolean isPrefix)
  {
    return RelDataTypeSystem.DEFAULT.getLiteral(typeName, isPrefix);
  }

  @Override
  public boolean isCaseSensitive(final SqlTypeName typeName)
  {
    return RelDataTypeSystem.DEFAULT.isCaseSensitive(typeName);
  }

  @Override
  public boolean isAutoincrement(final SqlTypeName typeName)
  {
    return RelDataTypeSystem.DEFAULT.isAutoincrement(typeName);
  }

  @Override
  public int getNumTypeRadix(final SqlTypeName typeName)
  {
    return RelDataTypeSystem.DEFAULT.getNumTypeRadix(typeName);
  }

  @Override
  public RelDataType deriveSumType(final RelDataTypeFactory typeFactory, final RelDataType argumentType)
  {
    // Widen all sums to 64-bits regardless of the size of the inputs.

    if (SqlTypeName.INT_TYPES.contains(argumentType.getSqlTypeName())) {
      return Calcites.createSqlType(typeFactory, SqlTypeName.BIGINT);
    } else {
      return Calcites.createSqlType(typeFactory, SqlTypeName.DOUBLE);
    }
  }

  @Override
  public RelDataType deriveAvgAggType(
      final RelDataTypeFactory typeFactory,
      final RelDataType argumentType
  )
  {
    return RelDataTypeSystem.DEFAULT.deriveAvgAggType(typeFactory, argumentType);
  }

  @Override
  public RelDataType deriveCovarType(
      final RelDataTypeFactory typeFactory,
      final RelDataType arg0Type,
      final RelDataType arg1Type
  )
  {
    return RelDataTypeSystem.DEFAULT.deriveCovarType(typeFactory, arg0Type, arg1Type);
  }

  @Override
  public RelDataType deriveFractionalRankType(final RelDataTypeFactory typeFactory)
  {
    return RelDataTypeSystem.DEFAULT.deriveFractionalRankType(typeFactory);
  }

  @Override
  public RelDataType deriveRankType(final RelDataTypeFactory typeFactory)
  {
    return RelDataTypeSystem.DEFAULT.deriveRankType(typeFactory);
  }

  @Override
  public boolean isSchemaCaseSensitive()
  {
    return RelDataTypeSystem.DEFAULT.isSchemaCaseSensitive();
  }
}
