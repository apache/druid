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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.type.SqlTypeCoercionRule;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.implicit.TypeCoercion;
import org.apache.calcite.sql.validate.implicit.TypeCoercionImpl;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Druid's custom {@link TypeCoercion} and {@link SqlTypeCoercionRule}.
 */
public class DruidTypeCoercion extends TypeCoercionImpl
{
  /**
   * Custom {@link SqlTypeCoercionRule} that allows booleans (and boolean arrays) to be coerced to
   * numerics (and numeric arrays).
   */
  public static final SqlTypeCoercionRule TYPE_COERCION_RULE = buildCoercionRule();

  public DruidTypeCoercion(RelDataTypeFactory typeFactory, SqlValidator validator)
  {
    super(typeFactory, validator);
  }

  @Override
  @Nullable
  public RelDataType commonTypeForBinaryComparison(@Nullable RelDataType type1, @Nullable RelDataType type2)
  {
    // For ARRAY <-> ARRAY comparisons, compare using the element type given
    // by druidCommonElementType.
    if (type1 != null && type2 != null
        && type1.getSqlTypeName() == SqlTypeName.ARRAY
        && type2.getSqlTypeName() == SqlTypeName.ARRAY) {
      final RelDataType e1 = type1.getComponentType();
      final RelDataType e2 = type2.getComponentType();
      if (e1 != null && e2 != null) {
        final RelDataType commonElement = druidCommonElementType(e1, e2);
        if (commonElement != null) {
          final boolean anyNullable = type1.isNullable() || type2.isNullable();
          return factory.createTypeWithNullability(
              factory.createArrayType(commonElement, -1),
              anyNullable
          );
        }
      }
    }

    return super.commonTypeForBinaryComparison(type1, type2);
  }

  /**
   * Choose a common array element type for comparisons.
   */
  @Nullable
  private RelDataType druidCommonElementType(RelDataType e1, RelDataType e2)
  {
    if (SqlTypeUtil.isBoolean(e1) && SqlTypeUtil.isNumeric(e2)) {
      return e2;
    } else if (SqlTypeUtil.isBoolean(e2) && SqlTypeUtil.isNumeric(e1)) {
      return e1;
    } else if (SqlTypeUtil.isCharacter(e1) && SqlTypeUtil.isNumeric(e2)) {
      return e1;
    } else if (SqlTypeUtil.isCharacter(e2) && SqlTypeUtil.isNumeric(e1)) {
      return e2;
    } else {
      return factory.leastRestrictive(ImmutableList.of(e1, e2));
    }
  }

  /**
   * Create a coercion rule that allows coercing booleans to numeric types. This matches Druid's execution layer,
   * where booleans are represented as integers.
   */
  private static SqlTypeCoercionRule buildCoercionRule()
  {
    // Start with the default mapping.
    final Map<SqlTypeName, ImmutableSet<SqlTypeName>> mapping =
        new HashMap<>(SqlTypeCoercionRule.instance().getTypeMapping());

    // Ensure all NUMERIC_TYPES have a coercion mapping from BOOLEAN.
    for (final SqlTypeName target : SqlTypeName.NUMERIC_TYPES) {
      final Set<SqlTypeName> current = mapping.getOrDefault(target, ImmutableSet.of());
      if (!current.contains(SqlTypeName.BOOLEAN)) {
        mapping.put(
            target,
            ImmutableSet.<SqlTypeName>builder().addAll(current).add(SqlTypeName.BOOLEAN).build()
        );
      }
    }

    return SqlTypeCoercionRule.instance(mapping);
  }
}
