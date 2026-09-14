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

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.type.SqlTypeMappingRule;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

/**
 * Druid's type factory.
 */
public class DruidTypeFactory extends JavaTypeFactoryImpl
{
  public DruidTypeFactory(final RelDataTypeSystem typeSystem)
  {
    super(typeSystem);
  }

  @Nullable
  @Override
  public RelDataType leastRestrictive(final List<RelDataType> types, final SqlTypeMappingRule mappingRule)
  {
    final RelDataType leastRestrictive = super.leastRestrictive(types, mappingRule);

    if (leastRestrictive != null
        && (SqlTypeUtil.isCollection(leastRestrictive) || leastRestrictive.getSqlTypeName() == SqlTypeName.MAP)
        && types.stream().anyMatch(SqlTypeUtil::isCharacter)) {
      // Return null, indicating that character types cannot be implicitly cast to arrays/maps. Such implicit casts
      // became allowed in Calcite 1.42 (see https://issues.apache.org/jira/browse/CALCITE-7358). Allowing them
      // causes execution failures for e.g. COALESCE(multiValueString, ARRAY['fallback']). See the test
      // CalciteMultiValueStringQueryTest#testMultiValueStringOverlapFilterInconsistentUsage
      return null;
    }

    // Otherwise return what JavaTypeFactoryImpl would have returned.
    return leastRestrictive;
  }
}
