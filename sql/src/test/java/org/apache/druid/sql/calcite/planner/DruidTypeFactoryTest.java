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
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class DruidTypeFactoryTest
{
  private final DruidTypeFactory typeFactory = new DruidTypeFactory(DruidTypeSystem.INSTANCE);

  private final RelDataType varcharType = typeFactory.createSqlType(SqlTypeName.VARCHAR);
  private final RelDataType charType = typeFactory.createSqlType(SqlTypeName.CHAR, 5);
  private final RelDataType arrayType = typeFactory.createArrayType(varcharType, -1);
  private final RelDataType multisetType = typeFactory.createMultisetType(varcharType, -1);
  private final RelDataType mapType = typeFactory.createMapType(varcharType, varcharType);

  @Test
  public void testLeastRestrictiveArrayAndCharacter()
  {
    Assertions.assertNull(typeFactory.leastRestrictive(ImmutableList.of(arrayType, varcharType)));
  }

  @Test
  public void testLeastRestrictiveMultisetAndCharacter()
  {
    Assertions.assertNull(typeFactory.leastRestrictive(ImmutableList.of(multisetType, varcharType)));
  }

  @Test
  public void testLeastRestrictiveMapAndCharacter()
  {
    Assertions.assertNull(typeFactory.leastRestrictive(ImmutableList.of(mapType, varcharType)));
  }

  @Test
  public void testLeastRestrictiveArrayAndArray()
  {
    Assertions.assertEquals(arrayType, typeFactory.leastRestrictive(ImmutableList.of(arrayType, arrayType)));
  }

  @Test
  public void testLeastRestrictiveMapAndMap()
  {
    Assertions.assertEquals(mapType, typeFactory.leastRestrictive(ImmutableList.of(mapType, mapType)));
  }

  @Test
  public void testLeastRestrictiveCharacters()
  {
    final RelDataType leastRestrictive = typeFactory.leastRestrictive(ImmutableList.of(charType, varcharType));
    Assertions.assertNotNull(leastRestrictive);
    Assertions.assertEquals(SqlTypeName.VARCHAR, leastRestrictive.getSqlTypeName());
  }

  @Test
  public void testLeastRestrictiveDefaultDecimalAndFractionalDecimal()
  {
    final RelDataType defaultDecimalType = typeFactory.createSqlType(SqlTypeName.DECIMAL);
    Assertions.assertEquals(19, defaultDecimalType.getPrecision());
    Assertions.assertEquals(0, defaultDecimalType.getScale());
    Assertions.assertEquals(
        typeFactory.createSqlType(SqlTypeName.DECIMAL, 20, 1),
        typeFactory.leastRestrictive(
            ImmutableList.of(defaultDecimalType, typeFactory.createSqlType(SqlTypeName.DECIMAL, 2, 1))
        )
    );
  }

  @Test
  public void testLeastRestrictiveDecimals()
  {
    Assertions.assertEquals(
        typeFactory.createSqlType(SqlTypeName.DECIMAL, 10, 2),
        typeFactory.leastRestrictive(
            ImmutableList.of(
                typeFactory.createSqlType(SqlTypeName.DECIMAL, 10, 2),
                typeFactory.createSqlType(SqlTypeName.DECIMAL, 2, 1)
            )
        )
    );
    Assertions.assertEquals(
        typeFactory.createSqlType(SqlTypeName.DECIMAL),
        typeFactory.leastRestrictive(
            ImmutableList.of(
                typeFactory.createSqlType(SqlTypeName.DECIMAL),
                typeFactory.createSqlType(SqlTypeName.INTEGER)
            )
        )
    );
  }
}
