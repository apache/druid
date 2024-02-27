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

import com.google.common.collect.ImmutableList;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.math.expr.ExpressionProcessing;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.server.security.Action;
import org.apache.druid.server.security.Resource;
import org.apache.druid.server.security.ResourceAction;
import org.apache.druid.server.security.ResourceType;
import org.apache.druid.sql.calcite.expression.DruidExpression;
import org.apache.druid.sql.calcite.expression.SimpleExtraction;
import org.apache.druid.sql.http.SqlParameter;
import org.junit.BeforeClass;

import java.util.Collections;
import java.util.List;

public abstract class CalciteTestBase
{
  public static final List<SqlParameter> DEFAULT_PARAMETERS = ImmutableList.of();

  @BeforeClass
  public static void setupCalciteProperties()
  {
    NullHandling.initializeForTests();
    ExpressionProcessing.initializeForTests();
  }

  /**
   * @deprecated prefer to make {@link DruidExpression} directly to ensure expression tests accurately test the full
   * expression structure, this method is just to have a convenient way to fix a very large number of existing tests
   */
  @Deprecated
  public static DruidExpression makeColumnExpression(final String column)
  {
    return DruidExpression.ofColumn(ColumnType.STRING, column);
  }

  /**
   * @deprecated prefer to make {@link DruidExpression} directly to ensure expression tests accurately test the full
   * expression structure, this method is just to have a convenient way to fix a very large number of existing tests
   */
  @Deprecated
  public static DruidExpression makeExpression(final String staticExpression)
  {
    return makeExpression(ColumnType.STRING, staticExpression);
  }

  /**
   * @deprecated prefer to make {@link DruidExpression} directly to ensure expression tests accurately test the full
   * expression structure, this method is just to have a convenient way to fix a very large number of existing tests
   */
  @Deprecated
  public static DruidExpression makeExpression(final ColumnType columnType, final String staticExpression)
  {
    return makeExpression(columnType, null, staticExpression);
  }

  /**
   * @deprecated prefer to make {@link DruidExpression} directly to ensure expression tests accurately test the full
   * expression structure, this method is just to have a convenient way to fix a very large number of existing tests
   */
  @Deprecated
  public static DruidExpression makeExpression(final SimpleExtraction simpleExtraction, final String staticExpression)
  {
    return makeExpression(ColumnType.STRING, simpleExtraction, staticExpression);
  }

  /**
   * @deprecated prefer to make {@link DruidExpression} directly to ensure expression tests accurately test the full
   * expression structure, this method is just to have a convenient way to fix a very large number of existing tests
   */
  @Deprecated
  public static DruidExpression makeExpression(
      final ColumnType columnType,
      final SimpleExtraction simpleExtraction,
      final String staticExpression
  )
  {
    return DruidExpression.ofExpression(
        columnType,
        simpleExtraction,
        (args) -> staticExpression,
        Collections.emptyList()
    );
  }

  protected static ResourceAction viewRead(final String viewName)
  {
    return new ResourceAction(new Resource(viewName, ResourceType.VIEW), Action.READ);
  }

  protected static ResourceAction dataSourceRead(final String dataSource)
  {
    return new ResourceAction(new Resource(dataSource, ResourceType.DATASOURCE), Action.READ);
  }

  protected static ResourceAction dataSourceWrite(final String dataSource)
  {
    return new ResourceAction(new Resource(dataSource, ResourceType.DATASOURCE), Action.WRITE);
  }

  protected static ResourceAction externalRead(final String inputSourceType)
  {
    return new ResourceAction(new Resource(inputSourceType, ResourceType.EXTERNAL), Action.READ);
  }

  protected static ResourceAction externalWrite(final String inputSourceType)
  {
    return new ResourceAction(new Resource(inputSourceType, ResourceType.EXTERNAL), Action.WRITE);
  }
}
