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

package org.apache.druid.sql.calcite.rel;

import com.google.common.collect.ImmutableList;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.sql.calcite.expression.DruidExpression;
import org.junit.Test;

public class GroupingTest
{
  @Test
  public void testEquals()
  {
    EqualsVerifier.forClass(Grouping.class)
                  .usingGetClass()
                  .withNonnullFields("dimensions", "subtotals", "aggregations", "outputRowSignature")
                  .withPrefabValues(
                      DruidExpression.class,
                      DruidExpression.ofLiteral(ColumnType.LONG, "100"),
                      DruidExpression.ofExpression(
                          ColumnType.LONG,
                          (args) -> StringUtils.format("%s + %s", args.get(0), args.get(1)),
                          ImmutableList.of(
                              DruidExpression.ofLiteral(ColumnType.LONG, "100"),
                              DruidExpression.ofLiteral(ColumnType.LONG, "200")
                          )
                      )
                  )
                  .verify();
  }
}
