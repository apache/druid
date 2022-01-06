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

package org.apache.druid.sql.calcite.parser;

import com.google.common.base.Preconditions;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlWriter;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Extends the Select call to have custom context parameters specific to Druid
 * These custom parameters are to be manually extracted.
 * This class extends {@link SqlSelect} so that the node can be used further for conversions (Sql to Rel)
 */
public class DruidSqlOrderBy extends SqlOrderBy
{
  // Unsure if this should be kept as is, but this allows reusing super.unparse
  public static final SqlOperator OPERATOR = SqlOrderBy.OPERATOR;
  private final Map<String, String> context;

  public DruidSqlOrderBy(
      SqlOrderBy orderByNode,
      @Nullable SqlNodeList contextMapAsList
  )
  {
    super(
        orderByNode.getParserPosition(),
        orderByNode.getOperandList().get(0),
        (SqlNodeList) orderByNode.getOperandList().get(1),
        orderByNode.getOperandList().get(2),
        orderByNode.getOperandList().get(3)
    );
    this.context = new LinkedHashMap<>();
    if (contextMapAsList != null) {
      int listSize = contextMapAsList.size();
      Preconditions.checkArgument(listSize % 2 == 0);
      for (int i = 0; i < listSize / 2; ++i) {
        SqlNode keyNode = contextMapAsList.get(2 * i);
        SqlNode valueNode = contextMapAsList.get(2 * i + 1);
        if (!(keyNode instanceof SqlLiteral)) {
          // Log some warning about conversion
        }
        if (!(valueNode instanceof SqlLiteral)) {
          // Log some warning about conversion

        }
        String key = keyNode.toString();
        String value = valueNode.toString();
        context.put(key, value);
      }
    }
  }

  @Override
  public SqlOperator getOperator()
  {
    return OPERATOR;
  }

  public Map<String, String> getContext()
  {
    return context;
  }


  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec)
  {
    super.unparse(writer, leftPrec, rightPrec);
    writer.keyword("DESCRIBE");
    writer.keyword("SPACE");
    writer.keyword("POWER");
  }

}
