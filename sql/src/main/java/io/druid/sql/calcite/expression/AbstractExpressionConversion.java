/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.sql.calcite.expression;

import org.apache.calcite.sql.SqlKind;

public abstract class AbstractExpressionConversion implements ExpressionConversion
{
  private final SqlKind kind;
  private final String operatorName;

  public AbstractExpressionConversion(SqlKind kind)
  {
    this(kind, null);
  }

  public AbstractExpressionConversion(SqlKind kind, String operatorName)
  {
    this.kind = kind;
    this.operatorName = operatorName;

    if (kind == SqlKind.OTHER_FUNCTION && operatorName == null) {
      throw new NullPointerException("operatorName must be non-null for kind OTHER_FUNCTION");
    } else if (kind != SqlKind.OTHER_FUNCTION && operatorName != null) {
      throw new NullPointerException("operatorName must be non-null for kind " + kind);
    }
  }

  @Override
  public SqlKind sqlKind()
  {
    return kind;
  }

  @Override
  public String operatorName()
  {
    return operatorName;
  }
}
