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

package org.apache.druid.sql.calcite.expression;

import org.apache.druid.common.config.NullHandling;
import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.math.expr.ExpressionType;

import javax.annotation.Nullable;

/**
 * Literal value, plus a {@link ExpressionType} that represents how to interpret the literal value.
 *
 * These are similar to {@link ExprEval}, but not identical: unlike {@link ExprEval}, string values in this class
 * are not normalized through {@link NullHandling#emptyToNullIfNeeded(String)}. This allows us to differentiate
 * between null and empty-string literals even when {@link NullHandling#replaceWithDefault()}.
 */
public class DruidLiteral
{
  @Nullable
  private final ExpressionType type;

  @Nullable
  private final Object value;

  DruidLiteral(final ExpressionType type, @Nullable final Object value)
  {
    this.type = type;
    this.value = value;
  }

  @Nullable
  public ExpressionType type()
  {
    return type;
  }

  @Nullable
  public Object value()
  {
    return value;
  }

  public DruidLiteral castTo(final ExpressionType toType)
  {
    if (type.equals(toType)) {
      return this;
    }

    final ExprEval<?> castEval = ExprEval.ofType(type, value).castTo(toType);
    return new DruidLiteral(castEval.type(), castEval.value());
  }
}
