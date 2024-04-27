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

package org.apache.druid.math.expr;

import com.google.errorprone.annotations.Immutable;
import org.apache.druid.segment.column.TypeDescriptor;

/**
 * Base 'value' types of Druid expression language, all {@link Expr} must evaluate to one of these types.
 */
@Immutable
public enum ExprType implements TypeDescriptor
{
  DOUBLE,
  LONG,
  STRING,
  ARRAY,
  COMPLEX;

  @Override
  public boolean isNumeric()
  {
    return LONG.equals(this) || DOUBLE.equals(this);
  }

  @Override
  public boolean isPrimitive()
  {
    return this != ARRAY && this != COMPLEX;
  }

  @Override
  public boolean isArray()
  {
    return this == ExprType.ARRAY;
  }
}
