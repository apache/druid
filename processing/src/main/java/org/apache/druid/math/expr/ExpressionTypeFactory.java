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

import com.google.common.collect.Interner;
import com.google.common.collect.Interners;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.segment.column.TypeFactory;
import org.apache.druid.segment.column.TypeStrategies;
import org.apache.druid.segment.column.TypeStrategy;

import javax.annotation.Nullable;

public class ExpressionTypeFactory implements TypeFactory<ExpressionType>
{
  private static final ExpressionTypeFactory INSTANCE = new ExpressionTypeFactory();
  private static final Interner<ExpressionType> INTERNER = Interners.newWeakInterner();

  public static ExpressionTypeFactory getInstance()
  {
    return INSTANCE;
  }

  private ExpressionTypeFactory()
  {
    // no instantiation
  }

  @Override
  public ExpressionType ofString()
  {
    return ExpressionType.STRING;
  }

  @Override
  public ExpressionType ofFloat()
  {
    throw new IllegalStateException("FLOAT types are not supported natively by Druid expressions");
  }

  @Override
  public ExpressionType ofDouble()
  {
    return ExpressionType.DOUBLE;
  }

  @Override
  public ExpressionType ofLong()
  {
    return ExpressionType.LONG;
  }

  @Override
  public ExpressionType ofArray(ExpressionType elementType)
  {
    if (elementType.isPrimitive()) {
      switch (elementType.getType()) {
        case STRING:
          return ExpressionType.STRING_ARRAY;
        case DOUBLE:
          return ExpressionType.DOUBLE_ARRAY;
        case LONG:
          return ExpressionType.LONG_ARRAY;
      }
    }
    return INTERNER.intern(new ExpressionType(ExprType.ARRAY, null, elementType));
  }

  @Override
  public ExpressionType ofComplex(@Nullable String complexTypeName)
  {
    return INTERNER.intern(new ExpressionType(ExprType.COMPLEX, complexTypeName, null));
  }

  @Override
  public <T> TypeStrategy<T> getTypeStrategy(ExpressionType expressionType)
  {
    final TypeStrategy strategy;
    switch (expressionType.getType()) {
      case LONG:
        strategy = TypeStrategies.LONG;
        break;
      case DOUBLE:
        strategy = TypeStrategies.DOUBLE;
        break;
      case STRING:
        strategy = TypeStrategies.STRING;
        break;
      case ARRAY:
        strategy = new TypeStrategies.ArrayTypeStrategy(expressionType);
        break;
      case COMPLEX:
        TypeStrategy<?> complexStrategy = TypeStrategies.getComplex(expressionType.getComplexTypeName());
        if (complexStrategy == null) {
          throw new IAE("Cannot find strategy for type [%s]", expressionType.asTypeString());
        }
        strategy = complexStrategy;
        break;
      default:
        throw new ISE("Unsupported expression type[%s]", expressionType.getType());
    }
    return strategy;
  }
}
