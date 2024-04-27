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

package org.apache.druid.segment.column;

import com.google.common.collect.Interner;
import com.google.common.collect.Interners;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;

import javax.annotation.Nullable;

public class ColumnTypeFactory implements TypeFactory<ColumnType>
{
  private static final ColumnTypeFactory INSTANCE = new ColumnTypeFactory();

  private static final Interner<ColumnType> INTERNER = Interners.newWeakInterner();

  public static TypeFactory<ColumnType> getInstance()
  {
    return INSTANCE;
  }

  private ColumnTypeFactory()
  {
    // no instantiation
  }

  public static ColumnType ofType(TypeSignature<ValueType> type)
  {
    switch (type.getType()) {
      case LONG:
        return ColumnType.LONG;
      case FLOAT:
        return ColumnType.FLOAT;
      case DOUBLE:
        return ColumnType.DOUBLE;
      case STRING:
        return ColumnType.STRING;
      case ARRAY:
        switch (type.getElementType().getType()) {
          case LONG:
            return ColumnType.LONG_ARRAY;
          case FLOAT:
            return ColumnType.FLOAT_ARRAY;
          case DOUBLE:
            return ColumnType.DOUBLE_ARRAY;
          case STRING:
            return ColumnType.STRING_ARRAY;
          default:
            return ColumnType.ofArray(ofType(type.getElementType()));
        }
      case COMPLEX:
        return INTERNER.intern(new ColumnType(ValueType.COMPLEX, type.getComplexTypeName(), null));
      default:
        throw new ISE("Unsupported column type[%s]", type.asTypeString());
    }
  }

  public static ColumnType ofValueType(ValueType type)
  {
    switch (type) {
      case LONG:
        return ColumnType.LONG;
      case FLOAT:
        return ColumnType.FLOAT;
      case DOUBLE:
        return ColumnType.DOUBLE;
      case STRING:
        return ColumnType.STRING;
      case COMPLEX:
        return ColumnType.UNKNOWN_COMPLEX;
      default:
        throw new ISE("Unsupported column type[%s]", type);
    }
  }

  @Override
  public <T> TypeStrategy<T> getTypeStrategy(ColumnType type)
  {
    final TypeStrategy strategy;
    switch (type.getType()) {
      case LONG:
        strategy = TypeStrategies.LONG;
        break;
      case FLOAT:
        strategy = TypeStrategies.FLOAT;
        break;
      case DOUBLE:
        strategy = TypeStrategies.DOUBLE;
        break;
      case STRING:
        strategy = TypeStrategies.STRING;
        break;
      case ARRAY:
        strategy = new TypeStrategies.ArrayTypeStrategy(type);
        break;
      case COMPLEX:
        TypeStrategy<?> complexStrategy = TypeStrategies.getComplex(type.getComplexTypeName());
        if (complexStrategy == null) {
          throw new IAE("Cannot find strategy for type [%s]", type.asTypeString());
        }
        strategy = complexStrategy;
        break;
      default:
        throw new ISE("Unsupported column type[%s]", type);
    }
    return strategy;
  }

  @Override
  public ColumnType ofString()
  {
    return ColumnType.STRING;
  }

  @Override
  public ColumnType ofFloat()
  {
    return ColumnType.FLOAT;
  }

  @Override
  public ColumnType ofDouble()
  {
    return ColumnType.DOUBLE;
  }

  @Override
  public ColumnType ofLong()
  {
    return ColumnType.LONG;
  }

  @Override
  public ColumnType ofArray(ColumnType elementType)
  {
    // i guess this is potentially unbounded if we ever support arbitrarily deep nested arrays
    return INTERNER.intern(new ColumnType(ValueType.ARRAY, null, elementType));
  }

  @Override
  public ColumnType ofComplex(@Nullable String complexTypeName)
  {
    return INTERNER.intern(new ColumnType(ValueType.COMPLEX, complexTypeName, null));
  }
}
