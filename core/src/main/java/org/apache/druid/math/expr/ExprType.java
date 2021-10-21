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

import it.unimi.dsi.fastutil.bytes.Byte2ObjectArrayMap;
import it.unimi.dsi.fastutil.bytes.Byte2ObjectMap;
import org.apache.druid.segment.column.TypeDescriptor;

/**
 * Base 'value' types of Druid expression language, all {@link Expr} must evaluate to one of these types.
 */
public enum ExprType implements TypeDescriptor
{
  DOUBLE((byte) 0x01),
  LONG((byte) 0x02),
  STRING((byte) 0x03),
  ARRAY((byte) 0x04),
  COMPLEX((byte) 0x05);

  private static final Byte2ObjectMap<ExprType> TYPE_BYTES = new Byte2ObjectArrayMap<>(ExprType.values().length);

  static {
    for (ExprType type : ExprType.values()) {
      TYPE_BYTES.put(type.getId(), type);
    }
  }

  final byte id;

  ExprType(byte id)
  {
    this.id = id;
  }

  public byte getId()
  {
    return id;
  }

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

  public static ExprType fromByte(byte id)
  {
    return TYPE_BYTES.get(id);
  }
}
