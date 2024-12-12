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

package org.apache.druid.query.aggregation;

import org.apache.druid.common.config.NullHandling;
import org.apache.druid.error.DruidException;
import org.apache.druid.error.InvalidInput;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.NullableTypeStrategy;
import org.apache.druid.segment.column.TypeStrategies;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;

public class SingleValueBufferAggregator implements BufferAggregator
{
  private final ColumnValueSelector selector;
  private final ColumnType columnType;
  private final NullableTypeStrategy typeStrategy;
  private boolean isAggregateInvoked = false;

  public SingleValueBufferAggregator(ColumnValueSelector selector, ColumnType columnType)
  {
    this.selector = selector;
    this.columnType = columnType;
    this.typeStrategy = columnType.getNullableStrategy();
  }

  @Override
  public void init(ByteBuffer buf, int position)
  {
    buf.put(position, NullHandling.IS_NULL_BYTE);
  }

  @Override
  public void aggregate(ByteBuffer buf, int position)
  {
    if (isAggregateInvoked) {
      throw InvalidInput.exception("Subquery expression returned more than one row");
    }

    int maxbufferSixe = Byte.BYTES + (columnType.isNumeric()
                                      ? Double.BYTES
                                      : SingleValueAggregatorFactory.DEFAULT_MAX_VALUE_SIZE);
    int written = typeStrategy.write(
        buf,
        position,
        getSelectorObject(),
        maxbufferSixe
    );
    if (written < 0) {
      throw DruidException.forPersona(DruidException.Persona.ADMIN)
                          .ofCategory(DruidException.Category.RUNTIME_FAILURE)
                          .build("Subquery result exceeds the buffer limit [%s]", maxbufferSixe);
    }
    isAggregateInvoked = true;
  }

  @Nullable
  private Object getSelectorObject()
  {
    if (columnType.isNumeric() && selector.isNull()) {
      return null;
    }
    switch (columnType.getType()) {
      case LONG:
        return selector.getLong();
      case FLOAT:
        return selector.getFloat();
      case DOUBLE:
        return selector.getDouble();
      default:
        return selector.getObject();
    }
  }

  @Nullable
  @Override
  public Object get(ByteBuffer buf, int position)
  {
    return typeStrategy.read(buf, position);
  }

  @Override
  public float getFloat(ByteBuffer buf, int position)
  {
    return TypeStrategies.isNullableNull(buf, position)
           ? NullHandling.ZERO_FLOAT
           : TypeStrategies.readNotNullNullableFloat(buf, position);
  }

  @Override
  public double getDouble(ByteBuffer buf, int position)
  {
    return TypeStrategies.isNullableNull(buf, position)
           ? NullHandling.ZERO_DOUBLE
           : TypeStrategies.readNotNullNullableDouble(buf, position);
  }

  @Override
  public long getLong(ByteBuffer buf, int position)
  {
    return TypeStrategies.isNullableNull(buf, position)
           ? NullHandling.ZERO_LONG
           : TypeStrategies.readNotNullNullableLong(buf, position);
  }

  @Override
  public void close()
  {
    // no resources to cleanup
  }
}
