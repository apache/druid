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

package org.apache.druid.frame.field;

import org.apache.datasketches.memory.WritableMemory;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.frame.write.FrameWriterUtils;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.ColumnValueSelector;

import javax.annotation.Nullable;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class NumericArrayFieldWriter implements FieldWriter
{

  public static final byte NULL_ROW = 0x00;
  public static final byte NON_NULL_ROW = 0x01;

  // Different from NULL_ROW and NON_NULL_ROW bytes
  public static final byte ARRAY_TERMINATOR = 0x00;

  private final ColumnValueSelector selector;
  private final NumericFieldWriterFactory writerFactory;

  public static NumericArrayFieldWriter getLongArrayFieldWriter(final ColumnValueSelector selector)
  {
    return new NumericArrayFieldWriter(selector, LongFieldWriter::forArray);
  }

  public static NumericArrayFieldWriter getFloatArrayFieldWriter(final ColumnValueSelector selector)
  {
    return new NumericArrayFieldWriter(selector, FloatFieldWriter::forArray);
  }

  public static NumericArrayFieldWriter getDoubleArrayFieldWriter(final ColumnValueSelector selector)
  {
    return new NumericArrayFieldWriter(selector, DoubleFieldWriter::forArray);
  }

  public NumericArrayFieldWriter(final ColumnValueSelector selector, NumericFieldWriterFactory writerFactory)
  {
    this.selector = selector;
    this.writerFactory = writerFactory;
  }

  @Override
  public long writeTo(WritableMemory memory, long position, long maxSize)
  {
    Object row = selector.getObject();
    if (row == null) {
      int requiredSize = Byte.BYTES;
      if (requiredSize > maxSize) {
        return -1;
      }
      memory.putByte(position, NULL_ROW);
      return requiredSize;
    } else {
      List<? extends Number> list = FrameWriterUtils.getNumericArrayFromNumericArray(row);

      if (list == null) {
        int requiredSize = Byte.BYTES;
        if (requiredSize > maxSize) {
          return -1;
        }
        memory.putByte(position, NULL_ROW);
        return requiredSize;
      }

      AtomicInteger index = new AtomicInteger(0);
      ColumnValueSelector<Number> columnValueSelector = new ColumnValueSelector<Number>()
      {
        @Override
        public double getDouble()
        {
          final Number n = getObject();
          assert NullHandling.replaceWithDefault() || n != null;
          return n != null ? n.doubleValue() : 0d;
        }

        @Override
        public float getFloat()
        {
          final Number n = getObject();
          assert NullHandling.replaceWithDefault() || n != null;
          return n != null ? n.floatValue() : 0f;
        }

        @Override
        public long getLong()
        {
          final Number n = getObject();
          assert NullHandling.replaceWithDefault() || n != null;
          return n != null ? n.longValue() : 0L;
        }

        @Override
        public void inspectRuntimeShape(RuntimeShapeInspector inspector)
        {

        }

        @Override
        public boolean isNull()
        {
          // TODO(laksh): Add a comment why NullHandling.replaceWithDefault() is not required here
          return getObject() == null;
        }

        @Nullable
        @Override
        public Number getObject()
        {
          return list.get(index.get());
        }

        @Override
        public Class<? extends Number> classOfObject()
        {
          return Number.class;
        }
      };

      NumericFieldWriter writer = writerFactory.get(columnValueSelector);

      int requiredSize = Byte.BYTES + (writer.getNumericSize() + Byte.BYTES) * list.size() + Byte.BYTES;

      if (requiredSize > maxSize) {
        return -1;
      }

      long offset = 0;
      memory.putByte(position + offset, NON_NULL_ROW);
      offset += Byte.BYTES;

      for (; index.get() < list.size(); index.incrementAndGet()) {
        writer.writeTo(
            memory,
            position + offset,
            maxSize - offset
        );
        offset += Byte.BYTES + writer.getNumericSize();
      }

      memory.putByte(position + offset, ARRAY_TERMINATOR);

      return requiredSize;

    }
  }

  @Override
  public void close()
  {
    // Do nothing
  }
}
