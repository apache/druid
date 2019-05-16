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

package org.apache.druid.segment.serde;

import org.apache.druid.io.Channels;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;

public final class MetaSerdeHelper<T>
{
  public static <T> MetaSerdeHelper<T> firstWriteByte(ByteFieldWriter<T> fieldWriter)
  {
    return new MetaSerdeHelper<T>().writeByte(fieldWriter);
  }

  private final List<FieldWriter<T>> fieldWriters = new ArrayList<>();

  private MetaSerdeHelper()
  {
  }

  public MetaSerdeHelper<T> writeInt(IntFieldWriter<T> fieldWriter)
  {
    return writeSomething(fieldWriter);
  }

  public MetaSerdeHelper<T> writeByte(ByteFieldWriter<T> fieldWriter)
  {
    return writeSomething(fieldWriter);
  }

  public MetaSerdeHelper<T> maybeWriteByte(Predicate<T> condition, ByteFieldWriter<T> fieldWriter)
  {
    return writeSomething(
        new FieldWriter<T>()
        {
          @Override
          public void writeTo(ByteBuffer buffer, T x)
          {
            if (condition.test(x)) {
              buffer.put(fieldWriter.getField(x));
            }
          }

          @Override
          public int size(T x)
          {
            return condition.test(x) ? Byte.BYTES : 0;
          }
        }
    );
  }

  public MetaSerdeHelper<T> writeByteArray(Function<T, byte[]> getByteArray)
  {
    return writeSomething(
        new FieldWriter<T>()
        {
          @Override
          public void writeTo(ByteBuffer buffer, T x)
          {
            buffer.put(getByteArray.apply(x));
          }

          @Override
          public int size(T x)
          {
            return getByteArray.apply(x).length;
          }
        }
    );
  }

  public MetaSerdeHelper<T> writeSomething(FieldWriter<T> fieldWriter)
  {
    fieldWriters.add(fieldWriter);
    return this;
  }

  public void writeTo(WritableByteChannel channel, T x) throws IOException
  {
    ByteBuffer meta = ByteBuffer.allocate(size(x));
    for (FieldWriter<T> w : fieldWriters) {
      w.writeTo(meta, x);
    }
    meta.flip();
    Channels.writeFully(channel, meta);
  }

  public int size(T x)
  {
    return fieldWriters.stream().mapToInt(w -> w.size(x)).sum();
  }

  public interface FieldWriter<T>
  {
    void writeTo(ByteBuffer buffer, T x) throws IOException;

    int size(T x);
  }

  @FunctionalInterface
  public interface IntFieldWriter<T> extends FieldWriter<T>
  {
    int getField(T x) throws IOException;

    @Override
    default void writeTo(ByteBuffer buffer, T x) throws IOException
    {
      buffer.putInt(getField(x));
    }

    @Override
    default int size(T x)
    {
      return Integer.BYTES;
    }
  }

  @FunctionalInterface
  public interface ByteFieldWriter<T> extends FieldWriter<T>
  {
    byte getField(T x);

    @Override
    default void writeTo(ByteBuffer buffer, T x)
    {
      buffer.put(getField(x));
    }

    @Override
    default int size(T x)
    {
      return Byte.BYTES;
    }
  }
}
