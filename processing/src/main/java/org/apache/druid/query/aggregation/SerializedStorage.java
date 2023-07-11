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

import com.google.common.base.Preconditions;
import org.apache.druid.segment.serde.cell.IOIterator;
import org.apache.druid.segment.serde.cell.IntSerializer;
import org.apache.druid.segment.serde.cell.StagedSerde;
import org.apache.druid.segment.writeout.WriteOutBytes;

import javax.annotation.Nullable;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.NoSuchElementException;

/**
 * simple utility class useful for when multiple passes of input are needed for encoding (e.g. delta or dictionary
 * encoding).
 * <p/>
 * This allows objects to be serialized to some temporary storage and iterated over for final processing.
 * <p/>
 * @param <T>
 */
public class SerializedStorage<T>
{
  private final WriteOutBytes writeOutBytes;
  private final StagedSerde<T> serde;
  private final IntSerializer intSerializer = new IntSerializer();

  public SerializedStorage(WriteOutBytes writeOutBytes, StagedSerde<T> serde)
  {
    this.writeOutBytes = writeOutBytes;
    this.serde = serde;
  }

  public void store(@Nullable T value) throws IOException
  {
    byte[] bytes = serde.serialize(value);

    writeOutBytes.write(intSerializer.serialize(bytes.length));
    writeOutBytes.write(bytes);
  }

  public IOIterator<T> iterator() throws IOException
  {
    return new DeserializingIOIterator<>(writeOutBytes.asInputStream(), serde);
  }

  private static class DeserializingIOIterator<T> implements IOIterator<T>
  {
    private static final int NEEDS_READ = -2;
    private static final int EOF = -1;

    private final byte[] intBytes;
    private final BufferedInputStream inputStream;
    private final StagedSerde<T> serde;

    private int nextSize;

    public DeserializingIOIterator(InputStream inputStream, StagedSerde<T> serde)
    {
      this.inputStream = new BufferedInputStream(inputStream);
      this.serde = serde;
      intBytes = new byte[Integer.BYTES];
      nextSize = NEEDS_READ;
    }

    @Override
    public boolean hasNext() throws IOException
    {
      return getNextSize() > EOF;
    }

    @Override
    public T next() throws IOException
    {
      int currentNextSize = getNextSize();

      if (currentNextSize == -1) {
        throw new NoSuchElementException("end of buffer reached");
      }

      byte[] nextBytes = new byte[currentNextSize];
      int bytesRead = 0;

      while (bytesRead < currentNextSize) {
        int result = inputStream.read(nextBytes, bytesRead, currentNextSize - bytesRead);

        if (result == -1) {
          throw new NoSuchElementException("unexpected end of buffer reached");
        }

        bytesRead += result;
      }

      Preconditions.checkState(bytesRead == currentNextSize);
      T value = serde.deserialize(nextBytes);

      nextSize = NEEDS_READ;

      return value;
    }

    private int getNextSize() throws IOException
    {
      if (nextSize == NEEDS_READ) {
        int bytesRead = 0;

        while (bytesRead < Integer.BYTES) {
          int result = inputStream.read(intBytes, bytesRead, Integer.BYTES - bytesRead);

          if (result == -1) {
            nextSize = EOF;
            return EOF;
          } else {
            bytesRead += result;
          }
        }
        Preconditions.checkState(bytesRead == Integer.BYTES);

        nextSize = ByteBuffer.wrap(intBytes).order(ByteOrder.nativeOrder()).getInt();
      }

      return nextSize;
    }

    @Override
    public void close() throws IOException
    {
      inputStream.close();
    }
  }
}
