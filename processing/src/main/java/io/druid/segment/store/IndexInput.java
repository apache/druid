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
package io.druid.segment.store;


import java.io.Closeable;
import java.io.IOException;

/**
 * a high level abstract class provided by Directory
 * Druid use this class to read index data
 * this class is not thread safe
 */
public abstract class IndexInput extends DataInput implements Closeable
{


  /**
   * Closes this input to release related resources .
   * Expert: slice and duplicate operations must not close the original IndexInput
   */
  @Override
  public abstract void close() throws IOException;

  /**
   * Returns the current position in this file, where the next read will
   * occur.
   *
   * @see #seek(long)
   */
  public abstract long getFilePointer() throws IOException;

  ;

  /**
   * Sets current position in this file, where the next read will occur
   *
   * @see #getFilePointer()
   */
  public abstract void seek(long pos) throws IOException;

  /**
   * The capacity of this input ,if slice called ,the sliced one's
   * length value will be the slice function's param length
   */
  public abstract long length() throws IOException;

  ;

  /**
   * Creates a slice copy of the initial index input, with the given start offset, and length.
   * The sliced part will be independent to the origin one.
   * Expert : the value returned by the spawned IndexInput's length() method  should be the param length value
   * and the spawned one's file point will keep on increasing.
   *
   * @param offset file point where to slice the input
   * @param length number of bytes from the offset to be sliced into the new IndexInput
   */
  public abstract IndexInput slice(long offset, long length) throws IOException;

  /**
   *
   * create a duplicated one ,which offset position and available length should be the same with the initial one.
   * @return
   */
  public abstract IndexInput duplicate() throws IOException;

  ;

  /**
   * to test whether the file has enough bytes to read
   *
   * @return
   */
  public abstract boolean hasRemaining() throws IOException;

  /**
   * remaining available bytes to  read ,affected by slice or duplicate operations
   *
   * @return
   */
  public abstract long remaining() throws IOException;

  /**
   * Creates a random-access slice of this index input,
   * Subclass overriding this method must treat the returned RandomAccessInput be thread safe.
   * <p>
   * The default implementation calls {@link #duplicate}, and it implements absolute reads as seek+read,
   * may not be best performance choice.
   */
  public RandomAccessInput randomAccess() throws IOException
  {
    final IndexInput duplicate = duplicate();
    final long initialPosition = duplicate.getFilePointer();
    if (duplicate instanceof RandomAccessInput) {
      // slice() already supports random access
      return (RandomAccessInput) duplicate;
    } else {
      // return default impl
      return new RandomAccessInput()
      {
        @Override
        public synchronized byte readByte(long pos) throws IOException
        {
          duplicate.seek(pos);
          byte value = duplicate.readByte();
          duplicate.seek(initialPosition);
          return value;
        }

        @Override
        public synchronized short readShort(long pos) throws IOException
        {
          duplicate.seek(pos);
          short value = duplicate.readShort();
          duplicate.seek(initialPosition);
          return value;
        }

        @Override
        public synchronized int readInt(long pos) throws IOException
        {
          duplicate.seek(pos);
          int value = duplicate.readInt();
          duplicate.seek(initialPosition);
          return value;
        }

        @Override
        public synchronized long readLong(long pos) throws IOException
        {
          duplicate.seek(pos);
          long value = duplicate.readLong();
          duplicate.seek(initialPosition);
          return value;
        }

        @Override
        public String toString()
        {
          return "RandomAccessInput(" + IndexInput.this.toString() + ")";
        }
      };
    }
  }

}
