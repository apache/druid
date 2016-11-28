/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
   * Closes the stream to further operations.
   * Expert: slice and duplicate operations must not close the original IndexInput.
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

  /**
   * Sets current position in this file, where the next read will occur
   *
   * @see #getFilePointer()
   */
  public abstract void seek(long pos) throws IOException;

  /**
   * The number of bytes in the file.
   */
  public abstract long length() throws IOException;


  /**
   * Creates a slice of this index input, with the given offset, and length.
   * The sliced part will be independent to the origin one.
   *
   * @param offset file point where to slice the input
   * @param length number of bytes to be sliced to the new IndexInput
   */
  public abstract IndexInput slice(long offset, long length) throws IOException;

  /**
   * Creats a copy of this index input,which have the same content 、file point position .
   * but the file point is independent
   * somehow
   *
   * @return
   */
  public abstract IndexInput duplicate() throws IOException;

  /**
   * to test whether the end of the file has been reached
   *
   * @return
   */
  public abstract boolean hasRemaining() throws IOException;

}
