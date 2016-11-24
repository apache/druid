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
 * A Directory is a flat list of files.  Files may be written once, when they
 * are created.  Once a file is created it may only be opened for read, or
 * deleted.  Random access is permitted both when reading.
 * <p>
 * <p> Java's i/o APIs not used directly, but rather all i/o is
 * through this API.
 * <p>
 */
public abstract class Directory implements Closeable
{

  /**
   * Returns an array of strings, one for each entry in the directory, in sorted (UTF16, java's String.compare) order.
   */
  public abstract String[] listAll() throws IOException;

  /**
   * Removes an existing file in the directory.
   */
  public abstract void deleteFile(String name) throws IOException;

  /**
   * Creates a new, empty file in the directory with the given name.
   * Returns a stream writing this file.
   */
  public abstract IndexOutput createOutput(String name) throws IOException;

  /**
   * Creates a new, empty file for writing in the directory, with a
   * temporary file name including prefix and suffix.  Use
   * {@link IndexOutput#getName} to see what name was used.
   */
  public abstract IndexOutput createTempOutput(String prefix, String suffix) throws IOException;


  /**
   * Renames {@code source} to {@code dest} as an atomic operation,
   * where {@code dest} does not yet exist in the directory.
   * <p>
   */
  public abstract void rename(String source, String dest) throws IOException;


  /**
   * Returns a stream reading an existing file.
   */
  public abstract IndexInput openInput(String name) throws IOException;

  /**
   * Closes the store.
   */
  @Override
  public abstract void close() throws IOException;

  @Override
  public String toString()
  {
    return getClass().getSimpleName() + '@' + Integer.toHexString(hashCode());
  }


}
