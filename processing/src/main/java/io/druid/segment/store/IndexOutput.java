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
 * Abstract base class for output to a file in a Directory.  A random-access
 * output stream.  Used for all Druid index output operations.
 * <p>
 * <p>{@code IndexOutput} can only be used from one thread, because it is not
 * thread safe (it keeps internal state like file position).
 *
 * @see Directory
 * @see IndexInput
 */
public abstract class IndexOutput extends DataOutput implements Closeable
{


  private final String name;


  protected IndexOutput(String name)
  {
    this.name = name;
  }


  public String getName()
  {
    return name;
  }

  /**
   * Closes this stream to further operations.
   */
  @Override
  public abstract void close() throws IOException;

  /**
   * Returns the current position in this file, where the next write will
   * occur.
   */
  public abstract long getFilePointer() throws IOException;

  ;
}
