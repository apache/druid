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

package io.druid.java.util.common;

import java.io.File;
import java.nio.MappedByteBuffer;

/**
 * Facilitates using try-with-resources with {@link MappedByteBuffer}s which don't implement {@link AutoCloseable}.
 *
 * <p>This interface is a specialization of {@code io.druid.collections.ResourceHandler}.
 * @see FileUtils#map(File)
 */
public final class MappedByteBufferHandler implements AutoCloseable
{
  private final MappedByteBuffer mappedByteBuffer;

  MappedByteBufferHandler(MappedByteBuffer mappedByteBuffer)
  {
    this.mappedByteBuffer = mappedByteBuffer;
  }

  /**
   * Returns the wrapped buffer.
   */
  public MappedByteBuffer get()
  {
    return mappedByteBuffer;
  }

  /**
   * Unmaps the wrapped buffer.
   */
  @Override
  public void close()
  {
    ByteBufferUtils.unmap(mappedByteBuffer);
  }
}
