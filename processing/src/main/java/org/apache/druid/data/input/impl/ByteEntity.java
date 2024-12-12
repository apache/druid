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

package org.apache.druid.data.input.impl;

import org.apache.druid.data.input.InputEntity;
import org.apache.druid.io.ByteBufferInputStream;

import javax.annotation.Nullable;
import java.io.InputStream;
import java.net.URI;
import java.nio.ByteBuffer;

public class ByteEntity implements InputEntity
{
  private final ByteBuffer buffer;

  /**
   * Create a new entity. The buffer is not duplicated, so it is important to ensure that its position and limit
   * are not modified after this entity is created.
   */
  public ByteEntity(ByteBuffer buffer)
  {
    this.buffer = buffer;
  }

  public ByteEntity(byte[] bytes)
  {
    this(ByteBuffer.wrap(bytes));
  }

  /**
   * Return the buffer backing this entity. Calling code must not modify the mark, position, or limit of this buffer.
   * If you need to modify them, call {@link ByteBuffer#duplicate()} or {@link ByteBuffer#asReadOnlyBuffer()} and
   * modify the copy.
   */
  public ByteBuffer getBuffer()
  {
    return buffer;
  }

  @Override
  @Nullable
  public URI getUri()
  {
    return null;
  }

  @Override
  public InputStream open()
  {
    return new ByteBufferInputStream(buffer.duplicate());
  }
}
