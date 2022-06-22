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

package org.apache.druid.indexing.seekablestream;

import com.google.common.base.Preconditions;
import org.apache.druid.data.input.InputEntity;
import org.apache.druid.data.input.impl.ByteEntity;
import org.apache.druid.io.ByteBufferInputStream;
import org.apache.druid.java.util.common.IAE;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

import java.io.InputStream;
import java.net.URI;
import java.nio.ByteBuffer;

/**
 * This class is only to be used with {@link SettableByteEntityReader} and {code KafkaInputFormat}. It is useful for stream
 * processing where binary records are arriving as a list but {@link org.apache.druid.data.input.InputEntityReader}, that
 * parses the data, expects an {@link InputStream}. This class mimics a continuous InputStream while behind the scenes,
 * binary records are being put one after the other that the InputStream consumes bytes from. One record is fully
 * consumed and only then the next record is set. This class doesn't allow reading the same data twice.
 * This class solely exists to overcome the limitations imposed by interfaces for reading and parsing data.
 *
 */
@NotThreadSafe
public class SettableByteEntity<T extends ByteEntity> implements InputEntity
{
  private final SettableByteBufferInputStream inputStream;
  private boolean opened = false;
  private T entity;

  public SettableByteEntity()
  {
    this.inputStream = new SettableByteBufferInputStream();
  }

  public void setEntity(T entity)
  {
    inputStream.setBuffer(entity.getBuffer());
    this.entity = entity;
    opened = false;
  }

  @Nullable
  @Override
  public URI getUri()
  {
    return null;
  }

  public T getEntity()
  {
    return entity;
  }

  /**
   * This method can be called multiple times only for different data. So you can open a new input stream
   * only after a new buffer is in use.
   */
  @Override
  public InputStream open()
  {
    if (opened) {
      throw new IllegalArgumentException("Can't open the input stream on SettableByteEntity more than once");
    }

    opened = true;
    return inputStream;
  }

  public static final class SettableByteBufferInputStream extends InputStream
  {
    @Nullable
    private ByteBufferInputStream delegate;

    public void setBuffer(ByteBuffer newBuffer)
    {
      if (null != delegate && available() > 0) {
        throw new IAE("New data cannot be set in buffer till all the old data has been read");
      }
      this.delegate = new ByteBufferInputStream(newBuffer);
    }

    @Override
    public int read()
    {
      Preconditions.checkNotNull(delegate, "Buffer is not set");
      return delegate.read();
    }

    @Override
    public int read(byte[] bytes, int off, int len)
    {
      Preconditions.checkNotNull(delegate, "Buffer is not set");
      return delegate.read(bytes, off, len);
    }

    @Override
    public int available()
    {
      Preconditions.checkNotNull(delegate, "Buffer is not set");
      return delegate.available();
    }
  }
}
