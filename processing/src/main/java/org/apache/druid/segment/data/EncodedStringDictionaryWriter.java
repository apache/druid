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

package org.apache.druid.segment.data;

import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.io.smoosh.FileSmoosher;
import org.apache.druid.segment.column.StringEncodingStrategy;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

public class EncodedStringDictionaryWriter implements DictionaryWriter<String>
{
  public static final byte VERSION = Byte.MAX_VALUE; // hopefully GenericIndexed never makes a version this high...

  private final StringEncodingStrategy encodingStrategy;
  private final DictionaryWriter<byte[]> delegate;

  public EncodedStringDictionaryWriter(
      DictionaryWriter<byte[]> delegate,
      StringEncodingStrategy encodingStrategy
  )
  {
    this.delegate = delegate;
    this.encodingStrategy = encodingStrategy;
  }

  @Override
  public boolean isSorted()
  {
    return delegate.isSorted();
  }

  @Override
  public void open() throws IOException
  {
    delegate.open();
  }

  @Override
  public void write(@Nullable String objectToWrite) throws IOException
  {
    delegate.write(StringUtils.toUtf8Nullable(NullHandling.emptyToNullIfNeeded(objectToWrite)));
  }

  @Nullable
  @Override
  public String get(int dictId) throws IOException
  {
    final byte[] bytes = delegate.get(dictId);
    if (bytes == null) {
      return null;
    }
    return StringUtils.fromUtf8(bytes);
  }

  @Override
  public long getSerializedSize() throws IOException
  {
    return 2 + delegate.getSerializedSize();
  }

  @Override
  public void writeTo(WritableByteChannel channel, FileSmoosher smoosher) throws IOException
  {
    channel.write(ByteBuffer.wrap(new byte[]{VERSION}));
    channel.write(ByteBuffer.wrap(new byte[]{encodingStrategy.getId()}));
    delegate.writeTo(channel, smoosher);
  }
}
