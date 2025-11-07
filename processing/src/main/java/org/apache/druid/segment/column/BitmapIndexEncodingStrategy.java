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

package org.apache.druid.segment.column;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.apache.druid.collections.bitmap.BitmapFactory;
import org.apache.druid.collections.bitmap.ImmutableBitmap;
import org.apache.druid.collections.bitmap.MutableBitmap;
import org.apache.druid.error.DruidException;
import org.apache.druid.io.Channels;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.segment.data.GenericIndexedWriter;
import org.apache.druid.segment.file.SegmentFileBuilder;
import org.apache.druid.segment.serde.Serializer;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.Objects;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes({
    @JsonSubTypes.Type(value = BitmapIndexEncodingStrategy.DictionaryId.class, name = "DictionaryId"),
    @JsonSubTypes.Type(value = BitmapIndexEncodingStrategy.NullsOnly.class, name = "nullsOnly")
})
public abstract class BitmapIndexEncodingStrategy implements Serializer
{
  public static BitmapIndexEncodingStrategy fromByteBuffer(@Nullable ByteBuffer buffer)
  {
    if (buffer == null) {
      return DictionaryId.LEGACY;
    }
    byte b = buffer.get();
    if (b == 0x00) {
      return DictionaryId.INSTANCE;
    } else if (b == 0x01) {
      return NullsOnly.INSTANCE;
    }
    throw new IAE("Unsupported BitmapIndexEncodingStrategy[%s]", b);
  }

  public byte[] toTypeBytes()
  {
    if (this instanceof DictionaryId) {
      return new byte[]{0x00};
    } else if (this instanceof NullsOnly) {
      return new byte[]{0x01};
    }
    throw new IAE("Unsupported BitmapIndexEncodingStrategy[%s]", this);
  }

  @JsonProperty
  protected final boolean writeStrategyByte;

  /**
   * Assigned in {@link #init(BitmapFactory, int)}
   */
  @Nullable
  protected MutableBitmap[] bitmaps;
  /**
   * Assigned in {@link #close(BitmapFactory, GenericIndexedWriter)}
   */
  @Nullable
  GenericIndexedWriter<ImmutableBitmap> writer;

  BitmapIndexEncodingStrategy()
  {
    this(true);
  }

  BitmapIndexEncodingStrategy(boolean writeStrategyByte)
  {
    this.writeStrategyByte = writeStrategyByte;
  }

  public abstract void init(BitmapFactory bitmapFactory, int dictionarySize);

  public abstract void add(int row, int sortedId, @Nullable Object o);

  public void close(BitmapFactory bitmapFactory, GenericIndexedWriter<ImmutableBitmap> writer) throws IOException
  {
    if (bitmaps == null) {
      throw DruidException.defensive("Not initiated yet");
    }
    this.writer = writer;
    for (int i = 0; i < bitmaps.length; i++) {
      writer.write(bitmapFactory.makeImmutableBitmap(bitmaps[i]));
      bitmaps[i] = null; // Reclaim memory
    }
    bitmaps = null;
  }

  @Override
  public long getSerializedSize()
  {
    return (writeStrategyByte ? toTypeBytes().length : 0) + writer.getSerializedSize();
  }

  @Override
  public void writeTo(WritableByteChannel channel, SegmentFileBuilder fileBuilder) throws IOException
  {
    if (writeStrategyByte) {
      Channels.writeFully(channel, ByteBuffer.wrap(toTypeBytes()));
    }
    writer.writeTo(channel, fileBuilder);
  }

  public static class DictionaryId extends BitmapIndexEncodingStrategy
  {
    public static final DictionaryId LEGACY = new DictionaryId(false);
    public static final DictionaryId INSTANCE = new DictionaryId(true);

    DictionaryId(@JsonProperty("writeStrategyByte") boolean writeStrategyByte)
    {
      super(writeStrategyByte);
    }

    @Override
    public void init(BitmapFactory bitmapFactory, int dictionarySize)
    {
      bitmaps = new MutableBitmap[dictionarySize];
      for (int index = 0; index < dictionarySize; index++) {
        bitmaps[index] = bitmapFactory.makeEmptyMutableBitmap();
      }
    }

    @Override
    public void add(int row, int sortedId, @Nullable Object o)
    {
      bitmaps[sortedId].add(row);
    }

    @Override
    public boolean equals(Object o)
    {
      if (this == o) {
        return true;
      }
      return o != null && getClass() == o.getClass() && this.writeStrategyByte == ((DictionaryId) o).writeStrategyByte;
    }

    @Override
    public int hashCode()
    {
      return Objects.hashCode(writeStrategyByte);
    }

    @Override
    public String toString()
    {
      return "DictionaryId{" +
             "writeStrategyByte=" + writeStrategyByte +
             '}';
    }
  }

  public static class NullsOnly extends BitmapIndexEncodingStrategy
  {
    public static final NullsOnly INSTANCE = new NullsOnly();

    @Override
    public void init(BitmapFactory bitmapFactory, int unused)
    {
      bitmaps = new MutableBitmap[1];
      bitmaps[0] = bitmapFactory.makeEmptyMutableBitmap();
    }

    @Override
    public void add(int row, int sortedId, @Nullable Object o)
    {
      if (o == null) {
        bitmaps[0].add(row);
      }
    }

    @Override
    public boolean equals(Object o)
    {
      if (this == o) {
        return true;
      }
      return o != null && getClass() == o.getClass();
    }

    @Override
    public int hashCode()
    {
      return Objects.hashCode(getClass());
    }

    @Override
    public String toString()
    {
      return "NullsOnly{}";
    }
  }
}
