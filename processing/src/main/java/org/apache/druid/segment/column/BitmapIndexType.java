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

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.apache.druid.collections.bitmap.BitmapFactory;
import org.apache.druid.collections.bitmap.ImmutableBitmap;
import org.apache.druid.collections.bitmap.MutableBitmap;
import org.apache.druid.error.DruidException;
import org.apache.druid.segment.data.ByteBufferWriter;
import org.apache.druid.segment.data.GenericIndexed;
import org.apache.druid.segment.data.GenericIndexedWriter;
import org.apache.druid.segment.data.ObjectStrategy;
import org.apache.druid.segment.file.SegmentFileBuilder;
import org.apache.druid.segment.file.SegmentFileMapper;
import org.apache.druid.segment.serde.Serializer;
import org.apache.druid.segment.writeout.SegmentWriteOutMedium;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.Objects;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes({
    @JsonSubTypes.Type(value = BitmapIndexType.DictionaryEncodedValueIndex.class, name = BitmapIndexType.TYPE_DICTIONARY),
    @JsonSubTypes.Type(value = BitmapIndexType.NullValueIndex.class, name = BitmapIndexType.TYPE_NULL)
})
public abstract class BitmapIndexType
{
  protected static final String TYPE_DICTIONARY = "dictionaryEncodedValueIndex";
  protected static final String TYPE_NULL = "nullValueIndex";

  /**
   * Interface to serialize bitmap index.
   */
  public interface Writer extends Serializer
  {
    /**
     * Creates a new writer, and prepares the writer for writing.
     */
    void openWriter(
        SegmentWriteOutMedium writeoutMedium,
        String columnName,
        ObjectStrategy<ImmutableBitmap> objectStrategy
    ) throws IOException;

    /**
     * Creates mutable bitmaps on the heap.
     */
    void init(BitmapFactory bitmapFactory, int dictionarySize);

    /**
     * Update the bitmaps on the heap accordingly.
     */
    void add(int row, int sortedId, @Nullable Object o);

    /**
     * Writes the bitmaps to writer, and free up heap memory for bitmaps.
     */
    void finalizeWriter(BitmapFactory bitmapFactory) throws IOException;
  }

  /**
   * Returns a new {@link Writer} to serialize bitmap index.
   */
  public abstract Writer getWriter();

  public static class DictionaryEncodedValueIndex extends BitmapIndexType
  {
    public static final DictionaryEncodedValueIndex INSTANCE = new DictionaryEncodedValueIndex();

    public static GenericIndexed<ImmutableBitmap> read(
        ByteBuffer dataBuffer,
        ObjectStrategy<ImmutableBitmap> objectStrategy,
        SegmentFileMapper fileMapper
    )
    {
      return GenericIndexed.read(dataBuffer, objectStrategy, fileMapper);
    }

    @Override
    public BitmapIndexType.Writer getWriter()
    {
      return new BitmapIndexType.Writer()
      {
        @Nullable
        private GenericIndexedWriter<ImmutableBitmap> writer;

        @Nullable
        private MutableBitmap[] bitmaps;

        @Override
        public void openWriter(
            SegmentWriteOutMedium writeoutMedium,
            String columnName,
            ObjectStrategy<ImmutableBitmap> objectStrategy
        ) throws IOException
        {
          if (writer != null) {
            throw DruidException.defensive("Writer already initiated");
          }
          writer = new GenericIndexedWriter<>(writeoutMedium, columnName, objectStrategy);
          writer.open();
          writer.setObjectsNotSorted();
        }

        @Override
        public void init(BitmapFactory bitmapFactory, int dictionarySize)
        {
          if (bitmaps != null) {
            throw DruidException.defensive("Bitmaps already initiated");
          }
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
        public void finalizeWriter(BitmapFactory bitmapFactory) throws IOException
        {
          if (writer == null) {
            throw DruidException.defensive("Writer not initiated yet");
          } else if (bitmaps == null) {
            throw DruidException.defensive("Invalid state, missing bitmaps");
          }
          for (int i = 0; i < bitmaps.length; i++) {
            writer.write(bitmapFactory.makeImmutableBitmap(bitmaps[i]));
            bitmaps[i] = null; // Reclaim memory
          }
          bitmaps = null;
        }

        @Override
        public long getSerializedSize()
        {
          if (writer == null) {
            throw DruidException.defensive("Writer not initiated yet");
          }
          return writer.getSerializedSize();
        }

        @Override
        public void writeTo(WritableByteChannel channel, SegmentFileBuilder fileBuilder) throws IOException
        {
          if (writer == null) {
            throw DruidException.defensive("Writer not initiated yet");
          }
          writer.writeTo(channel, fileBuilder);
        }
      };
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
      return TYPE_DICTIONARY;
    }
  }

  public static class NullValueIndex extends BitmapIndexType
  {
    public static final NullValueIndex INSTANCE = new NullValueIndex();

    public static ImmutableBitmap read(ByteBuffer dataBuffer, ObjectStrategy<ImmutableBitmap> objectStrategy)
    {
      return objectStrategy.fromByteBufferWithSize(dataBuffer);
    }

    @Override
    public Writer getWriter()
    {
      return new Writer()
      {
        @Nullable
        protected ByteBufferWriter<ImmutableBitmap> writer;

        @Nullable
        protected MutableBitmap[] bitmaps;

        @Override
        public void openWriter(
            SegmentWriteOutMedium writeoutMedium,
            String columnName,
            ObjectStrategy<ImmutableBitmap> objectStrategy
        ) throws IOException
        {
          if (writer != null) {
            throw DruidException.defensive("Writer already initiated");
          }
          writer = new ByteBufferWriter(writeoutMedium, objectStrategy);
          writer.open();
        }

        @Override
        public void init(BitmapFactory bitmapFactory, int unused)
        {
          if (bitmaps != null) {
            throw DruidException.defensive("Bitmaps already initiated");
          }
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
        public void finalizeWriter(BitmapFactory bitmapFactory) throws IOException
        {
          if (writer == null) {
            throw DruidException.defensive("Writer not initiated yet");
          } else if (bitmaps == null) {
            throw DruidException.defensive("Invalid state, missing bitmaps");
          }
          writer.write(bitmapFactory.makeImmutableBitmap(bitmaps[0]));
          bitmaps[0] = null; // Reclaim memory
          bitmaps = null;
        }

        @Override
        public long getSerializedSize()
        {
          if (writer == null) {
            throw DruidException.defensive("Writer not initiated yet");
          }
          return writer.getSerializedSize();
        }

        @Override
        public void writeTo(WritableByteChannel channel, SegmentFileBuilder fileBuilder) throws IOException
        {
          if (writer == null) {
            throw DruidException.defensive("Writer not initiated yet");
          }
          writer.writeTo(channel, fileBuilder);
        }
      };
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
      return TYPE_NULL;
    }
  }
}
