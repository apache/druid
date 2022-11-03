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

package org.apache.druid.segment.nested;

import com.google.common.primitives.Ints;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrays;
import it.unimi.dsi.fastutil.ints.IntIterator;
import org.apache.druid.collections.bitmap.ImmutableBitmap;
import org.apache.druid.collections.bitmap.MutableBitmap;
import org.apache.druid.io.Channels;
import org.apache.druid.java.util.common.io.smoosh.FileSmoosher;
import org.apache.druid.java.util.common.io.smoosh.SmooshedWriter;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.data.CompressedVSizeColumnarIntsSerializer;
import org.apache.druid.segment.data.CompressionStrategy;
import org.apache.druid.segment.data.FixedIndexedIntWriter;
import org.apache.druid.segment.data.GenericIndexedWriter;
import org.apache.druid.segment.data.SingleValueColumnarIntsSerializer;
import org.apache.druid.segment.data.VSizeColumnarIntsSerializer;
import org.apache.druid.segment.serde.DictionaryEncodedColumnPartSerde;
import org.apache.druid.segment.serde.Serializer;
import org.apache.druid.segment.writeout.SegmentWriteOutMedium;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.WritableByteChannel;

/**
 * Base class for writer of global dictionary encoded nested literal columns for {@link NestedDataColumnSerializer}.
 * {@link NestedDataColumnSerializer} while processing the 'raw' nested data will call {@link #addValue(int, Object)}
 * for all literal writers, which for this type of writer entails building a local dictionary to map into to the global
 * dictionary ({@link #localDictionary}) and writes this unsorted localId to an intermediate integer column,
 * {@link #intermediateValueWriter}.
 *
 * When processing the 'raw' value column is complete, the {@link #writeTo(int, FileSmoosher)} method will sort the
 * local ids and write them out to a local sorted dictionary, iterate over {@link #intermediateValueWriter} swapping
 * the unsorted local ids with the sorted ids and writing to the compressed id column writer
 * {@link #encodedValueSerializer} building the bitmap indexes along the way.
 */
public abstract class GlobalDictionaryEncodedFieldColumnWriter<T>
{
  private static final Logger log = new Logger(GlobalDictionaryEncodedFieldColumnWriter.class);

  protected final SegmentWriteOutMedium segmentWriteOutMedium;
  protected final String columnName;
  protected final String fieldName;
  protected final IndexSpec indexSpec;
  protected final GlobalDictionaryIdLookup globalDictionaryIdLookup;
  protected final LocalDimensionDictionary localDictionary = new LocalDimensionDictionary();

  protected FixedIndexedIntWriter intermediateValueWriter;
  // maybe someday we allow no bitmap indexes or multi-value columns
  protected int flags = DictionaryEncodedColumnPartSerde.NO_FLAGS;
  protected DictionaryEncodedColumnPartSerde.VERSION version = null;
  protected SingleValueColumnarIntsSerializer encodedValueSerializer;

  protected int cursorPosition;

  protected GlobalDictionaryEncodedFieldColumnWriter(
      String columnName,
      String fieldName,
      SegmentWriteOutMedium segmentWriteOutMedium,
      IndexSpec indexSpec,
      GlobalDictionaryIdLookup globalDictionaryIdLookup
  )
  {
    this.columnName = columnName;
    this.fieldName = fieldName;
    this.segmentWriteOutMedium = segmentWriteOutMedium;
    this.indexSpec = indexSpec;
    this.globalDictionaryIdLookup = globalDictionaryIdLookup;
  }

  /**
   * Perform any value conversion needed before storing the value in the
   */
  T processValue(Object value)
  {
    return (T) value;
  }

  /**
   * Hook to allow implementors the chance to do additional operations during {@link #addValue(int, Object)}, such as
   * writing an additional value column
   */
  void writeValue(@Nullable T value) throws IOException
  {
    // do nothing, if a value column is present this method should be overridden to write the value to the serializer
  }

  /**
   * Find a value in {@link #globalDictionaryIdLookup} as is most appropriate to the writer type
   */
  abstract int lookupGlobalId(T value);

  /**
   * Open the writer so that {@link #addValue(int, Object)} can be called
   */
  public void open() throws IOException
  {
    intermediateValueWriter = new FixedIndexedIntWriter(segmentWriteOutMedium, false);
    intermediateValueWriter.open();
    cursorPosition = 0;
  }

  /**
   * Add a value to the unsorted local dictionary and write to an intermediate column
   */
  public void addValue(int row, Object val) throws IOException
  {
    if (row > cursorPosition) {
      fillNull(row);
    }
    final T value = processValue(val);
    final int globalId = lookupGlobalId(value);
    final int localId = localDictionary.add(globalId);
    intermediateValueWriter.write(localId);
    writeValue(value);
    cursorPosition++;
  }

  private void fillNull(int row) throws IOException
  {
    final T value = processValue(null);
    final int localId = localDictionary.add(0);
    while (cursorPosition < row) {
      intermediateValueWriter.write(localId);
      writeValue(value);
      cursorPosition++;
    }
  }


  /**
   * How many bytes {@link #writeColumnTo(WritableByteChannel, FileSmoosher)} is expected to write to the segment.
   */
  long getSerializedColumnSize() throws IOException
  {
    return Integer.BYTES + Integer.BYTES + encodedValueSerializer.getSerializedSize();
  }

  /**
   * Defines how to write the column, including the dictionary id column, along with any additional columns
   * such as the long or double value column as type appropriate.
   */
  abstract void writeColumnTo(WritableByteChannel channel, FileSmoosher smoosher) throws IOException;

  public void writeTo(int finalRowCount, FileSmoosher smoosher) throws IOException
  {
    if (finalRowCount > cursorPosition) {
      fillNull(finalRowCount);
    }
    // use a child writeout medium so that we can close them when we are finished and don't leave temporary files
    // hanging out until the entire segment is done
    final SegmentWriteOutMedium tmpWriteoutMedium = segmentWriteOutMedium.makeChildWriteOutMedium();
    final FixedIndexedIntWriter sortedDictionaryWriter = new FixedIndexedIntWriter(tmpWriteoutMedium, true);
    sortedDictionaryWriter.open();
    GenericIndexedWriter<ImmutableBitmap> bitmapIndexWriter = new GenericIndexedWriter<>(
        tmpWriteoutMedium,
        columnName,
        indexSpec.getBitmapSerdeFactory().getObjectStrategy()
    );
    bitmapIndexWriter.open();
    bitmapIndexWriter.setObjectsNotSorted();
    final Int2IntOpenHashMap globalToUnsorted = localDictionary.getGlobalIdToLocalId();
    final int[] unsortedToGlobal = new int[localDictionary.size()];
    for (int key : globalToUnsorted.keySet()) {
      unsortedToGlobal[globalToUnsorted.get(key)] = key;
    }
    final int[] sortedGlobal = new int[unsortedToGlobal.length];
    System.arraycopy(unsortedToGlobal, 0, sortedGlobal, 0, unsortedToGlobal.length);
    IntArrays.unstableSort(sortedGlobal);

    final int[] unsortedToSorted = new int[unsortedToGlobal.length];
    final MutableBitmap[] bitmaps = new MutableBitmap[sortedGlobal.length];
    for (int index = 0; index < sortedGlobal.length; index++) {
      final int globalId = sortedGlobal[index];
      sortedDictionaryWriter.write(globalId);
      final int unsortedId = globalToUnsorted.get(globalId);
      unsortedToSorted[unsortedId] = index;
      bitmaps[index] = indexSpec.getBitmapSerdeFactory().getBitmapFactory().makeEmptyMutableBitmap();
    }

    openColumnSerializer(tmpWriteoutMedium, sortedGlobal[sortedGlobal.length - 1]);
    final IntIterator rows = intermediateValueWriter.getIterator();
    int rowCount = 0;
    while (rows.hasNext()) {
      final int unsortedLocalId = rows.nextInt();
      final int sortedLocalId = unsortedToSorted[unsortedLocalId];
      encodedValueSerializer.addValue(sortedLocalId);
      bitmaps[sortedLocalId].add(rowCount++);
    }

    for (MutableBitmap bitmap : bitmaps) {
      bitmapIndexWriter.write(
          indexSpec.getBitmapSerdeFactory().getBitmapFactory().makeImmutableBitmap(bitmap)
      );
    }

    final Serializer fieldSerializer = new Serializer()
    {
      @Override
      public long getSerializedSize() throws IOException
      {
        return 1 + Integer.BYTES +
               sortedDictionaryWriter.getSerializedSize() +
               bitmapIndexWriter.getSerializedSize() +
               getSerializedColumnSize();
      }

      @Override
      public void writeTo(WritableByteChannel channel, FileSmoosher smoosher) throws IOException
      {
        Channels.writeFully(channel, ByteBuffer.wrap(new byte[]{version.asByte()}));
        channel.write(ByteBuffer.wrap(Ints.toByteArray(flags)));
        sortedDictionaryWriter.writeTo(channel, smoosher);
        writeColumnTo(channel, smoosher);
        bitmapIndexWriter.writeTo(channel, smoosher);
      }
    };
    final String fieldFileName = NestedDataColumnSerializer.getFieldFileName(columnName, fieldName);
    final long size = fieldSerializer.getSerializedSize();
    log.debug("Column [%s] serializing [%s] field of size [%d].", columnName, fieldName, size);
    try (SmooshedWriter smooshChannel = smoosher.addWithSmooshedWriter(fieldFileName, size)) {
      fieldSerializer.writeTo(smooshChannel, smoosher);
    }
    finally {
      tmpWriteoutMedium.close();
    }
  }

  private void openColumnSerializer(SegmentWriteOutMedium medium, int maxId) throws IOException
  {
    if (indexSpec.getDimensionCompression() != CompressionStrategy.UNCOMPRESSED) {
      this.version = DictionaryEncodedColumnPartSerde.VERSION.COMPRESSED;
      encodedValueSerializer = CompressedVSizeColumnarIntsSerializer.create(
          fieldName,
          medium,
          columnName,
          maxId,
          indexSpec.getDimensionCompression()
      );
    } else {
      encodedValueSerializer = new VSizeColumnarIntsSerializer(medium, maxId);
      this.version = DictionaryEncodedColumnPartSerde.VERSION.UNCOMPRESSED_SINGLE_VALUE;
    }
    encodedValueSerializer.open();
  }

  public void writeLongAndDoubleColumnLength(WritableByteChannel channel, int longLength, int doubleLength)
      throws IOException
  {
    ByteBuffer intBuffer = ByteBuffer.allocate(Integer.BYTES).order(ByteOrder.nativeOrder());
    intBuffer.position(0);
    intBuffer.putInt(longLength);
    intBuffer.flip();
    Channels.writeFully(channel, intBuffer);
    intBuffer.position(0);
    intBuffer.limit(intBuffer.capacity());
    intBuffer.putInt(doubleLength);
    intBuffer.flip();
    Channels.writeFully(channel, intBuffer);
  }
}
