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

package org.apache.druid.mapStringString;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2ObjectMap;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectAVLTreeSet;
import it.unimi.dsi.fastutil.objects.ObjectSortedSet;
import org.apache.druid.collections.bitmap.ImmutableBitmap;
import org.apache.druid.collections.bitmap.MutableBitmap;
import org.apache.druid.common.utils.SerializerUtils;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.guava.Comparators;
import org.apache.druid.java.util.common.io.smoosh.FileSmoosher;
import org.apache.druid.java.util.common.io.smoosh.SmooshedWriter;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.GenericColumnSerializer;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.data.BitmapSerdeFactory;
import org.apache.druid.segment.data.CompressedVSizeColumnarIntsSerializer;
import org.apache.druid.segment.data.CompressionStrategy;
import org.apache.druid.segment.data.GenericIndexed;
import org.apache.druid.segment.data.GenericIndexedWriter;
import org.apache.druid.segment.data.ObjectStrategy;
import org.apache.druid.segment.data.SingleValueColumnarIntsSerializer;
import org.apache.druid.segment.data.VSizeColumnarIntsSerializer;
import org.apache.druid.segment.serde.DictionaryEncodedColumnPartSerde;
import org.apache.druid.segment.serde.Serializer;
import org.apache.druid.segment.writeout.SegmentWriteOutMedium;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.WritableByteChannel;
import java.util.Map;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;

/**
 * Serialize the Map<String, String> rows data. For each Map key encountered, one dictionary encoded internal
 * column is created very similar to Druid's builtin String type column's disk layout.
 * So this column's disk layout contains several internal "Key Columns".
 */
public class MapStringStringColumnSerializer implements GenericColumnSerializer
{
  private static final Logger LOGGER = new Logger(MapStringStringColumnSerializer.class);

  public static final byte VERSION = 0x1;

  public static final SerializerUtils SERIALIZER_UTILS = new SerializerUtils();

  // Note: keys are kept sorted which is the order we write them on disk.
  private final SortedMap<String, KeyColumnValuesHolder> keyColumnValues = new TreeMap<>();

  private int rowCount = 0;

  private GenericIndexedWriter<String> keysWriter;

  private boolean closedForWrite = false;

  private final byte[] versionAndMetadataBytes;

  private final SegmentWriteOutMedium segmentWriteOutMedium;
  private final String columnName;

  private final BitmapSerdeFactory bitmapSerdeFactory;
  private final CompressionStrategy compressionStrategy;

  private final ByteOrder byteOrder = IndexIO.BYTE_ORDER;

  private final ObjectMapper jsonMapper;

  public MapStringStringColumnSerializer(
      SegmentWriteOutMedium segmentWriteOutMedium,
      String columnName,
      BitmapSerdeFactory bitmapSerdeFactory,
      CompressionStrategy compressionStrategy,
      ObjectMapper jsonMapper
  )
  {
    this.segmentWriteOutMedium = segmentWriteOutMedium;
    this.columnName = columnName;
    this.bitmapSerdeFactory = bitmapSerdeFactory;
    this.compressionStrategy = compressionStrategy;
    this.jsonMapper = jsonMapper;

    try {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      baos.write(VERSION);
      SERIALIZER_UTILS.writeString(
          baos,
          jsonMapper.writeValueAsString(new MapStringStringColumnMetadata(
            byteOrder,
            bitmapSerdeFactory,
            columnName
            )
          )
      );

      this.versionAndMetadataBytes = baos.toByteArray();
    }
    catch (IOException ex) {
      throw new IAE(ex, "Failed to serialize metadata.");
    }
  }

  @Override
  public void open()
  {

  }

  @Override
  public void serialize(ColumnValueSelector selector) throws IOException
  {
    MapStringStringRow dimRow = (MapStringStringRow) selector.getObject();
    Map<String, String> rowVal = dimRow.getValues();

    for (String key : rowVal.keySet()) {
      keyColumnValues.computeIfAbsent(
          key,
          unused -> new KeyColumnValuesHolder(rowCount)
      );
    }

    for (Map.Entry<String, KeyColumnValuesHolder> e : keyColumnValues.entrySet()) {
      e.getValue().addValue(rowVal.getOrDefault(e.getKey(), null));
    }

    rowCount++;
  }

  @Override
  public long getSerializedSize() throws IOException
  {
    if (!closedForWrite) {
      closedForWrite = true;

      //Do closing stuff necessary to be able to compute serialized size
      for (Map.Entry<String, KeyColumnValuesHolder> e : keyColumnValues.entrySet()) {
        e.getValue().writesFinished(e.getKey());
      }

      keysWriter = createGenericIndexedWriter(GenericIndexed.STRING_STRATEGY);
      for (String key : keyColumnValues.keySet()) {
        keysWriter.write(key);
      }
    }

    long serializedSize = versionAndMetadataBytes.length + keysWriter.getSerializedSize();

    LOGGER.info("Column [%s] metadata serialized size is [%d]", columnName, serializedSize);
    return serializedSize;
  }

  @Override
  public void writeTo(
      WritableByteChannel channel,
      FileSmoosher smoosher
  ) throws IOException
  {
    Preconditions.checkState(closedForWrite, "WTH! Not Closed Yet.");

    channel.write(ByteBuffer.wrap(versionAndMetadataBytes));
    keysWriter.writeTo(channel, smoosher);

    LOGGER.info("Column [%s] serialized metadata with [%d] key columns.", columnName, keyColumnValues.size());

    for (Map.Entry<String, KeyColumnValuesHolder> e : keyColumnValues.entrySet()) {
      e.getValue().writeTo(e.getKey(), smoosher);
    }

    LOGGER.info("Column [%s] serialized successfully with [%d] key columns.", columnName, keyColumnValues.size());
  }

  private <T> GenericIndexedWriter<T> createGenericIndexedWriter(ObjectStrategy<T> objectStrategy) throws IOException
  {
    GenericIndexedWriter<T> writer = new GenericIndexedWriter<>(segmentWriteOutMedium, columnName, objectStrategy);
    writer.open();
    return writer;
  }

  private <T> GenericIndexedWriter<T> createUnsortedGenericIndexedWriter(
      ObjectStrategy<T> objectStrategy
  ) throws IOException
  {
    GenericIndexedWriter<T> writer = createGenericIndexedWriter(objectStrategy);
    writer.setObjectsNotSorted();
    return writer;
  }

  public static String getKeyColumnFileName(String key, String fileNameBase)
  {
    return StringUtils.format("%s_%s", fileNameBase, key);
  }

  private class KeyColumnValuesHolder
  {
    //Note: this is kept on heap as we want to be able to store unique values.
    //technically, sorting can be delayed till the end.
    //If this becomes a bottleneck then we could use an on-disk hash table and sort in the end
    ObjectSortedSet<String> dictionary = new ObjectAVLTreeSet<>(Comparators.naturalNullsFirst());

    GenericIndexedWriter<String> intermediateValueWriter;
    Object2ObjectMap<String, MutableBitmap> bitmaps = new Object2ObjectOpenHashMap<>();

    Serializer finalSerializer;

    KeyColumnValuesHolder(int zeroRowCount)
    {
      try {
        intermediateValueWriter = createUnsortedGenericIndexedWriter(GenericIndexed.STRING_STRATEGY);

        if (zeroRowCount > 0) {
          dictionary.add(null);
          MutableBitmap nullBitmap = bitmapSerdeFactory.getBitmapFactory().makeEmptyMutableBitmap();
          for (int i = 0; i < zeroRowCount; i++) {
            nullBitmap.add(i);
            intermediateValueWriter.write(null);
          }
          bitmaps.put(null, nullBitmap);
        }
      }
      catch (IOException ex) {
        throw new RuntimeException("Unknown ex during serialization.", ex);
      }
    }

    void addValue(String value) throws IOException
    {
      dictionary.add(value);

      MutableBitmap bitmap = bitmaps.get(value);
      if (bitmap == null) {
        bitmap = bitmapSerdeFactory.getBitmapFactory().makeEmptyMutableBitmap();
        bitmaps.put(value, bitmap);
      }
      bitmap.add(rowCount);

      intermediateValueWriter.write(value);
    }

    void writesFinished(String key) throws IOException
    {
      // create dictionary writer
      GenericIndexedWriter<String> sortedDictionaryWriter = createGenericIndexedWriter(GenericIndexed.STRING_STRATEGY);
      for (String s : dictionary) {
        sortedDictionaryWriter.write(s);
      }

      // create values writer
      SingleValueColumnarIntsSerializer encodedValueSerializer;
      if (compressionStrategy != CompressionStrategy.UNCOMPRESSED) {
        encodedValueSerializer = CompressedVSizeColumnarIntsSerializer.create(
            key,
            segmentWriteOutMedium,
            columnName,
            dictionary.size(),
            compressionStrategy
        );
      } else {
        encodedValueSerializer = new VSizeColumnarIntsSerializer(segmentWriteOutMedium, dictionary.size());
      }
      encodedValueSerializer.open();

      Object2IntMap<String> dictionaryEncodingLookup = createRankMap(dictionary);
      for (int i = 0; i < rowCount; i++) {
        encodedValueSerializer.addValue(dictionaryEncodingLookup.getInt(intermediateValueWriter.get(i)));
      }

      // create immutable bitmaps, write in same order as dictionary
      GenericIndexedWriter<ImmutableBitmap> bitmapsWriter = createUnsortedGenericIndexedWriter(bitmapSerdeFactory.getObjectStrategy());
      for (String value : dictionary) {
        bitmapsWriter.write(bitmapSerdeFactory.getBitmapFactory().makeImmutableBitmap(bitmaps.get(value)));
      }


      finalSerializer = DictionaryEncodedColumnPartSerde
          .serializerBuilder()
          .withDictionary(sortedDictionaryWriter)
          .withValue(
              encodedValueSerializer,
              false,
              compressionStrategy != CompressionStrategy.UNCOMPRESSED
          )
          .withBitmapSerdeFactory(bitmapSerdeFactory)
          .withBitmapIndex(bitmapsWriter)
          .withByteOrder(byteOrder)
          .build()
          .getSerializer();
    }

    private <T> Object2IntMap<T> createRankMap(SortedSet<T> dict)
    {
      Object2IntMap<T> rankMap = new Object2IntOpenHashMap<>(dict.size());
      int counter = 0;
      for (T x : dict) {
        rankMap.put(x, counter++);
      }
      return rankMap;
    }

    void writeTo(String key, FileSmoosher smoosher) throws IOException
    {
      LOGGER.info("Column [%s] serializing [%s] key column of size [%d].", columnName, key, finalSerializer.getSerializedSize());
      try (SmooshedWriter smooshChannel = smoosher
          .addWithSmooshedWriter(getKeyColumnFileName(key, columnName), finalSerializer.getSerializedSize())) {
        finalSerializer.writeTo(smooshChannel, smoosher);
      }
    }
  }
}
