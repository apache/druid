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

import org.apache.druid.common.utils.SerializerUtils;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.ByteBufferUtils;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.io.smoosh.FileSmoosher;
import org.apache.druid.java.util.common.io.smoosh.SmooshedFileMapper;
import org.apache.druid.java.util.common.io.smoosh.SmooshedWriter;
import org.apache.druid.segment.column.StringEncodingStrategies;
import org.apache.druid.segment.column.TypeStrategies;
import org.apache.druid.segment.data.DictionaryWriter;
import org.apache.druid.segment.data.FixedIndexed;
import org.apache.druid.segment.data.FrontCodedIntArrayIndexed;
import org.apache.druid.segment.data.Indexed;
import org.apache.druid.segment.serde.ColumnSerializerUtils;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Value to dictionary id lookup, backed with memory mapped dictionaries populated lazily by the supplied
 * {@link DictionaryWriter}.
 */
public final class DictionaryIdLookup implements Closeable
{

  private final String name;
  private final Path tempBasePath;

  @Nullable
  private final DictionaryWriter<String> stringDictionaryWriter;
  private Path stringDictionaryFile = null;
  private SmooshedFileMapper stringBufferMapper = null;
  private Indexed<ByteBuffer> stringDictionary = null;

  @Nullable
  private final DictionaryWriter<Long> longDictionaryWriter;
  private Path longDictionaryFile = null;
  private MappedByteBuffer longBuffer = null;
  private FixedIndexed<Long> longDictionary = null;

  @Nullable
  private final DictionaryWriter<Double> doubleDictionaryWriter;
  private Path doubleDictionaryFile = null;
  MappedByteBuffer doubleBuffer = null;
  FixedIndexed<Double> doubleDictionary = null;

  @Nullable
  private final DictionaryWriter<int[]> arrayDictionaryWriter;
  private Path arrayDictionaryFile = null;
  private MappedByteBuffer arrayBuffer = null;
  private FrontCodedIntArrayIndexed arrayDictionary = null;

  public DictionaryIdLookup(
      String name,
      Path tempBasePath,
      @Nullable DictionaryWriter<String> stringDictionaryWriter,
      @Nullable DictionaryWriter<Long> longDictionaryWriter,
      @Nullable DictionaryWriter<Double> doubleDictionaryWriter,
      @Nullable DictionaryWriter<int[]> arrayDictionaryWriter
  )
  {
    this.name = name;
    this.tempBasePath = tempBasePath;
    this.stringDictionaryWriter = stringDictionaryWriter;
    this.longDictionaryWriter = longDictionaryWriter;
    this.doubleDictionaryWriter = doubleDictionaryWriter;
    this.arrayDictionaryWriter = arrayDictionaryWriter;
  }

  public int[] getArrayValue(int id)
  {
    ensureArrayDictionaryLoaded();
    return arrayDictionary.get(id - arrayOffset());
  }

  @Nullable
  public Object getDictionaryValue(int id)
  {
    ensureStringDictionaryLoaded();
    ensureLongDictionaryLoaded();
    ensureDoubleDictionaryLoaded();
    ensureArrayDictionaryLoaded();
    if (id < longOffset()) {
      return StringUtils.fromUtf8Nullable(stringDictionary.get(id));
    } else if (id < doubleOffset()) {
      return longDictionary.get(id - longOffset());
    } else if (id < arrayOffset()) {
      return doubleDictionary.get(id - doubleOffset());
    } else {
      return arrayDictionary.get(id - arrayOffset());
    }
  }

  public int lookupString(@Nullable String value)
  {
    ensureStringDictionaryLoaded();
    final byte[] bytes = StringUtils.toUtf8Nullable(value);
    final int index = stringDictionary.indexOf(bytes == null ? null : ByteBuffer.wrap(bytes));
    if (index < 0) {
      throw DruidException.defensive("Value not found in column[%s] string dictionary", name);
    }
    return index;
  }

  public int lookupLong(@Nullable Long value)
  {
    ensureLongDictionaryLoaded();
    final int index = longDictionary.indexOf(value);
    if (index < 0) {
      throw DruidException.defensive("Value not found in column[%s] long dictionary", name);
    }
    return index + longOffset();
  }

  public int lookupDouble(@Nullable Double value)
  {
    ensureDoubleDictionaryLoaded();
    final int index = doubleDictionary.indexOf(value);
    if (index < 0) {
      throw DruidException.defensive("Value not found in column[%s] double dictionary", name);
    }
    return index + doubleOffset();
  }

  public int lookupArray(@Nullable int[] value)
  {
    ensureArrayDictionaryLoaded();
    final int index = arrayDictionary.indexOf(value);
    if (index < 0) {
      throw DruidException.defensive("Value not found in column[%s] array dictionary", name);
    }
    return index + arrayOffset();
  }

  @Nullable
  public SmooshedFileMapper getStringBufferMapper()
  {
    return stringBufferMapper;
  }

  @Nullable
  public ByteBuffer getLongBuffer()
  {
    return longBuffer;
  }

  @Nullable
  public ByteBuffer getDoubleBuffer()
  {
    return doubleBuffer;
  }

  @Nullable
  public ByteBuffer getArrayBuffer()
  {
    return arrayBuffer;
  }

  @Override
  public void close()
  {
    if (stringBufferMapper != null) {
      stringBufferMapper.close();
      deleteTempFile(stringDictionaryFile);
    }
    if (longBuffer != null) {
      ByteBufferUtils.unmap(longBuffer);
      deleteTempFile(longDictionaryFile);
    }
    if (doubleBuffer != null) {
      ByteBufferUtils.unmap(doubleBuffer);
      deleteTempFile(doubleDictionaryFile);
    }
    if (arrayBuffer != null) {
      ByteBufferUtils.unmap(arrayBuffer);
      deleteTempFile(arrayDictionaryFile);
    }
  }

  private int longOffset()
  {
    return stringDictionaryWriter != null ? stringDictionaryWriter.getCardinality() : 0;
  }

  private int doubleOffset()
  {
    return longOffset() + (longDictionaryWriter != null ? longDictionaryWriter.getCardinality() : 0);
  }

  private int arrayOffset()
  {
    return doubleOffset() + (doubleDictionaryWriter != null ? doubleDictionaryWriter.getCardinality() : 0);
  }

  private void ensureStringDictionaryLoaded()
  {
    if (stringDictionary == null) {
      // GenericIndexed v2 can write to multiple files if the dictionary is larger than 2gb, so we use a smooshfile
      // for strings because of this. if other type dictionary writers could potentially use multiple internal files
      // in the future, we should transition them to using this approach as well (or build a combination smoosher and
      // mapper so that we can have a mutable smoosh)
      File stringSmoosh = FileUtils.createTempDirInLocation(tempBasePath, StringUtils.urlEncode(name) + "__stringTempSmoosh");
      stringDictionaryFile = stringSmoosh.toPath();
      final String fileName = ColumnSerializerUtils.getInternalFileName(
          name,
          ColumnSerializerUtils.STRING_DICTIONARY_FILE_NAME
      );

      try (
          final FileSmoosher smoosher = new FileSmoosher(stringSmoosh);
          final SmooshedWriter writer = smoosher.addWithSmooshedWriter(
              fileName,
              stringDictionaryWriter.getSerializedSize()
          )
      ) {
        stringDictionaryWriter.writeTo(writer, smoosher);
        writer.close();
        smoosher.close();
        stringBufferMapper = SmooshedFileMapper.load(stringSmoosh);
        final ByteBuffer stringBuffer = stringBufferMapper.mapFile(fileName);
        stringDictionary = StringEncodingStrategies.getStringDictionarySupplier(
            stringBufferMapper,
            stringBuffer,
            ByteOrder.nativeOrder()
        ).get();
      }
      catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  private void ensureLongDictionaryLoaded()
  {
    if (longDictionary == null) {
      longDictionaryFile = makeTempFile(name + ColumnSerializerUtils.LONG_DICTIONARY_FILE_NAME);
      longBuffer = SerializerUtils.mapSerializer(longDictionaryFile, longDictionaryWriter);
      longDictionary = FixedIndexed.read(longBuffer, TypeStrategies.LONG, ByteOrder.nativeOrder(), Long.BYTES).get();
      // reset position
      longBuffer.position(0);
    }
  }

  private void ensureDoubleDictionaryLoaded()
  {
    if (doubleDictionary == null) {
      doubleDictionaryFile = makeTempFile(name + ColumnSerializerUtils.DOUBLE_DICTIONARY_FILE_NAME);
      doubleBuffer = SerializerUtils.mapSerializer(doubleDictionaryFile, doubleDictionaryWriter);
      doubleDictionary = FixedIndexed.read(
          doubleBuffer,
          TypeStrategies.DOUBLE,
          ByteOrder.nativeOrder(),
          Double.BYTES
      ).get();
      // reset position
      doubleBuffer.position(0);
    }
  }

  private void ensureArrayDictionaryLoaded()
  {
    if (arrayDictionary == null && arrayDictionaryWriter != null) {
      arrayDictionaryFile = makeTempFile(name + ColumnSerializerUtils.ARRAY_DICTIONARY_FILE_NAME);
      arrayBuffer = SerializerUtils.mapSerializer(arrayDictionaryFile, arrayDictionaryWriter);
      arrayDictionary = FrontCodedIntArrayIndexed.read(arrayBuffer, ByteOrder.nativeOrder()).get();
      // reset position
      arrayBuffer.position(0);
    }
  }

  private Path makeTempFile(String name)
  {
    try {
      return Files.createTempFile(tempBasePath, StringUtils.urlEncode(name), null);
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public int getStringCardinality()
  {
    ensureStringDictionaryLoaded();
    return stringDictionary == null ? 0 : stringDictionary.size();
  }

  public int getLongCardinality()
  {
    ensureLongDictionaryLoaded();
    return longDictionary == null ? 0 : longDictionary.size();
  }

  public int getDoubleCardinality()
  {
    ensureDoubleDictionaryLoaded();
    return doubleDictionary == null ? 0 : doubleDictionary.size();
  }

  public int getArrayCardinality()
  {
    ensureArrayDictionaryLoaded();
    return arrayDictionary == null ? 0 : arrayDictionary.size();
  }

  public static void deleteTempFile(Path path)
  {
    try {
      final File file = path.toFile();
      if (file.isDirectory()) {
        FileUtils.deleteDirectory(file);
      } else {
        Files.delete(path);
      }
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
