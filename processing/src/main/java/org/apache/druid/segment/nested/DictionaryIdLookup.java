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

import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.io.smoosh.SmooshedFileMapper;
import org.apache.druid.segment.column.StringEncodingStrategies;
import org.apache.druid.segment.column.TypeStrategies;
import org.apache.druid.segment.data.DictionaryWriter;
import org.apache.druid.segment.data.FixedIndexed;
import org.apache.druid.segment.data.FrontCodedIntArrayIndexed;
import org.apache.druid.segment.data.Indexed;
import org.apache.druid.segment.serde.ColumnSerializerUtils;
import org.apache.druid.utils.CloseableUtils;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Value to dictionary id lookup, backed with memory mapped dictionaries populated lazily by the supplied
 * {@link DictionaryWriter}.
 */
public final class DictionaryIdLookup implements Closeable
{

  private final String name;
  private final File tempBasePath;

  @Nullable
  private final DictionaryWriter<String> stringDictionaryWriter;
  private File stringDictionaryFile = null;
  private SmooshedFileMapper stringBufferMapper = null;
  private Indexed<ByteBuffer> stringDictionary = null;

  @Nullable
  private final DictionaryWriter<Long> longDictionaryWriter;
  private File longDictionaryFile = null;
  private SmooshedFileMapper longBufferMapper = null;
  private FixedIndexed<Long> longDictionary = null;

  @Nullable
  private final DictionaryWriter<Double> doubleDictionaryWriter;
  private File doubleDictionaryFile = null;
  SmooshedFileMapper doubleBufferMapper = null;
  FixedIndexed<Double> doubleDictionary = null;

  @Nullable
  private final DictionaryWriter<int[]> arrayDictionaryWriter;
  private File arrayDictionaryFile = null;
  private SmooshedFileMapper arrayBufferMapper = null;
  private FrontCodedIntArrayIndexed arrayDictionary = null;
  private final Closer closer = Closer.create();

  public DictionaryIdLookup(
      String name,
      File tempBaseDir,
      @Nullable DictionaryWriter<String> stringDictionaryWriter,
      @Nullable DictionaryWriter<Long> longDictionaryWriter,
      @Nullable DictionaryWriter<Double> doubleDictionaryWriter,
      @Nullable DictionaryWriter<int[]> arrayDictionaryWriter
  )
  {
    this.name = name;
    this.tempBasePath = tempBaseDir;
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
  public SmooshedFileMapper getLongBufferMapper()
  {
    return longBufferMapper;
  }

  @Nullable
  public SmooshedFileMapper getDoubleBufferMapper()
  {
    return doubleBufferMapper;
  }

  @Nullable
  public SmooshedFileMapper getArrayBufferMapper()
  {
    return arrayBufferMapper;
  }

  @Override
  public void close()
  {
    CloseableUtils.closeAndWrapExceptions(closer);
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
      final String fileName = ColumnSerializerUtils.getInternalFileName(
          name,
          ColumnSerializerUtils.STRING_DICTIONARY_FILE_NAME
      );
      stringDictionaryFile = makeTempDir(fileName);
      stringBufferMapper = closer.register(
          ColumnSerializerUtils.mapSerializer(stringDictionaryFile, stringDictionaryWriter, fileName)
      );

      try {
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
      final String fileName = ColumnSerializerUtils.getInternalFileName(
          name,
          ColumnSerializerUtils.LONG_DICTIONARY_FILE_NAME
      );
      longDictionaryFile = makeTempDir(fileName);
      longBufferMapper = closer.register(
          ColumnSerializerUtils.mapSerializer(longDictionaryFile, longDictionaryWriter, fileName)
      );
      try {
        final ByteBuffer buffer = longBufferMapper.mapFile(fileName);
        longDictionary = FixedIndexed.read(buffer, TypeStrategies.LONG, ByteOrder.nativeOrder(), Long.BYTES).get();
      }
      catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  private void ensureDoubleDictionaryLoaded()
  {
    if (doubleDictionary == null) {
      final String fileName = ColumnSerializerUtils.getInternalFileName(
          name,
          ColumnSerializerUtils.DOUBLE_DICTIONARY_FILE_NAME
      );
      doubleDictionaryFile = makeTempDir(fileName);
      doubleBufferMapper = closer.register(
          ColumnSerializerUtils.mapSerializer(doubleDictionaryFile, doubleDictionaryWriter, fileName)
      );
      try {
        final ByteBuffer buffer = doubleBufferMapper.mapFile(fileName);
        doubleDictionary = FixedIndexed.read(buffer, TypeStrategies.DOUBLE, ByteOrder.nativeOrder(), Long.BYTES).get();
      }
      catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  private void ensureArrayDictionaryLoaded()
  {
    if (arrayDictionary == null && arrayDictionaryWriter != null) {
      final String fileName = ColumnSerializerUtils.getInternalFileName(
          name,
          ColumnSerializerUtils.ARRAY_DICTIONARY_FILE_NAME
      );
      arrayDictionaryFile = makeTempDir(fileName);
      arrayBufferMapper = closer.register(
          ColumnSerializerUtils.mapSerializer(arrayDictionaryFile, arrayDictionaryWriter, fileName)
      );
      try {
        final ByteBuffer buffer = arrayBufferMapper.mapFile(fileName);
        arrayDictionary = FrontCodedIntArrayIndexed.read(buffer, ByteOrder.nativeOrder()).get();
      }
      catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  private File makeTempDir(String fileName)
  {
    try {
      final File f = new File(tempBasePath, StringUtils.urlEncode(fileName));
      FileUtils.mkdirp(f);
      closer.register(() -> FileUtils.deleteDirectory(f));
      return f;
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
}
