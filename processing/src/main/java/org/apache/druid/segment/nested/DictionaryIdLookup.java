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
import org.apache.druid.annotations.SuppressFBWarnings;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.ByteBufferUtils;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.ISE;
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

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.GatheringByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.EnumSet;

/**
 * Value to dictionary id lookup, backed with memory mapped dictionaries populated lazily by the supplied
 * @link DictionaryWriter}.
 */
public final class DictionaryIdLookup implements Closeable
{
  private final String name;
  @Nullable
  private final DictionaryWriter<String> stringDictionaryWriter;
  private SmooshedFileMapper stringBufferMapper = null;
  private Indexed<ByteBuffer> stringDictionary = null;

  @Nullable
  private final DictionaryWriter<Long> longDictionaryWriter;
  private MappedByteBuffer longBuffer = null;
  private FixedIndexed<Long> longDictionary = null;

  @Nullable
  private final DictionaryWriter<Double> doubleDictionaryWriter;
  MappedByteBuffer doubleBuffer = null;
  FixedIndexed<Double> doubleDictionary = null;

  @Nullable
  private final DictionaryWriter<int[]> arrayDictionaryWriter;
  private MappedByteBuffer arrayBuffer = null;
  private FrontCodedIntArrayIndexed arrayDictionary = null;

  public DictionaryIdLookup(
      String name,
      @Nullable DictionaryWriter<String> stringDictionaryWriter,
      @Nullable DictionaryWriter<Long> longDictionaryWriter,
      @Nullable DictionaryWriter<Double> doubleDictionaryWriter,
      @Nullable DictionaryWriter<int[]> arrayDictionaryWriter
  )
  {
    this.name = name;
    this.stringDictionaryWriter = stringDictionaryWriter;
    this.longDictionaryWriter = longDictionaryWriter;
    this.doubleDictionaryWriter = doubleDictionaryWriter;
    this.arrayDictionaryWriter = arrayDictionaryWriter;
  }

  public int lookupString(@Nullable String value)
  {
    if (stringDictionary == null) {
      // GenericIndexed v2 can write to multiple files if the dictionary is larger than 2gb, so we use a smooshfile
      // for strings because of this. if other type dictionary writers could potentially use multiple internal files
      // in the future, we should transition them to using this approach as well (or build a combination smoosher and
      // mapper so that we can have a mutable smoosh)
      File stringSmoosh = FileUtils.createTempDir(name + "__stringTempSmoosh");
      final String fileName = NestedCommonFormatColumnSerializer.getInternalFileName(
          name,
          NestedCommonFormatColumnSerializer.STRING_DICTIONARY_FILE_NAME
      );
      final FileSmoosher smoosher = new FileSmoosher(stringSmoosh);
      try (final SmooshedWriter writer = smoosher.addWithSmooshedWriter(
          fileName,
          stringDictionaryWriter.getSerializedSize()
      )) {
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
    final byte[] bytes = StringUtils.toUtf8Nullable(value);
    final int index = stringDictionary.indexOf(bytes == null ? null : ByteBuffer.wrap(bytes));
    if (index < 0) {
      throw DruidException.defensive("Value not found in string dictionary");
    }
    return index;
  }

  public int lookupLong(@Nullable Long value)
  {
    if (longDictionary == null) {
      Path longFile = makeTempFile(name + NestedCommonFormatColumnSerializer.LONG_DICTIONARY_FILE_NAME);
      longBuffer = mapWriter(longFile, longDictionaryWriter);
      longDictionary = FixedIndexed.read(longBuffer, TypeStrategies.LONG, ByteOrder.nativeOrder(), Long.BYTES).get();
      // reset position
      longBuffer.position(0);
    }
    final int index = longDictionary.indexOf(value);
    if (index < 0) {
      throw DruidException.defensive("Value not found in long dictionary");
    }
    return index + longOffset();
  }

  public int lookupDouble(@Nullable Double value)
  {
    if (doubleDictionary == null) {
      Path doubleFile = makeTempFile(name + NestedCommonFormatColumnSerializer.DOUBLE_DICTIONARY_FILE_NAME);
      doubleBuffer = mapWriter(doubleFile, doubleDictionaryWriter);
      doubleDictionary = FixedIndexed.read(doubleBuffer, TypeStrategies.DOUBLE, ByteOrder.nativeOrder(), Double.BYTES).get();
      // reset position
      doubleBuffer.position(0);
    }
    final int index = doubleDictionary.indexOf(value);
    if (index < 0) {
      throw DruidException.defensive("Value not found in double dictionary");
    }
    return index + doubleOffset();
  }

  public int lookupArray(@Nullable int[] value)
  {
    if (arrayDictionary == null) {
      Path arrayFile = makeTempFile(name + NestedCommonFormatColumnSerializer.ARRAY_DICTIONARY_FILE_NAME);
      arrayBuffer = mapWriter(arrayFile, arrayDictionaryWriter);
      arrayDictionary = FrontCodedIntArrayIndexed.read(arrayBuffer, ByteOrder.nativeOrder()).get();
      // reset position
      arrayBuffer.position(0);
    }
    final int index = arrayDictionary.indexOf(value);
    if (index < 0) {
      throw DruidException.defensive("Value not found in array dictionary");
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
    }
    if (longBuffer != null) {
      ByteBufferUtils.unmap(longBuffer);
    }
    if (doubleBuffer != null) {
      ByteBufferUtils.unmap(doubleBuffer);
    }
    if (arrayBuffer != null) {
      ByteBufferUtils.unmap(arrayBuffer);
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

  private Path makeTempFile(String name)
  {
    try {
      return Files.createTempFile(name, ".tmp");
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @SuppressFBWarnings("NP_NONNULL_PARAM_VIOLATION")
  private MappedByteBuffer mapWriter(Path path, DictionaryWriter<?> writer)
  {
    final EnumSet<StandardOpenOption> options = EnumSet.of(
        StandardOpenOption.READ,
        StandardOpenOption.WRITE,
        StandardOpenOption.CREATE,
        StandardOpenOption.TRUNCATE_EXISTING
    );

    try (FileChannel fileChannel = FileChannel.open(path, options);
         GatheringByteChannel smooshChannel = makeWriter(fileChannel, writer.getSerializedSize())) {
      //noinspection DataFlowIssue
      writer.writeTo(smooshChannel, null);
      return fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, writer.getSerializedSize());
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private GatheringByteChannel makeWriter(FileChannel channel, long size)
  {
    // basically same code as smooshed writer, can't use channel directly because copying between channels
    // doesn't handle size of source channel correctly
    return new GatheringByteChannel()
    {
      private boolean isClosed = false;
      private long currOffset = 0;

      @Override
      public boolean isOpen()
      {
        return !isClosed;
      }

      @Override
      public void close() throws IOException
      {
        channel.close();
        isClosed = true;
      }

      public int bytesLeft()
      {
        return (int) (size - currOffset);
      }

      @Override
      public int write(ByteBuffer buffer) throws IOException
      {
        return addToOffset(channel.write(buffer));
      }

      @Override
      public long write(ByteBuffer[] srcs, int offset, int length) throws IOException
      {
        return addToOffset(channel.write(srcs, offset, length));
      }

      @Override
      public long write(ByteBuffer[] srcs) throws IOException
      {
        return addToOffset(channel.write(srcs));
      }

      public int addToOffset(long numBytesWritten)
      {
        if (numBytesWritten > bytesLeft()) {
          throw new ISE("Wrote more bytes[%,d] than available[%,d]. Don't do that.", numBytesWritten, bytesLeft());
        }
        currOffset += numBytesWritten;

        return Ints.checkedCast(numBytesWritten);
      }
    };
  }
}
