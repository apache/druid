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

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.druid.java.util.common.io.smoosh.FileSmoosher;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.data.VByte;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.WritableByteChannel;

public class ConstantColumnSerializer extends NestedCommonFormatColumnSerializer
{
  private int rowCount = 0;
  private final String name;
  @Nullable
  private final Object constantValue;

  private boolean closedForWrite = false;
  private ByteBuffer columnNameBytes = null;
  private ByteBuffer rowCountBytes = null;
  private ByteBuffer constantValueBytes = null;

  public ConstantColumnSerializer(String name, @Nullable Object constantValue)
  {
    this.name = name;
    this.constantValue = constantValue;
  }

  @Override
  public String getColumnName()
  {
    return name;
  }

  @Override
  public boolean hasNulls()
  {
    return constantValue == null;
  }

  @Override
  public void serialize(ColumnValueSelector<? extends StructuredData> selector) throws IOException
  {
    rowCount++;
  }

  @Override
  public void open() throws IOException
  {
    // nothing to open
  }

  @Override
  public void openDictionaryWriter() throws IOException
  {
    // no dictionary
  }

  @Override
  public void serializeDictionaries(
      Iterable<String> strings,
      Iterable<Long> longs,
      Iterable<Double> doubles,
      Iterable<int[]> arrays
  ) throws IOException
  {
    // no-op
  }

  @Override
  public DictionaryIdLookup getGlobalLookup()
  {
    return new DictionaryIdLookup();
  }

  private void closeForWrite()
  {
    if (!closedForWrite) {
      columnNameBytes = computeFilenameBytes();

      final int rowCountSize = VByte.computeIntSize(rowCount);
      rowCountBytes = ByteBuffer.allocate(rowCountSize).order(ByteOrder.nativeOrder());
      VByte.writeInt(rowCountBytes, rowCount);
      rowCountBytes.flip();
      try {
        final byte[] valueBytes;
        valueBytes = NestedDataComplexTypeSerde.OBJECT_MAPPER.writeValueAsBytes(constantValue);
        final int constantValueLength = VByte.computeIntSize(valueBytes.length);
        constantValueBytes = ByteBuffer.allocate(constantValueLength + valueBytes.length).order(ByteOrder.nativeOrder());
        VByte.writeInt(constantValueBytes, valueBytes.length);
        constantValueBytes.put(valueBytes);
        constantValueBytes.flip();
      }
      catch (JsonProcessingException e) {
        throw new RuntimeException(e);
      }
    }
  }
  @Override
  public long getSerializedSize() throws IOException
  {
    closeForWrite();
    return 1 + columnNameBytes.capacity() + rowCountBytes.capacity() + constantValueBytes.capacity();
  }

  @Override
  public void writeTo(WritableByteChannel channel, FileSmoosher smoosher) throws IOException
  {
    closeForWrite();

    writeV0Header(channel, columnNameBytes);
    channel.write(rowCountBytes);
    channel.write(constantValueBytes);
  }
}
