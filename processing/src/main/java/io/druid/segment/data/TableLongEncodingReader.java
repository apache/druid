/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.segment.data;

import io.druid.java.util.common.IAE;
import io.druid.java.util.common.IOE;
import io.druid.segment.store.IndexInput;

import java.io.IOException;
import java.nio.ByteBuffer;

public class TableLongEncodingReader implements CompressionFactory.LongEncodingReader
{
  private final long table[];
  private final int bitsPerValue;
  private final ByteBuffer buffer;
  private VSizeLongSerde.LongDeserializer deserializer;

  private final IndexInput indexInput;
  private final boolean isIIVersion;

  public TableLongEncodingReader(ByteBuffer fromBuffer)
  {
    this.buffer = fromBuffer.asReadOnlyBuffer();
    byte version = buffer.get();
    this.indexInput = null;
    this.isIIVersion = false;
    if (version == CompressionFactory.TABLE_ENCODING_VERSION) {
      int tableSize = buffer.getInt();
      if (tableSize < 0 || tableSize > CompressionFactory.MAX_TABLE_SIZE) {
        throw new IAE("Invalid table size[%s]", tableSize);
      }
      bitsPerValue = VSizeLongSerde.getBitsForMax(tableSize);
      table = new long[tableSize];
      for (int i = 0; i < tableSize; i++) {
        table[i] = buffer.getLong();
      }
      fromBuffer.position(buffer.position());
      deserializer = VSizeLongSerde.getDeserializer(bitsPerValue, buffer, buffer.position());
    } else {
      throw new IAE("Unknown version[%s]", version);
    }
  }

  public TableLongEncodingReader(IndexInput fromIndexInput)
  {
    try {
      this.indexInput = fromIndexInput.duplicate();
      this.buffer = null;
      this.isIIVersion = true;
      byte version = indexInput.readByte();
      if (version == CompressionFactory.TABLE_ENCODING_VERSION) {
        int tableSize = indexInput.readInt();
        if (tableSize < 0 || tableSize > CompressionFactory.MAX_TABLE_SIZE) {
          throw new IAE("Invalid table size[%s]", tableSize);
        }
        bitsPerValue = VSizeLongSerde.getBitsForMax(tableSize);
        table = new long[tableSize];
        for (int i = 0; i < tableSize; i++) {
          table[i] = indexInput.readLong();
        }
        fromIndexInput.seek(indexInput.getFilePointer());
        deserializer = VSizeLongSerde.getDeserializer(bitsPerValue, indexInput, (int) indexInput.getFilePointer());
      } else {
        throw new IAE("Unknown version[%s]", version);
      }
    }
    catch (IOException e) {
      throw new IOE(e);
    }
  }

  private TableLongEncodingReader(ByteBuffer buffer, long table[], int bitsPerValue)
  {
    this.buffer = buffer;
    this.table = table;
    this.bitsPerValue = bitsPerValue;
    deserializer = VSizeLongSerde.getDeserializer(bitsPerValue, buffer, buffer.position());
    this.indexInput = null;
    this.isIIVersion = false;
  }

  private TableLongEncodingReader(IndexInput indexInput, long table[], int bitsPerValue)
  {
    try {
      this.indexInput = indexInput;
      this.isIIVersion = true;
      this.buffer = null;
      this.table = table;
      this.bitsPerValue = bitsPerValue;
      deserializer = VSizeLongSerde.getDeserializer(bitsPerValue, indexInput, (int) indexInput.getFilePointer());
    }
    catch (IOException e) {
      throw new IOE(e);
    }
  }

  @Override
  public void setIndexInput(IndexInput indexInput)
  {
    try {
      deserializer = VSizeLongSerde.getDeserializer(bitsPerValue, indexInput, (int) indexInput.getFilePointer());
    }
    catch (IOException e) {
      throw new IOE(e);
    }
  }

  @Override
  public void setBuffer(ByteBuffer buffer)
  {
    deserializer = VSizeLongSerde.getDeserializer(bitsPerValue, buffer, buffer.position());
  }

  @Override
  public long read(int index)
  {
    return table[(int) deserializer.get(index)];
  }

  @Override
  public int getNumBytes(int values)
  {
    return VSizeLongSerde.getSerializedSize(bitsPerValue, values);
  }

  @Override
  public CompressionFactory.LongEncodingReader duplicate()
  {
    if (!isIIVersion) {
      return new TableLongEncodingReader(buffer.duplicate(), table, bitsPerValue);
    } else {
      try {
        return new TableLongEncodingReader(indexInput.duplicate(), table, bitsPerValue);
      }
      catch (IOException e) {
        throw new IOE(e);
      }
    }
  }
}
