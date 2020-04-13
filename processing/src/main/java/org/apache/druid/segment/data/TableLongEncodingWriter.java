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

import it.unimi.dsi.fastutil.longs.Long2IntMap;
import it.unimi.dsi.fastutil.longs.LongList;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.segment.writeout.WriteOutBytes;

import java.io.IOException;
import java.nio.ByteBuffer;

public class TableLongEncodingWriter implements CompressionFactory.LongEncodingWriter
{

  private final Long2IntMap table;
  private final LongList valueAddedInOrder;
  private final int bitsPerValue;
  private VSizeLongSerde.LongSerializer serializer;

  public TableLongEncodingWriter(Long2IntMap table, LongList valueAddedInOrder)
  {
    if (table.size() > CompressionFactory.MAX_TABLE_SIZE) {
      throw new IAE("Invalid table size[%s]", table.size());
    }
    this.table = table;
    this.valueAddedInOrder = valueAddedInOrder;
    this.bitsPerValue = VSizeLongSerde.getBitsForMax(table.size());
  }

  @Override
  public void setBuffer(ByteBuffer buffer)
  {
    serializer = VSizeLongSerde.getSerializer(bitsPerValue, buffer, buffer.position());
  }

  @Override
  public void setOutputStream(WriteOutBytes output)
  {
    serializer = VSizeLongSerde.getSerializer(bitsPerValue, output);
  }

  @Override
  public void write(long value) throws IOException
  {
    serializer.write(table.get(value));
  }

  @Override
  public void flush() throws IOException
  {
    if (serializer != null) {
      serializer.close();
    }
  }

  @Override
  public void putMeta(ByteBuffer metaOut, CompressionStrategy strategy)
  {
    metaOut.put(CompressionFactory.setEncodingFlag(strategy.getId()));
    metaOut.put(CompressionFactory.LongEncodingFormat.TABLE.getId());
    metaOut.put(CompressionFactory.TABLE_ENCODING_VERSION);
    metaOut.putInt(table.size());
    for (int i = 0; i < valueAddedInOrder.size(); i++) {
      metaOut.putLong(valueAddedInOrder.getLong(i));
    }
  }

  @Override
  public int metaSize()
  {
    return 1 + 1 + 1 + Integer.BYTES + (table.size() * Long.BYTES);
  }

  @Override
  public int getBlockSize(int bytesPerBlock)
  {
    return VSizeLongSerde.getNumValuesPerBlock(bitsPerValue, bytesPerBlock);
  }

  @Override
  public int getNumBytes(int values)
  {
    return VSizeLongSerde.getSerializedSize(bitsPerValue, values);
  }
}
