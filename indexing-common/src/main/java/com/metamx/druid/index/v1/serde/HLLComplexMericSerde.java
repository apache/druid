/*
 * Druid - a distributed column store.
 * Copyright (C) 2012  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package com.metamx.druid.index.v1.serde;

import com.metamx.druid.index.column.ColumnBuilder;
import com.metamx.druid.index.column.ValueType;
import com.metamx.druid.index.serde.ColumnPartSerde;
import com.metamx.druid.index.serde.ComplexColumnPartSerde;
import com.metamx.druid.index.serde.ComplexColumnPartSupplier;
import com.metamx.druid.index.v1.serde.ComplexMetricExtractor;
import com.metamx.druid.index.v1.serde.ComplexMetricSerde;
import com.metamx.druid.input.InputRow;
import com.metamx.druid.kv.GenericIndexed;
import com.metamx.druid.kv.ObjectStrategy;
import gnu.trove.map.hash.TIntByteHashMap;

import java.nio.ByteBuffer;
import java.util.List;

public class HLLComplexMericSerde extends ComplexMetricSerde
{
  @Override
  public String getTypeName()
  {
    return "hll";
  }

  @Override
  public ComplexMetricExtractor getExtractor()
  {
    return new HllComplexMetricExtractor();
  }

  @Override
  public ColumnPartSerde deserializeColumn(ByteBuffer buffer, ColumnBuilder builder)
  {
    GenericIndexed column = GenericIndexed.read(buffer, getObjectStrategy());
    builder.setType(ValueType.COMPLEX);
    builder.setComplexColumn(new ComplexColumnPartSupplier("hll", column));
    return new ComplexColumnPartSerde(column, "hll");
  }

  @Override
  public ObjectStrategy getObjectStrategy()
  {
    return new HllObjectStrategy();
  }

  public static class HllObjectStrategy implements ObjectStrategy<TIntByteHashMap>
  {
    @Override
    public Class<? extends TIntByteHashMap> getClazz()
    {
      return TIntByteHashMap.class;
    }

    @Override
    public TIntByteHashMap fromByteBuffer(ByteBuffer buffer, int numBytes)
    {
      int keylength = buffer.getInt();
      int valuelength = buffer.getInt();
      if (keylength == 0) {
        return new TIntByteHashMap();
      }
      int[] keys = new int[keylength];
      byte[] values = new byte[valuelength];

      for (int i = 0; i < keylength; i++) {
        keys[i] = buffer.getInt();
      }

      buffer.get(values);

      TIntByteHashMap tib = new TIntByteHashMap(keys, values);
      return tib;
    }

    @Override
    public byte[] toBytes(TIntByteHashMap val)
    {
      TIntByteHashMap ibmap = val;
      int[] indexesResult = ibmap.keys();
      byte[] valueResult = ibmap.values();
      ByteBuffer buffer = ByteBuffer.allocate(4 * indexesResult.length + valueResult.length + 8);
      byte[] result = new byte[4 * indexesResult.length + valueResult.length + 8];
      buffer.putInt((int) indexesResult.length);
      buffer.putInt((int) valueResult.length);
      for (int i = 0; i < indexesResult.length; i++) {
        buffer.putInt(indexesResult[i]);
      }

      buffer.put(valueResult);
      buffer.flip();
      buffer.get(result);
      return result;
    }

    @Override
    public int compare(TIntByteHashMap o1, TIntByteHashMap o2)
    {
      return o1.equals(o2) ? 0 : 1;
    }
  }

  public static class HllComplexMetricExtractor implements ComplexMetricExtractor
  {
    @Override
    public Class<?> extractedClass()
    {
      return List.class;
    }

    @Override
    public Object extractValue(InputRow inputRow, String metricName)
    {
      return inputRow.getRaw(metricName);
    }
  }
}

