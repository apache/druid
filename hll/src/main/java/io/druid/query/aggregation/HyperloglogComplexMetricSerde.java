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

package io.druid.query.aggregation;

import gnu.trove.map.hash.TIntByteHashMap;
import io.druid.data.input.InputRow;
import io.druid.segment.column.ColumnBuilder;
import io.druid.segment.column.ValueType;
import io.druid.segment.data.GenericIndexed;
import io.druid.segment.data.ObjectStrategy;
import io.druid.segment.serde.ColumnPartSerde;
import io.druid.segment.serde.ComplexColumnPartSerde;
import io.druid.segment.serde.ComplexColumnPartSupplier;
import io.druid.segment.serde.ComplexMetricExtractor;
import io.druid.segment.serde.ComplexMetricSerde;

import java.nio.ByteBuffer;
import java.util.List;

public class HyperloglogComplexMetricSerde extends ComplexMetricSerde
{
  @Override
  public String getTypeName()
  {
    return "hyperloglog";
  }

  @Override
  public ComplexMetricExtractor getExtractor()
  {
    return new HyperloglogComplexMetricExtractor();
  }

  @Override
  public ColumnPartSerde deserializeColumn(ByteBuffer buffer, ColumnBuilder builder)
  {
    GenericIndexed column = GenericIndexed.read(buffer, getObjectStrategy());
    builder.setType(ValueType.COMPLEX);
    builder.setComplexColumn(new ComplexColumnPartSupplier("hyperloglog", column));
    return new ComplexColumnPartSerde(column, "hyperloglog");
  }

  @Override
  public ObjectStrategy getObjectStrategy()
  {
    return new HyperloglogObjectStrategy();
  }

  public static class HyperloglogObjectStrategy implements ObjectStrategy<TIntByteHashMap>
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

  public static class HyperloglogComplexMetricExtractor implements ComplexMetricExtractor
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

