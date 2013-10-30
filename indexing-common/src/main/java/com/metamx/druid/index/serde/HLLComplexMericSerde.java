package com.metamx.druid.index.serde;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import com.metamx.druid.aggregation.HllAggregator;
import com.metamx.druid.input.InputRow;
import com.metamx.druid.index.column.ColumnBuilder;
import com.metamx.druid.index.column.ValueType;
import com.metamx.druid.index.serde.ColumnPartSerde;
import com.metamx.druid.index.v1.serde.ComplexMetricExtractor;
import com.metamx.druid.index.v1.serde.ComplexMetricSerde;
import com.metamx.druid.kv.Indexed;
import com.metamx.druid.kv.ObjectStrategy;
import com.metamx.common.logger.Logger;
import com.metamx.druid.kv.GenericIndexed;
import com.metamx.druid.index.column.IndexedComplexColumn;
import com.metamx.druid.index.serde.ComplexColumnPartSupplier;
import gnu.trove.map.hash.TIntByteHashMap;
import gnu.trove.map.TIntByteMap;

public class HLLComplexMericSerde extends ComplexMetricSerde {
  private static final Logger log = new Logger(HLLComplexMericSerde.class);
  private static long desTime=0;
  private static long sTime = 0;
  

  @Override
  public String getTypeName() {
    return "hll";
  }

  @Override
  public ComplexMetricExtractor getExtractor() {
    return new HllComplexMetricExtractor();
  }

  @Override
  public ColumnPartSerde deserializeColumn(ByteBuffer buffer, ColumnBuilder builder) {
    GenericIndexed column = GenericIndexed.read(buffer, getObjectStrategy());
    builder.setType(ValueType.COMPLEX);
    builder.setComplexColumn(new ComplexColumnPartSupplier("hll", column));
    return new ComplexColumnPartSerde(column, "hll");
  }

  @Override
  public ObjectStrategy getObjectStrategy() {
    return new HllObjectStrategy<byte[]>();
  }

  public static class HllObjectStrategy<T> implements ObjectStrategy<T> {
    @Override
    public Class<? extends T> getClazz() {
      return (Class<? extends T>) TIntByteHashMap.class;
    }

    @Override
      public T fromByteBuffer(ByteBuffer buffer, int numBytes) {
        int keylength = buffer.getInt();
        int valuelength = buffer.getInt();
        if (keylength == 0) {
          return (T) (new TIntByteHashMap());
        }
        int[] keys = new int[keylength];
        byte[] values = new byte[valuelength];

        for(int i = 0 ; i<keylength; i++)
        {
          keys[i]=buffer.getInt();
        }

        buffer.get(values);

        TIntByteHashMap tib = new TIntByteHashMap(keys, values);
        return (T) tib;
      }

    @Override
    public byte[] toBytes(T val) {
      TIntByteHashMap ibmap = (TIntByteHashMap) val;
      int[] indexesResult = ibmap.keys();
      byte[] valueResult = ibmap.values();
      ByteBuffer buffer = ByteBuffer.allocate(4*indexesResult.length + valueResult.length + 8);
      byte[] result = new byte[4*indexesResult.length + valueResult.length + 8];
      buffer.putInt((int)indexesResult.length);
      buffer.putInt((int)valueResult.length);
      for(int i = 0 ; i<indexesResult.length; i++)
      {
        buffer.putInt(indexesResult[i]);
      }

      buffer.put(valueResult);
      buffer.flip();
      buffer.get(result);
      return result;
    }

    @Override
    public int compare(T o1, T o2) {
      if (((TIntByteHashMap) o1).equals((TIntByteHashMap) o2)) {
        return 0;
      } else {
        return 1;
      }
    }

    @Override
    public boolean equals(Object obj) {
      return this.equals(obj);
    }

  }

  public static class HllComplexMetricExtractor implements
    ComplexMetricExtractor {

      @Override
      public Class<?> extractedClass() {
        return List.class;
      }

      @Override
      public Object extractValue(InputRow inputRow, String metricName) {
        return inputRow.getDimension(metricName);
      }

    }
}

