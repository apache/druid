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

package io.druid.query.aggregation.unique;

import io.druid.data.input.InputRow;
import io.druid.java.util.common.io.smoosh.FileSmoosher;
import io.druid.segment.GenericColumnSerializer;
import io.druid.segment.IndexIO;
import io.druid.segment.column.ColumnBuilder;
import io.druid.segment.column.ComplexColumn;
import io.druid.segment.data.ColumnarMultiInts;
import io.druid.segment.data.CompressionStrategy;
import io.druid.segment.data.IndexedInts;
import io.druid.segment.data.ObjectStrategy;
import io.druid.segment.data.V3CompressedVSizeColumnarMultiIntsSerializer;
import io.druid.segment.data.V3CompressedVSizeColumnarMultiIntsSupplier;
import io.druid.segment.serde.ComplexMetricExtractor;
import io.druid.segment.serde.ComplexMetricSerde;
import io.druid.segment.writeout.SegmentWriteOutMedium;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.List;

public class RoaringBitmapComplexMetricSerde extends ComplexMetricSerde
{

  @Override
  public String getTypeName()
  {
    return "ImmutableRoaringBitmap";
  }

  @Override
  public ComplexMetricExtractor getExtractor()
  {
    return new ComplexMetricExtractor()
    {
      @Override
      public Class<ImmutableRoaringBitmap> extractedClass()
      {
        return ImmutableRoaringBitmap.class;
      }

      @Nullable
      @Override
      public ImmutableRoaringBitmap extractValue(InputRow inputRow, String metricName)
      {
        final Object rawValue = inputRow.getRaw(metricName);
        if (rawValue == null) {
          return null;
        } else if (rawValue instanceof ImmutableRoaringBitmap) {
          return (ImmutableRoaringBitmap) rawValue;
        } else {
          MutableRoaringBitmap bitmap = new MutableRoaringBitmap();
          List<String> dimValues = inputRow.getDimension(metricName);
          if (dimValues == null) {
            return bitmap;
          }
          for (String dimensionValue : dimValues) {
            bitmap.add(Integer.valueOf(dimensionValue));
          }
          return bitmap;
        }
      }
    };
  }

  @Override
  public void deserializeColumn(ByteBuffer buffer, ColumnBuilder builder)
  {

    builder.setComplexColumn(() -> {
      final int position = buffer.position();
      final int limit = buffer.limit();
      ColumnarMultiInts indexedInts = V3CompressedVSizeColumnarMultiIntsSupplier.fromByteBuffer(
          buffer,
          // TODO byteorder 必须和压缩的相同
          IndexIO.BYTE_ORDER
      ).get();
      return new ComplexColumn()
      {
        @Override
        public Class<?> getClazz()
        {
          return indexedInts.getClazz();
        }

        @Override
        public String getTypeName()
        {
          return "ImmutableRoaringBitmap";
        }

        @Override
        public Object getRowValue(int rowNum)
        {
          return indexedInts.get(rowNum);
        }

        @Override
        public void close()
        {
          buffer.position(position);
          buffer.limit(limit);
        }
      };
    });

  }

  @Override
  public ObjectStrategy getObjectStrategy()
  {
    return ImmutableRoaringBitmapObjectStrategy.STRATEGY;
  }


  @Override
  public GenericColumnSerializer getSerializer(SegmentWriteOutMedium segmentWriteOutMedium, String column)
  {
    return new GenericColumnSerializer()
    {
      V3CompressedVSizeColumnarMultiIntsSerializer serializer = V3CompressedVSizeColumnarMultiIntsSerializer.create(
          segmentWriteOutMedium,
          "%s.complex_column",
          Integer.MAX_VALUE,
          CompressionStrategy.fromString(System.getProperty("druid.uniq.compress", "UNCOMPRESSED"))
      );


      @Override
      public void open() throws IOException
      {
        serializer.open();
      }

      @Override
      public void serialize(Object obj) throws IOException
      {
        if (obj instanceof ImmutableRoaringBitmap) {
          serializer.add(((ImmutableRoaringBitmap) obj).toArray());
        } else if (obj instanceof IndexedInts) {
          IndexedInts indexedInts = (IndexedInts) obj;
          final int size = indexedInts.size();
          final int[] ints = new int[size];
          for (int i = 0; i < size; i++) {
            ints[i] = indexedInts.get(i);
          }
          serializer.add(ints);
        } else {
          throw new RuntimeException("Unexcept type: " + obj.getClass());
        }
      }

      @Override
      public long getSerializedSize() throws IOException
      {
        return serializer.getSerializedSize();
      }

      @Override
      public void writeTo(WritableByteChannel channel, FileSmoosher smoosher) throws IOException
      {
        serializer.writeTo(channel, smoosher);
      }
    };
  }
}
