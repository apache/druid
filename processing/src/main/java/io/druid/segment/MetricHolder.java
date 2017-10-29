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

package io.druid.segment;

import com.google.common.io.ByteSink;
import com.google.common.io.ByteStreams;
import com.google.common.io.InputSupplier;
import com.google.common.io.OutputSupplier;
import io.druid.common.utils.SerializerUtils;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.io.smoosh.SmooshedFileMapper;
import io.druid.segment.data.CompressedDoublesIndexedSupplier;
import io.druid.segment.data.CompressedFloatsIndexedSupplier;
import io.druid.segment.data.CompressedLongsIndexedSupplier;
import io.druid.segment.data.DoubleSupplierSerializer;
import io.druid.segment.data.FloatSupplierSerializer;
import io.druid.segment.data.GenericIndexed;
import io.druid.segment.data.GenericIndexedWriter;
import io.druid.segment.data.Indexed;
import io.druid.segment.data.IndexedDoubles;
import io.druid.segment.data.IndexedFloats;
import io.druid.segment.data.IndexedLongs;
import io.druid.segment.data.LongSupplierSerializer;
import io.druid.segment.data.ObjectStrategy;
import io.druid.segment.serde.ComplexMetricSerde;
import io.druid.segment.serde.ComplexMetrics;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 */
public class MetricHolder
{
  private static final byte[] version = new byte[]{0x0};
  private static final SerializerUtils serializerUtils = new SerializerUtils();

  public static MetricHolder floatMetric(String name, CompressedFloatsIndexedSupplier column)
  {
    MetricHolder retVal = new MetricHolder(name, "float");
    retVal.floatType = column;
    return retVal;
  }

  public static MetricHolder complexMetric(String name, String typeName, Indexed column)
  {
    MetricHolder retVal = new MetricHolder(name, typeName);
    retVal.complexType = column;
    return retVal;
  }

  public static void writeComplexMetric(
      OutputSupplier<? extends OutputStream> outSupplier, String name, String typeName, GenericIndexedWriter column
  ) throws IOException
  {
    try (OutputStream out = outSupplier.getOutput()) {
      out.write(version);
      serializerUtils.writeString(out, name);
      serializerUtils.writeString(out, typeName);
      final InputSupplier<InputStream> supplier = column.combineStreams();
      try (InputStream in = supplier.getInput()) {
        ByteStreams.copy(in, out);
      }
    }
  }

  public static void writeFloatMetric(
      final ByteSink outSupplier, String name, FloatSupplierSerializer column
  ) throws IOException
  {
    outSupplier.write(version);
    serializerUtils.writeString(toOutputSupplier(outSupplier), name);
    serializerUtils.writeString(toOutputSupplier(outSupplier), "float");
    column.closeAndConsolidate(outSupplier);
  }

  public static void writeLongMetric(
      ByteSink outSupplier, String name, LongSupplierSerializer column
  ) throws IOException
  {
    outSupplier.write(version);
    serializerUtils.writeString(toOutputSupplier(outSupplier), name);
    serializerUtils.writeString(toOutputSupplier(outSupplier), "long");
    column.closeAndConsolidate(outSupplier);
  }

  public static void writeDoubleMetric(ByteSink outSupplier, String name, DoubleSupplierSerializer column
  ) throws IOException
  {
    outSupplier.write(version);
    serializerUtils.writeString(toOutputSupplier(outSupplier), name);
    serializerUtils.writeString(toOutputSupplier(outSupplier), "double");
    column.closeAndConsolidate(outSupplier);
  }


  public static MetricHolder fromByteBuffer(ByteBuffer buf, SmooshedFileMapper mapper) throws IOException
  {
    return fromByteBuffer(buf, null, mapper);
  }

  public static MetricHolder fromByteBuffer(ByteBuffer buf, ObjectStrategy strategy, SmooshedFileMapper mapper)
      throws IOException
  {
    final byte ver = buf.get();
    if (version[0] != ver) {
      throw new ISE("Unknown version[%s] of MetricHolder", ver);
    }

    final String metricName = serializerUtils.readString(buf);
    final String typeName = serializerUtils.readString(buf);
    MetricHolder holder = new MetricHolder(metricName, typeName);

    switch (holder.type) {
      case LONG:
        holder.longType = CompressedLongsIndexedSupplier.fromByteBuffer(buf, ByteOrder.nativeOrder(), mapper);
        break;
      case FLOAT:
        holder.floatType = CompressedFloatsIndexedSupplier.fromByteBuffer(buf, ByteOrder.nativeOrder(), mapper);
        break;
      case DOUBLE:
        holder.doubleType = CompressedDoublesIndexedSupplier.fromByteBuffer(buf, ByteOrder.nativeOrder(), mapper);
        break;
      case COMPLEX:
        if (strategy != null) {
          holder.complexType = GenericIndexed.read(buf, strategy, mapper);
        } else {
          final ComplexMetricSerde serdeForType = ComplexMetrics.getSerdeForType(holder.getTypeName());

          if (serdeForType == null) {
            throw new ISE("Unknown type[%s], cannot load.", holder.getTypeName());
          }

          holder.complexType = GenericIndexed.read(buf, serdeForType.getObjectStrategy());
        }
        break;
    }

    return holder;
  }

  // This is only for guava14 compat. Eventually it should be able to be removed.
  private static OutputSupplier<? extends OutputStream> toOutputSupplier(final ByteSink sink)
  {
    return new OutputSupplier<OutputStream>()
    {
      @Override
      public OutputStream getOutput() throws IOException
      {
        return sink.openStream();
      }
    };
  }

  private final String name;
  private final String typeName;
  private final MetricType type;

  public enum MetricType
  {
    LONG,
    FLOAT,
    DOUBLE,
    COMPLEX;

    static MetricType determineType(String typeName)
    {
      if ("long".equalsIgnoreCase(typeName)) {
        return LONG;
      } else if ("float".equalsIgnoreCase(typeName)) {
        return FLOAT;
      } else if ("double".equalsIgnoreCase(typeName)) {
        return DOUBLE;
      }
      return COMPLEX;
    }
  }

  CompressedLongsIndexedSupplier longType = null;
  CompressedFloatsIndexedSupplier floatType = null;
  CompressedDoublesIndexedSupplier doubleType = null;
  Indexed complexType = null;

  private MetricHolder(
      String name,
      String typeName
  )
  {
    this.name = name;
    this.typeName = typeName;
    this.type = MetricType.determineType(typeName);
  }

  public String getName()
  {
    return name;
  }

  public String getTypeName()
  {
    return typeName;
  }

  public MetricType getType()
  {
    return type;
  }

  public IndexedLongs getLongType()
  {
    assertType(MetricType.LONG);
    return longType.get();
  }

  public IndexedFloats getFloatType()
  {
    assertType(MetricType.FLOAT);
    return floatType.get();
  }

  public IndexedDoubles getDoubleType()
  {
    assertType(MetricType.DOUBLE);
    return doubleType.get();
  }

  public Indexed getComplexType()
  {
    assertType(MetricType.COMPLEX);
    return complexType;
  }

  private void assertType(MetricType type)
  {
    if (this.type != type) {
      throw new IAE("type[%s] cannot be cast to [%s]", typeName, type);
    }
  }
}
