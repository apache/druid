/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
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

package io.druid.segment;

import com.google.common.io.ByteStreams;
import com.google.common.io.InputSupplier;
import com.google.common.io.OutputSupplier;
import com.metamx.common.IAE;
import com.metamx.common.ISE;
import com.metamx.common.guava.CloseQuietly;
import io.druid.common.utils.SerializerUtils;
import io.druid.segment.data.CompressedFloatsIndexedSupplier;
import io.druid.segment.data.CompressedFloatsSupplierSerializer;
import io.druid.segment.data.GenericIndexed;
import io.druid.segment.data.GenericIndexedWriter;
import io.druid.segment.data.Indexed;
import io.druid.segment.data.IndexedFloats;
import io.druid.segment.data.ObjectStrategy;
import io.druid.segment.serde.ComplexMetricSerde;
import io.druid.segment.serde.ComplexMetrics;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.WritableByteChannel;

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
    OutputStream out = null;
    InputStream in = null;

    try {
      out = outSupplier.getOutput();

      out.write(version);
      serializerUtils.writeString(out, name);
      serializerUtils.writeString(out, typeName);

      final InputSupplier<InputStream> supplier = column.combineStreams();
      in = supplier.getInput();

      ByteStreams.copy(in, out);
    }
    finally {
      CloseQuietly.close(out);
      CloseQuietly.close(in);
    }
  }

  public static void writeFloatMetric(
      OutputSupplier<? extends OutputStream> outSupplier, String name, CompressedFloatsSupplierSerializer column
  ) throws IOException
  {
    ByteStreams.write(version, outSupplier);
    serializerUtils.writeString(outSupplier, name);
    serializerUtils.writeString(outSupplier, "float");
    column.closeAndConsolidate(outSupplier);
  }

  public static void writeToChannel(MetricHolder holder, WritableByteChannel out) throws IOException
  {
    out.write(ByteBuffer.wrap(version));
    serializerUtils.writeString(out, holder.name);
    serializerUtils.writeString(out, holder.typeName);

    switch (holder.type) {
      case FLOAT:
        holder.floatType.writeToChannel(out);
        break;
      case COMPLEX:
        if (holder.complexType instanceof GenericIndexed) {
          ((GenericIndexed) holder.complexType).writeToChannel(out);
        } else {
          throw new IAE("Cannot serialize out MetricHolder for complex type that is not a GenericIndexed");
        }
        break;
    }
  }

  public static MetricHolder fromByteBuffer(ByteBuffer buf) throws IOException
  {
    return fromByteBuffer(buf, null);
  }

  public static MetricHolder fromByteBuffer(ByteBuffer buf, ObjectStrategy strategy) throws IOException
  {
    final byte ver = buf.get();
    if (version[0] != ver) {
      throw new ISE("Unknown version[%s] of MetricHolder", ver);
    }

    final String metricName = serializerUtils.readString(buf);
    final String typeName = serializerUtils.readString(buf);
    MetricHolder holder = new MetricHolder(metricName, typeName);

    switch (holder.type) {
      case FLOAT:
        holder.floatType = CompressedFloatsIndexedSupplier.fromByteBuffer(buf, ByteOrder.nativeOrder());
        break;
      case COMPLEX:
        if (strategy != null) {
          holder.complexType = GenericIndexed.read(buf, strategy);
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

  private final String name;
  private final String typeName;
  private final MetricType type;

  public enum MetricType
  {
    FLOAT,
    COMPLEX;

    static MetricType determineType(String typeName)
    {
      if ("float".equalsIgnoreCase(typeName)) {
        return FLOAT;
      }
      return COMPLEX;
    }
  }

  CompressedFloatsIndexedSupplier floatType = null;
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

  public IndexedFloats getFloatType()
  {
    assertType(MetricType.FLOAT);
    return floatType.get();
  }

  public Indexed getComplexType()
  {
    assertType(MetricType.COMPLEX);
    return complexType;
  }

  public MetricHolder convertByteOrder(ByteOrder order)
  {
    switch (type) {
      case FLOAT:
        MetricHolder retVal = new MetricHolder(name, typeName);
        retVal.floatType = floatType.convertByteOrder(order);
        return retVal;
      case COMPLEX:
        return this;
    }

    return null;
  }

  private void assertType(MetricType type)
  {
    if (this.type != type) {
      throw new IAE("type[%s] cannot be cast to [%s]", typeName, type);
    }
  }
}
