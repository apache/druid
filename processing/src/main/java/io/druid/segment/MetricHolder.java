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

import io.druid.common.utils.SerializerUtils;
import io.druid.io.Channels;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.io.smoosh.SmooshedFileMapper;
import io.druid.segment.data.CompressedFloatsIndexedSupplier;
import io.druid.segment.data.CompressedLongsIndexedSupplier;
import io.druid.segment.data.FloatSupplierSerializer;
import io.druid.segment.data.GenericIndexed;
import io.druid.segment.data.GenericIndexedWriter;
import io.druid.segment.data.Indexed;
import io.druid.segment.data.IndexedFloats;
import io.druid.segment.data.IndexedLongs;
import io.druid.segment.data.LongSupplierSerializer;
import io.druid.segment.data.ObjectStrategy;
import io.druid.segment.serde.ComplexMetricSerde;
import io.druid.segment.serde.ComplexMetrics;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;

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

  public static void writeComplexMetric(File file, String name, String typeName, GenericIndexedWriter column)
      throws IOException
  {
    try (FileChannel out = FileChannel.open(file.toPath(), StandardOpenOption.CREATE, StandardOpenOption.APPEND)) {
      writeVersion(out);
      serializerUtils.writeString(out, name);
      serializerUtils.writeString(out, typeName);
      column.writeTo(out, null);
    }
  }

  static void writeFloatMetric(File outFile, String name, FloatSupplierSerializer column) throws IOException
  {
    try (FileChannel out = FileChannel.open(outFile.toPath(), StandardOpenOption.CREATE, StandardOpenOption.APPEND)) {
      writeVersion(out);
      serializerUtils.writeString(out, name);
      serializerUtils.writeString(out, "float");
      column.writeTo(out, null);
    }
  }

  public static void writeLongMetric(File outFile, String name, LongSupplierSerializer column) throws IOException
  {
    try (FileChannel out = FileChannel.open(outFile.toPath(), StandardOpenOption.CREATE, StandardOpenOption.APPEND)) {
      writeVersion(out);
      serializerUtils.writeString(out, name);
      serializerUtils.writeString(out, "long");
      column.writeTo(out, null);
    }
  }

  private static void writeVersion(FileChannel out) throws IOException
  {
    Channels.writeFully(out, ByteBuffer.wrap(version));
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

  private final String name;
  private final String typeName;
  private final MetricType type;

  public enum MetricType
  {
    LONG,
    FLOAT,
    COMPLEX;

    static MetricType determineType(String typeName)
    {
      if ("long".equalsIgnoreCase(typeName)) {
        return LONG;
      } else if ("float".equalsIgnoreCase(typeName)) {
        return FLOAT;
      }
      return COMPLEX;
    }
  }

  CompressedLongsIndexedSupplier longType = null;
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
