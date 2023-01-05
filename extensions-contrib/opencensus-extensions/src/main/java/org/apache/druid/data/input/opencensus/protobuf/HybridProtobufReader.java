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

package org.apache.druid.data.input.opencensus.protobuf;

import org.apache.druid.data.input.InputEntityReader;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowListPlusRawValues;
import org.apache.druid.data.input.KafkaUtils;
import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.data.input.impl.ByteEntity;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.opentelemetry.protobuf.OpenTelemetryMetricsProtobufReader;
import org.apache.druid.indexing.seekablestream.SettableByteEntity;
import org.apache.druid.java.util.common.parsers.CloseableIterator;

import java.io.IOException;
import java.lang.invoke.MethodHandle;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class HybridProtobufReader implements InputEntityReader
{
  private static final String VERSION_HEADER_KEY = "v";
  private static final int OPENTELEMETRY_FORMAT_VERSION = 1;

  private final DimensionsSpec dimensionsSpec;
  private final SettableByteEntity<? extends ByteEntity> source;
  private final String metricDimension;
  private final String valueDimension;
  private final String metricLabelPrefix;
  private final String resourceLabelPrefix;

  private volatile MethodHandle getHeaderMethod = null;

  enum ProtobufReader
  {
    OPENCENSUS,
    OPENTELEMETRY
  }

  public HybridProtobufReader(
      DimensionsSpec dimensionsSpec,
      SettableByteEntity<? extends ByteEntity> source,
      String metricDimension,
      String valueDimension,
      String metricLabelPrefix,
      String resourceLabelPrefix
  )
  {
    this.dimensionsSpec = dimensionsSpec;
    this.source = source;
    this.metricDimension = metricDimension;
    this.valueDimension = valueDimension;
    this.metricLabelPrefix = metricLabelPrefix;
    this.resourceLabelPrefix = resourceLabelPrefix;
  }

  @Override
  public CloseableIterator<InputRow> read() throws IOException
  {
    return newReader(whichReader()).read();
  }

  public InputEntityReader newReader(ProtobufReader which)
  {
    switch (which) {
      case OPENTELEMETRY:
        return new OpenTelemetryMetricsProtobufReader(
            dimensionsSpec,
            source,
            metricDimension,
            valueDimension,
            metricLabelPrefix,
            resourceLabelPrefix
        );
      case OPENCENSUS:
      default:
        return new OpenCensusProtobufReader(
            dimensionsSpec,
            source,
            metricDimension,
            metricLabelPrefix,
            resourceLabelPrefix
        );
    }
  }

  public ProtobufReader whichReader()
  {
    // assume InputEntity is always defined in a single classloader (the kafka-indexing-service classloader)
    // so we only have to look it up once. To be completely correct we should cache the method based on classloader
    if (getHeaderMethod == null) {
      getHeaderMethod = KafkaUtils.lookupGetHeaderMethod(
          source.getEntity().getClass().getClassLoader(),
          VERSION_HEADER_KEY
      );
    }

    try {
      byte[] versionHeader = (byte[]) getHeaderMethod.invoke(source.getEntity());
      if (versionHeader != null) {
        int version =
            ByteBuffer.wrap(versionHeader).order(ByteOrder.LITTLE_ENDIAN).getInt();
        if (version == OPENTELEMETRY_FORMAT_VERSION) {
          return ProtobufReader.OPENTELEMETRY;
        }
      }
    }
    catch (Throwable t) {
      // assume input is opencensus if something went wrong
    }
    return ProtobufReader.OPENCENSUS;
  }

  @Override
  public CloseableIterator<InputRowListPlusRawValues> sample() throws IOException
  {
    return read().map(row -> InputRowListPlusRawValues.of(row, ((MapBasedInputRow) row).getEvent()));
  }
}
