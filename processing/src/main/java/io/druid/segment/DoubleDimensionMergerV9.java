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

import io.druid.segment.column.ColumnDescriptor;
import io.druid.segment.column.ValueType;
import io.druid.segment.data.CompressionStrategy;
import io.druid.segment.serde.DoubleGenericColumnPartSerde;
import io.druid.segment.writeout.SegmentWriteOutMedium;

import java.io.IOException;
import java.nio.IntBuffer;
import java.util.List;

public class DoubleDimensionMergerV9 implements DimensionMergerV9<Double>
{
  protected String dimensionName;
  protected final IndexSpec indexSpec;
  private DoubleColumnSerializer serializer;

  public DoubleDimensionMergerV9(
      String dimensionName,
      IndexSpec indexSpec,
      SegmentWriteOutMedium segmentWriteOutMedium
  )
  {
    this.dimensionName = dimensionName;
    this.indexSpec = indexSpec;

    try {
      setupEncodedValueWriter(segmentWriteOutMedium);
    }
    catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }

  private void setupEncodedValueWriter(SegmentWriteOutMedium segmentWriteOutMedium) throws IOException
  {
    final CompressionStrategy metCompression = indexSpec.getMetricCompression();
    this.serializer = DoubleColumnSerializer.create(segmentWriteOutMedium, dimensionName, metCompression);
    serializer.open();
  }

  @Override
  public void writeMergedValueMetadata(List<IndexableAdapter> adapters) throws IOException
  {
    // double columns do not have additional metadata
  }

  @Override
  public Double convertSegmentRowValuesToMergedRowValues(Double segmentRow, int segmentIndexNumber)
  {
    return segmentRow;
  }

  @Override
  public void processMergedRow(Double rowValues) throws IOException
  {
    serializer.serialize(rowValues);
  }

  @Override
  public void writeIndexes(List<IntBuffer> segmentRowNumConversions) throws IOException
  {
    // double columns do not have indexes
  }

  @Override
  public boolean canSkip()
  {
    // a double column can never be all null
    return false;
  }

  @Override
  public ColumnDescriptor makeColumnDescriptor() throws IOException
  {
    final ColumnDescriptor.Builder builder = ColumnDescriptor.builder();
    builder.setValueType(ValueType.DOUBLE);
    builder.addSerde(
        DoubleGenericColumnPartSerde.serializerBuilder()
                                    .withByteOrder(IndexIO.BYTE_ORDER)
                                    .withDelegate(serializer)
                                    .build()
    );
    return builder.build();
  }
}
