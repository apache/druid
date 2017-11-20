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

import com.google.common.base.Throwables;
import io.druid.segment.column.ColumnDescriptor;
import io.druid.segment.column.ValueType;
import io.druid.segment.data.CompressionFactory;
import io.druid.segment.data.CompressionStrategy;
import io.druid.segment.serde.LongGenericColumnPartSerde;
import io.druid.segment.writeout.SegmentWriteOutMedium;

import java.io.IOException;
import java.nio.IntBuffer;
import java.util.List;

public class LongDimensionMergerV9 implements DimensionMergerV9<Long>
{
  protected String dimensionName;
  protected final IndexSpec indexSpec;
  protected LongColumnSerializer serializer;

  LongDimensionMergerV9(
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
      Throwables.propagate(ioe);
    }
  }

  protected void setupEncodedValueWriter(SegmentWriteOutMedium segmentWriteOutMedium) throws IOException
  {
    final CompressionStrategy metCompression = indexSpec.getMetricCompression();
    final CompressionFactory.LongEncodingStrategy longEncoding = indexSpec.getLongEncoding();
    this.serializer = LongColumnSerializer.create(segmentWriteOutMedium, dimensionName, metCompression, longEncoding);
    serializer.open();
  }

  @Override
  public void writeMergedValueMetadata(List<IndexableAdapter> adapters) throws IOException
  {
    // longs have no additional metadata
  }

  @Override
  public Long convertSegmentRowValuesToMergedRowValues(Long segmentRow, int segmentIndexNumber)
  {
    return segmentRow;
  }

  @Override
  public void processMergedRow(Long rowValues) throws IOException
  {
    serializer.serialize(rowValues);
  }

  @Override
  public void writeIndexes(List<IntBuffer> segmentRowNumConversions) throws IOException
  {
    // longs have no indices to write
  }

  @Override
  public boolean canSkip()
  {
    // a long column can never be all null
    return false;
  }

  @Override
  public ColumnDescriptor makeColumnDescriptor() throws IOException
  {
    final ColumnDescriptor.Builder builder = ColumnDescriptor.builder();
    builder.setValueType(ValueType.LONG);
    builder.addSerde(
        LongGenericColumnPartSerde.serializerBuilder()
                                  .withByteOrder(IndexIO.BYTE_ORDER)
                                  .withDelegate(serializer)
                                  .build()
    );
    return builder.build();
  }
}
