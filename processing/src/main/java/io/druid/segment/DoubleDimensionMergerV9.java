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

import io.druid.java.util.common.io.Closer;
import io.druid.segment.column.ColumnCapabilities;
import io.druid.segment.column.ColumnDescriptor;
import io.druid.segment.column.ValueType;
import io.druid.segment.data.CompressedObjectStrategy;
import io.druid.segment.data.IOPeon;
import io.druid.segment.serde.DoubleGenericColumnPartSerde;

import java.io.File;
import java.io.IOException;
import java.nio.IntBuffer;
import java.util.List;

public class DoubleDimensionMergerV9 implements DimensionMergerV9<Double>
{
  protected String dimensionName;
  protected ProgressIndicator progress;
  protected final IndexSpec indexSpec;
  protected ColumnCapabilities capabilities;
  protected final File outDir;
  protected IOPeon ioPeon;
  private DoubleColumnSerializer serializer;

  public DoubleDimensionMergerV9(
      String dimensionName,
      IndexSpec indexSpec,
      File outDir,
      IOPeon ioPeon,
      ColumnCapabilities capabilities,
      ProgressIndicator progress
  )
  {
    this.dimensionName = dimensionName;
    this.indexSpec = indexSpec;
    this.capabilities = capabilities;
    this.outDir = outDir;
    this.ioPeon = ioPeon;
    this.progress = progress;

    try {
      setupEncodedValueWriter();
    }
    catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }

  protected void setupEncodedValueWriter() throws IOException
  {
    final CompressedObjectStrategy.CompressionStrategy metCompression = indexSpec.getMetricCompression();
    this.serializer = DoubleColumnSerializer.create(ioPeon, dimensionName, metCompression);
    serializer.open();
  }

  @Override
  public ColumnDescriptor makeColumnDescriptor() throws IOException
  {
    serializer.close();
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
  public void writeIndexes(List<IntBuffer> segmentRowNumConversions, Closer closer) throws IOException
  {
    // double columns do not have indexes
  }

  @Override
  public boolean canSkip()
  {
    // a double column can never be all null
    return false;
  }
}
