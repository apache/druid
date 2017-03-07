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

import io.druid.segment.column.ColumnCapabilities;
import io.druid.segment.data.CompressedObjectStrategy;
import io.druid.segment.data.CompressionFactory;

import java.io.File;
import java.io.IOException;

public class LongDimensionMergerLegacy extends LongDimensionMergerV9 implements DimensionMergerLegacy<Long>
{
  private LongMetricColumnSerializer serializerV8;

  public LongDimensionMergerLegacy(
      String dimensionName,
      IndexSpec indexSpec,
      File outDir,
      ColumnCapabilities capabilities,
      ProgressIndicator progress
  )
  {
    super(dimensionName, indexSpec, outDir, capabilities, progress);
  }

  @Override
  protected void setupEncodedValueWriter() throws IOException
  {
    final CompressedObjectStrategy.CompressionStrategy metCompression = indexSpec.getMetricCompression();
    final CompressionFactory.LongEncodingStrategy longEncoding = indexSpec.getLongEncoding();
    serializerV8 = new LongMetricColumnSerializer(dimensionName, outDir, metCompression, longEncoding);
    serializerV8.open();
  }

  @Override
  public void processMergedRow(Long rowValues) throws IOException
  {
    serializerV8.serialize(rowValues);
  }

  @Override
  public void writeValueMetadataToFile(File valueEncodingFile) throws IOException
  {
    // longs have no metadata to write
  }

  @Override
  public void writeRowValuesToFile(File rowValueOut) throws IOException
  {
    // closing the serializer writes its data to the file
    serializerV8.closeFile(rowValueOut);
  }

  @Override
  public void writeIndexesToFiles(File invertedOut, File spatialOut) throws IOException
  {
    // longs have no indices to write
  }

  @Override
  public File makeDimFile() throws IOException
  {
    return IndexIO.makeNumericDimFile(outDir, dimensionName, IndexIO.BYTE_ORDER);
  }
}
