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
import com.google.common.io.OutputSupplier;
import io.druid.common.guava.FileOutputSupplier;
import io.druid.segment.column.ColumnCapabilities;
import io.druid.segment.data.CompressedObjectStrategy;
import io.druid.segment.data.CompressionFactory;
import io.druid.segment.data.IOPeon;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

public class LongDimensionMergerLegacy extends LongDimensionMergerV9 implements DimensionMergerLegacy<Long>
{
  private LongMetricColumnSerializer serializerV8;

  public LongDimensionMergerLegacy(
      String dimensionName,
      IndexSpec indexSpec,
      File outDir,
      IOPeon ioPeon,
      ColumnCapabilities capabilities,
      ProgressIndicator progress
  )
  {
    super(dimensionName, indexSpec, outDir, ioPeon, capabilities, progress);
  }

  @Override
  protected void setupEncodedValueWriter() throws IOException
  {
    final CompressedObjectStrategy.CompressionStrategy metCompression = indexSpec.getMetricCompression();
    final CompressionFactory.LongEncodingStrategy longEncoding = indexSpec.getLongEncoding();
    serializerV8 = new LongMetricColumnSerializer(dimensionName, outDir, ioPeon, metCompression, longEncoding);
    serializerV8.open();
  }

  @Override
  public void processMergedRow(Long rowValues) throws IOException
  {
    serializerV8.serialize(rowValues);
  }

  @Override
  public void writeValueMetadataToFile(FileOutputSupplier valueEncodingFile) throws IOException
  {
    // longs have no metadata to write
  }

  @Override
  public void writeRowValuesToFile(FileOutputSupplier rowValueOut) throws IOException
  {
    // closing the serializer writes its data to the file
    serializerV8.closeFile(rowValueOut.getFile());
  }

  @Override
  public void writeIndexesToFiles(
      ByteSink invertedOut, OutputSupplier<FileOutputStream> spatialOut
  ) throws IOException
  {
    // longs have no indices to write
  }

  @Override
  public File makeDimFile() throws IOException
  {
    return IndexIO.makeNumericDimFile(outDir, dimensionName, IndexIO.BYTE_ORDER);
  }
}
