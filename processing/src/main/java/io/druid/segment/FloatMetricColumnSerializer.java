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

import io.druid.segment.data.CompressionFactory;
import io.druid.segment.data.CompressionStrategy;
import io.druid.segment.data.FloatSupplierSerializer;

import java.io.File;
import java.io.IOException;

/**
 */
public class FloatMetricColumnSerializer implements MetricColumnSerializer
{
  private final String metricName;
  private final File outDir;
  private final CompressionStrategy compression;

  private FloatSupplierSerializer writer;

  FloatMetricColumnSerializer(
      String metricName,
      File outDir,
      CompressionStrategy compression
  )
  {
    this.metricName = metricName;
    this.outDir = outDir;
    this.compression = compression;
  }

  @Override
  public void open() throws IOException
  {
    writer = CompressionFactory.getFloatSerializer(
        String.format("%s_little", metricName),
        IndexIO.BYTE_ORDER,
        compression
    );
    writer.open();
  }

  @Override
  public void serialize(Object obj) throws IOException
  {
    float val = (obj == null) ? 0 : ((Number) obj).floatValue();
    writer.add(val);
  }

  @Override
  public void close() throws IOException
  {
    final File outFile = IndexIO.makeMetricFile(outDir, metricName, IndexIO.BYTE_ORDER);
    closeFile(outFile);
  }

  @Override
  public void closeFile(final File outFile) throws IOException
  {
    outFile.delete();
    MetricHolder.writeFloatMetric(outFile, metricName, writer);
    IndexIO.checkFileSize(outFile);

    writer = null;
  }
}
