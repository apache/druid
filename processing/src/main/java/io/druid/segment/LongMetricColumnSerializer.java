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

import com.google.common.io.Files;
import io.druid.segment.data.CompressedLongsSupplierSerializer;
import io.druid.segment.data.CompressedObjectStrategy;
import io.druid.segment.data.IOPeon;

import java.io.File;
import java.io.IOException;

/**
 */
public class LongMetricColumnSerializer implements MetricColumnSerializer
{
  private final String metricName;
  private final IOPeon ioPeon;
  private final File outDir;

  private CompressedLongsSupplierSerializer writer;

  public LongMetricColumnSerializer(
      String metricName,
      File outDir,
      IOPeon ioPeon
  )
  {
    this.metricName = metricName;
    this.ioPeon = ioPeon;
    this.outDir = outDir;
  }

  @Override
  public void open() throws IOException
  {
    writer = CompressedLongsSupplierSerializer.create(
        ioPeon, String.format("%s_little", metricName), IndexIO.BYTE_ORDER,
        CompressedObjectStrategy.DEFAULT_COMPRESSION_STRATEGY
    );

    writer.open();
  }

  @Override
  public void serialize(Object obj) throws IOException
  {
    long val = (obj == null) ? 0 : ((Number) obj).longValue();
    writer.add(val);
  }

  @Override
  public void close() throws IOException
  {
    final File outFile = IndexIO.makeMetricFile(outDir, metricName, IndexIO.BYTE_ORDER);
    outFile.delete();
    MetricHolder.writeLongMetric(
        Files.newOutputStreamSupplier(outFile, true), metricName, writer
    );
    IndexIO.checkFileSize(outFile);

    writer = null;
  }
}
