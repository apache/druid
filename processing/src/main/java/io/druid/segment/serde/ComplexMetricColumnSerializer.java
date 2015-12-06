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

package io.druid.segment.serde;

import com.google.common.io.Files;
import io.druid.segment.IndexIO;
import io.druid.segment.MetricColumnSerializer;
import io.druid.segment.MetricHolder;
import io.druid.segment.data.GenericIndexedWriter;
import io.druid.segment.data.IOPeon;

import java.io.File;
import java.io.IOException;

/**
 */
public class ComplexMetricColumnSerializer implements MetricColumnSerializer
{
  private final String metricName;
  private final ComplexMetricSerde serde;
  private final IOPeon ioPeon;
  private final File outDir;

  private GenericIndexedWriter writer;

  public ComplexMetricColumnSerializer(
      String metricName,
      File outDir,
      IOPeon ioPeon,
      ComplexMetricSerde serde
  )
  {
    this.metricName = metricName;
    this.serde = serde;
    this.ioPeon = ioPeon;
    this.outDir = outDir;
  }

  @SuppressWarnings(value = "unchecked")
  @Override
  public void open() throws IOException
  {
    writer = new GenericIndexedWriter(
        ioPeon, String.format("%s_%s", metricName, outDir.getName()), serde.getObjectStrategy()
    );

    writer.open();
  }

  @Override
  public void serialize(Object agg) throws IOException
  {
    writer.write(agg);
  }

  @Override
  public void close() throws IOException
  {
    writer.close();

    final File outFile = IndexIO.makeMetricFile(outDir, metricName, IndexIO.BYTE_ORDER);
    outFile.delete();
    MetricHolder.writeComplexMetric(
        Files.newOutputStreamSupplier(outFile, true), metricName, serde.getTypeName(), writer
    );
    IndexIO.checkFileSize(outFile);

    writer = null;
  }
}
