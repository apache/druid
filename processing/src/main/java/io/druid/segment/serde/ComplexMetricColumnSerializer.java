/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
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
