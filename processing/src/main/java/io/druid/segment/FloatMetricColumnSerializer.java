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

package io.druid.segment;

import com.google.common.io.Files;
import io.druid.segment.data.CompressedFloatsSupplierSerializer;
import io.druid.segment.data.IOPeon;

import java.io.File;
import java.io.IOException;

/**
 */
public class FloatMetricColumnSerializer implements MetricColumnSerializer
{
  private final String metricName;
  private final IOPeon ioPeon;
  private final File outDir;

  private CompressedFloatsSupplierSerializer writer;

  public FloatMetricColumnSerializer(
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
    writer = CompressedFloatsSupplierSerializer.create(
        ioPeon, String.format("%s_little", metricName), IndexIO.BYTE_ORDER
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
    outFile.delete();
    MetricHolder.writeFloatMetric(
        Files.newOutputStreamSupplier(outFile, true), metricName, writer
    );
    IndexIO.checkFileSize(outFile);

    writer = null;
  }
}
