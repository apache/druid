/*
 * Druid - a distributed column store.
 * Copyright (C) 2012  Metamarkets Group Inc.
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

package com.metamx.druid.index.v1;

import java.io.File;
import java.io.IOException;
import java.nio.ByteOrder;

import com.google.common.io.Files;
import com.metamx.druid.kv.IOPeon;

/**
 */
public class FloatMetricColumnSerializer implements MetricColumnSerializer
{
  private final String metricName;
  private final IOPeon ioPeon;
  private final File outDir;

  private CompressedFloatsSupplierSerializer littleMetricsWriter;
  private CompressedFloatsSupplierSerializer bigEndianMetricsWriter;

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
    littleMetricsWriter = CompressedFloatsSupplierSerializer.create(
        ioPeon, String.format("%s_little", metricName), ByteOrder.LITTLE_ENDIAN
    );
    bigEndianMetricsWriter = CompressedFloatsSupplierSerializer.create(
        ioPeon, String.format("%s_big", metricName), ByteOrder.BIG_ENDIAN
    );

    littleMetricsWriter.open();
    bigEndianMetricsWriter.open();
  }

  @Override
  public void serialize(Object obj) throws IOException
  {
    float val = (obj == null) ? 0 : ((Number) obj).floatValue();
    littleMetricsWriter.add(val);
    bigEndianMetricsWriter.add(val);
  }

  @Override
  public void close() throws IOException
  {
    final File littleEndianFile = IndexIO.makeMetricFile(outDir, metricName, ByteOrder.LITTLE_ENDIAN);
    littleEndianFile.delete();
    MetricHolder.writeFloatMetric(
        Files.newOutputStreamSupplier(littleEndianFile, true), metricName, littleMetricsWriter
    );
    IndexIO.checkFileSize(littleEndianFile);

    final File bigEndianFile = IndexIO.makeMetricFile(outDir, metricName, ByteOrder.BIG_ENDIAN);
    bigEndianFile.delete();
    MetricHolder.writeFloatMetric(
        Files.newOutputStreamSupplier(bigEndianFile, true), metricName, bigEndianMetricsWriter
    );
    IndexIO.checkFileSize(bigEndianFile);

    littleMetricsWriter = null;
    bigEndianMetricsWriter = null;
  }
}
