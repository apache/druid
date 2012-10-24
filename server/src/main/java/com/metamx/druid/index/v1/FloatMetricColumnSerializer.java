package com.metamx.druid.index.v1;

import com.google.common.io.Files;
import com.metamx.druid.kv.IOPeon;

import java.io.File;
import java.io.IOException;
import java.nio.ByteOrder;

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
