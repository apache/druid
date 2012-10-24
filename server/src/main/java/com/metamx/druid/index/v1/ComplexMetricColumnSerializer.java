package com.metamx.druid.index.v1;

import com.google.common.io.ByteStreams;
import com.google.common.io.Files;
import com.metamx.druid.index.v1.serde.ComplexMetricSerde;
import com.metamx.druid.kv.FlattenedArrayWriter;
import com.metamx.druid.kv.IOPeon;

import java.io.File;
import java.io.IOException;
import java.nio.ByteOrder;

/**
 */
public class ComplexMetricColumnSerializer implements MetricColumnSerializer
{
  private final String metricName;
  private final ComplexMetricSerde serde;
  private final IOPeon ioPeon;
  private final File outDir;

  private FlattenedArrayWriter writer;

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
    writer = new FlattenedArrayWriter(
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

    final File littleEndianFile = IndexIO.makeMetricFile(outDir, metricName, ByteOrder.LITTLE_ENDIAN);
    littleEndianFile.delete();
    MetricHolder.writeComplexMetric(
        Files.newOutputStreamSupplier(littleEndianFile, true), metricName, serde.getTypeName(), writer
    );
    IndexIO.checkFileSize(littleEndianFile);

    final File bigEndianFile = IndexIO.makeMetricFile(outDir, metricName, ByteOrder.BIG_ENDIAN);
    ByteStreams.copy(
        Files.newInputStreamSupplier(littleEndianFile),
        Files.newOutputStreamSupplier(bigEndianFile, false)
    );

    writer = null;
  }
}
