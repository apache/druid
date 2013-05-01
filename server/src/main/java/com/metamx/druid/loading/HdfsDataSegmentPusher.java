package com.metamx.druid.loading;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.ByteStreams;
import com.google.common.io.Closeables;
import com.google.common.io.OutputSupplier;
import com.metamx.common.logger.Logger;
import com.metamx.druid.client.DataSegment;
import com.metamx.druid.index.v1.IndexIO;
import com.metamx.druid.utils.CompressionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;

/**
 */
public class HdfsDataSegmentPusher implements DataSegmentPusher
{
  private static final Logger log = new Logger(HdfsDataSegmentPusher.class);

  private final HdfsDataSegmentPusherConfig config;
  private final Configuration hadoopConfig;
  private final ObjectMapper jsonMapper;

  public HdfsDataSegmentPusher(
      HdfsDataSegmentPusherConfig config,
      Configuration hadoopConfig,
      ObjectMapper jsonMapper
  )
  {
    this.config = config;
    this.hadoopConfig = hadoopConfig;
    this.jsonMapper = jsonMapper;
  }

  @Override
  public DataSegment push(File inDir, DataSegment segment) throws IOException
  {
    final String storageDir = DataSegmentPusherUtil.getStorageDir(segment);
    Path outFile = new Path(String.format("%s/%s/index.zip", config.getStorageDirectory(), storageDir));
    FileSystem fs = outFile.getFileSystem(hadoopConfig);

    fs.mkdirs(outFile.getParent());
    log.info("Compressing files from[%s] to [%s]", inDir, outFile);
    FSDataOutputStream out = null;
    long size;
    try {
      out = fs.create(outFile);

      size = CompressionUtils.zip(inDir, out);

      out.close();
    }
    finally {
      Closeables.closeQuietly(out);
    }

    return createDescriptorFile(
        segment.withLoadSpec(makeLoadSpec(outFile))
               .withSize(size)
               .withBinaryVersion(IndexIO.CURRENT_VERSION_ID),
        outFile.getParent(),
        fs
    );
  }

  private DataSegment createDescriptorFile(DataSegment segment, Path outDir, final FileSystem fs) throws IOException
  {
    final Path descriptorFile = new Path(outDir, "descriptor.json");
    log.info("Creating descriptor file at[%s]", descriptorFile);
    ByteStreams.copy(
        ByteStreams.newInputStreamSupplier(jsonMapper.writeValueAsBytes(segment)),
        new HdfsOutputStreamSupplier(fs, descriptorFile)
    );
    return segment;
  }

  private ImmutableMap<String, Object> makeLoadSpec(Path outFile)
  {
    return ImmutableMap.<String, Object>of("type", "hdfs", "path", outFile.toString());
  }

  private static class HdfsOutputStreamSupplier implements OutputSupplier<OutputStream>
  {
    private final FileSystem fs;
    private final Path descriptorFile;

    public HdfsOutputStreamSupplier(FileSystem fs, Path descriptorFile)
    {
      this.fs = fs;
      this.descriptorFile = descriptorFile;
    }

    @Override
    public OutputStream getOutput() throws IOException
    {
      return fs.create(descriptorFile);
    }
  }
}
