package com.metamx.druid.indexer;

import com.metamx.druid.kv.IOPeon;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 */
class HadoopIOPeon implements IOPeon
{
  private final JobContext job;
  private final Path baseDir;
  private final boolean overwriteFiles;

  public HadoopIOPeon(JobContext job, Path baseDir, final boolean overwriteFiles)
  {
    this.job = job;
    this.baseDir = baseDir;
    this.overwriteFiles = overwriteFiles;
  }

  @Override
  public OutputStream makeOutputStream(String filename) throws IOException
  {
    return Utils.makePathAndOutputStream(job, new Path(baseDir, filename), overwriteFiles);
  }

  @Override
  public InputStream makeInputStream(String filename) throws IOException
  {
    return Utils.openInputStream(job, new Path(baseDir, filename));
  }

  @Override
  public void cleanup() throws IOException
  {
    throw new UnsupportedOperationException();
  }
}
