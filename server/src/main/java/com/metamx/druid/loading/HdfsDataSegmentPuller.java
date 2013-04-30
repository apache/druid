package com.metamx.druid.loading;

import com.google.common.io.Closeables;
import com.metamx.druid.client.DataSegment;
import com.metamx.druid.utils.CompressionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.IOException;

/**
 */
public class HdfsDataSegmentPuller implements DataSegmentPuller
{
  private final Configuration config;

  public HdfsDataSegmentPuller(final Configuration config)
  {
    this.config = config;
  }

  @Override
  public void getSegmentFiles(DataSegment segment, File dir) throws SegmentLoadingException
  {
    final Path path = getPath(segment);

    final FileSystem fs = checkPathAndGetFilesystem(path);

    FSDataInputStream in = null;
    try {
      if (path.getName().endsWith(".zip")) {
        in = fs.open(path);
        CompressionUtils.unzip(in, dir);
        in.close();
      }
      else {
        throw new SegmentLoadingException("Unknown file type[%s]", path);
      }
    }
    catch (IOException e) {
      throw new SegmentLoadingException(e, "Some IOException");
    }
    finally {
      Closeables.closeQuietly(in);
    }
  }

  @Override
  public long getLastModified(DataSegment segment) throws SegmentLoadingException
  {
    Path path = getPath(segment);
    FileSystem fs = checkPathAndGetFilesystem(path);

    try {
      return fs.getFileStatus(path).getModificationTime();
    }
    catch (IOException e) {
      throw new SegmentLoadingException(e, "Problem loading status of path[%s]", path);
    }
  }

  private Path getPath(DataSegment segment) {
    return new Path(String.valueOf(segment.getLoadSpec().get("path")));
  }

  private FileSystem checkPathAndGetFilesystem(Path path) throws SegmentLoadingException
  {
    FileSystem fs;
    try {
      fs = path.getFileSystem(config);

      if (!fs.exists(path)) {
        throw new SegmentLoadingException("Path[%s] doesn't exist.", path);
      }

      return fs;
    }
    catch (IOException e) {
      throw new SegmentLoadingException(e, "Problems interacting with filesystem[%s].", path);
    }
  }
}
