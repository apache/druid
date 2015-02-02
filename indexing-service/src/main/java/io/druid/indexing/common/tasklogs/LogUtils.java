package io.druid.indexing.common.tasklogs;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.channels.Channels;

public class LogUtils
{
  /**
   * Open a stream to a file.
   *
   * @param offset If zero, stream the entire log. If positive, read from this byte position onwards. If negative,
   *               read this many bytes from the end of the file.
   *
   * @return input supplier for this log, if available from this provider
   */
  public static InputStream streamFile(final File file, final long offset) throws IOException
  {
    final RandomAccessFile raf = new RandomAccessFile(file, "r");
    final long rafLength = raf.length();
    if (offset > 0) {
      raf.seek(offset);
    } else if (offset < 0 && offset < rafLength) {
      raf.seek(Math.max(0, rafLength + offset));
    }
    return Channels.newInputStream(raf.getChannel());
  }
}
