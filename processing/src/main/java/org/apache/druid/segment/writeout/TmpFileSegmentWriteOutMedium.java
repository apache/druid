/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.segment.writeout;

import com.google.common.annotations.VisibleForTesting;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.logger.Logger;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Builds segment write out medium objects that are based on temporary files.  Some analysis of usage of these
 * objects shows that they periodically get used for very small things, where the overhead of creating a tmp file
 * is actually very large.  It would be best to go back and look at usages and try to make them lazy such that
 * they only actually use a medium when they need it.  But, in an attempt to get some benefits, we "shim" in the
 * laziness by returning a heap-based WriteOutBytes that only falls back to making a tmp file when it actually
 * fills up.
 */
public final class TmpFileSegmentWriteOutMedium implements SegmentWriteOutMedium
{
  private static final Logger log = new Logger(TmpFileSegmentWriteOutMedium.class);

  private final AtomicInteger filesCreated;
  private final SortedMap<Long, Integer> sizeDistribution;
  private int numLocallyCreated = 0;
  private boolean root = false;

  private final File dir;
  private final Closer closer = Closer.create();

  TmpFileSegmentWriteOutMedium(File outDir) throws IOException
  {
    this(outDir, new AtomicInteger(0), new ConcurrentSkipListMap<>());
    root = true;
  }

  private TmpFileSegmentWriteOutMedium(File outDir, AtomicInteger filesCreated, SortedMap<Long, Integer> sizeDistribution) throws IOException
  {
    this.filesCreated = filesCreated;
    this.sizeDistribution = sizeDistribution;
    File tmpOutputFilesDir = new File(outDir, "tmpOutputFiles");
    FileUtils.mkdirp(tmpOutputFilesDir);
    closer.register(() -> FileUtils.deleteDirectory(tmpOutputFilesDir));
    this.dir = tmpOutputFilesDir;
  }

  @Override
  public WriteOutBytes makeWriteOutBytes()
  {
    return new LazilyAllocatingHeapWriteOutBytes(
        () -> {
          final int i = filesCreated.incrementAndGet();
          ++numLocallyCreated;
          File file;
          FileChannel ch;
          try {
            file = File.createTempFile("filePeon", null, dir);
            ch = FileChannel.open(file.toPath(), StandardOpenOption.READ, StandardOpenOption.WRITE);
          }
          catch (IOException e) {
            throw DruidException.defensive(e, "Failed");
          }
          if (i % 1000 == 0) {
            log.info("Created [%,d] tmp files. [%s]", i, dir);
          }
          final FileWriteOutBytes retVal = new FileWriteOutBytes(file, ch, closer);
          closer.register(file::delete);
          closer.register(ch);
          closer.register(() -> {
            sizeDistribution.compute(retVal.size(), (key, val) -> val == null ? 1 : val + 1);
          });
          return retVal;
        },
        closer
    );
  }

  @Override
  public SegmentWriteOutMedium makeChildWriteOutMedium() throws IOException
  {
    TmpFileSegmentWriteOutMedium tmpFileSegmentWriteOutMedium = new TmpFileSegmentWriteOutMedium(
        dir,
        filesCreated,
        sizeDistribution
    );
    closer.register(tmpFileSegmentWriteOutMedium);
    return tmpFileSegmentWriteOutMedium;
  }

  @Override
  public Closer getCloser()
  {
    return closer;
  }

  @Override
  public void close() throws IOException
  {
    log.debug("Closing, files still open[%,d], filesBeingClosed[%,d], dir[%s]", filesCreated.get(), numLocallyCreated, dir);
    filesCreated.set(filesCreated.get() - numLocallyCreated);
    numLocallyCreated = 0;
    closer.close();

    if (root && log.isDebugEnabled()) {
      log.debug("Size distribution of files:");
      for (Map.Entry<Long, Integer> entry : sizeDistribution.entrySet()) {
        log.debug("%,15d => %,15d", entry.getKey(), entry.getValue());
      }
    }
  }

  @VisibleForTesting
  int getFilesCreated()
  {
    return filesCreated.get();
  }

  @VisibleForTesting
  int getNumLocallyCreated()
  {
    return numLocallyCreated;
  }
}
