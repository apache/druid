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

package com.metamx.druid.loading;

import com.google.common.base.Joiner;
import com.google.common.base.Throwables;
import com.google.inject.Inject;
import com.metamx.common.StreamUtils;
import com.metamx.common.logger.Logger;
import com.metamx.druid.client.DataSegment;
import com.metamx.druid.index.QueryableIndex;
import com.metamx.druid.index.QueryableIndexSegment;
import com.metamx.druid.index.Segment;
import org.apache.commons.io.FileUtils;

import java.io.*;

/**
 */
public class SingleSegmentLoader implements SegmentLoader
{
  private static final Logger log = new Logger(SingleSegmentLoader.class);

  private final DataSegmentPuller dataSegmentPuller;
  private final QueryableIndexFactory factory;
  private final SegmentLoaderConfig config;
  private static final Joiner JOINER = Joiner.on("/").skipNulls();

  @Inject
  public SingleSegmentLoader(
      DataSegmentPuller dataSegmentPuller,
      QueryableIndexFactory factory,
      SegmentLoaderConfig config
  )
  {
    this.dataSegmentPuller = dataSegmentPuller;
    this.factory = factory;
    this.config = config;
  }

  @Override
  public Segment getSegment(DataSegment segment) throws SegmentLoadingException
  {
    File segmentFiles = getSegmentFiles(segment);
    final QueryableIndex index = factory.factorize(segmentFiles);

    return new QueryableIndexSegment(segment.getIdentifier(), index);
  }

  public File getSegmentFiles(DataSegment segment) throws SegmentLoadingException
  {
    File localStorageDir = new File(config.getCacheDirectory(), DataSegmentPusherUtil.getStorageDir(segment));

    final String legacyDir = DataSegmentPusherUtil.getLegacyStorageDir(segment);
    if (legacyDir != null) {
      File legacyStorageDir = new File(config.getCacheDirectory(), legacyDir);

      if (legacyStorageDir.exists()) {
        log.info("Found legacyStorageDir[%s], moving to new storage location[%s]", legacyStorageDir, localStorageDir);
        if (localStorageDir.exists()) {
          try {
            FileUtils.deleteDirectory(localStorageDir);
          }
          catch (IOException e) {
            throw new SegmentLoadingException(e, "Error deleting localDir[%s]", localStorageDir);
          }
        }
        final File parentDir = localStorageDir.getParentFile();
        if (!parentDir.exists()) {
          log.info("Parent[%s] didn't exist, creating.", parentDir);
          if (!parentDir.mkdirs()) {
            log.warn("Unable to make parentDir[%s]", parentDir);
          }
        }

        if (!legacyStorageDir.renameTo(localStorageDir)) {
          log.warn("Failed moving [%s] to [%s]", legacyStorageDir, localStorageDir);
        }
      }
    }

    if (localStorageDir.exists()) {
      long localLastModified = localStorageDir.lastModified();
      long remoteLastModified = dataSegmentPuller.getLastModified(segment);
      if (remoteLastModified > 0 && localLastModified >= remoteLastModified) {
        log.info(
            "Found localStorageDir[%s] with modified[%s], which is same or after remote[%s].  Using.",
            localStorageDir, localLastModified, remoteLastModified
        );
        return localStorageDir;
      }
    }

    if (localStorageDir.exists()) {
      try {
        FileUtils.deleteDirectory(localStorageDir);
      }
      catch (IOException e) {
        log.warn(e, "Exception deleting previously existing local dir[%s]", localStorageDir);
      }
    }
    if (!localStorageDir.mkdirs()) {
      log.info("Unable to make parent file[%s]", localStorageDir);
    }

    dataSegmentPuller.getSegmentFiles(segment, localStorageDir);

    return localStorageDir;
  }

  private File getLocalStorageDir(DataSegment segment)
  {
    String outputKey = JOINER.join(
        segment.getDataSource(),
        String.format("%s_%s", segment.getInterval().getStart(), segment.getInterval().getEnd()),
        segment.getVersion(),
        segment.getShardSpec().getPartitionNum()
    );

    return new File(config.getCacheDirectory(), outputKey);
  }

  private void moveToCache(File pulledFile, File cacheFile) throws SegmentLoadingException
  {
    log.info("Rename pulledFile[%s] to cacheFile[%s]", pulledFile, cacheFile);
    if (!pulledFile.renameTo(cacheFile)) {
      log.warn("Error renaming pulledFile[%s] to cacheFile[%s].  Copying instead.", pulledFile, cacheFile);

      try {
        StreamUtils.copyToFileAndClose(new FileInputStream(pulledFile), cacheFile);
      }
      catch (IOException e) {
        throw new SegmentLoadingException(
            e,
            "Problem moving pulledFile[%s] to cache[%s]",
            pulledFile,
            cacheFile
        );
      }
      if (!pulledFile.delete()) {
        log.error("Could not delete pulledFile[%s].", pulledFile);
      }
    }
  }

  @Override
  public void cleanup(DataSegment segment) throws SegmentLoadingException
  {
    File cacheFile = getLocalStorageDir(segment);

    try {
      log.info("Deleting directory[%s]", cacheFile);
      FileUtils.deleteDirectory(cacheFile);
    }
    catch (IOException e) {
      throw new SegmentLoadingException(e, e.getMessage());
    }
  }
}
