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
import com.google.common.io.Closeables;
import com.google.inject.Inject;
import com.metamx.common.StreamUtils;
import com.metamx.common.logger.Logger;
import com.metamx.druid.client.DataSegment;
import com.metamx.druid.index.QueryableIndex;
import com.metamx.druid.index.QueryableIndexSegment;
import com.metamx.druid.index.Segment;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;

import java.io.*;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 */
public class SingleSegmentLoader implements SegmentLoader
{
  private static final Logger log = new Logger(SingleSegmentLoader.class);

  private final SegmentPuller segmentPuller;
  private final QueryableIndexFactory factory;
  private File cacheDirectory;
  private static final Joiner JOINER = Joiner.on("/").skipNulls();

  @Inject
  public SingleSegmentLoader(
          SegmentPuller segmentPuller,
          QueryableIndexFactory factory,
          File cacheDirectory)
  {
    this.segmentPuller = segmentPuller;
    this.factory = factory;
    this.cacheDirectory = cacheDirectory;
  }

  @Override
  public Segment getSegment(DataSegment segment) throws StorageAdapterLoadingException
  {
    File segmentFiles = getSegmentFiles(segment);
    final QueryableIndex index = factory.factorize(segmentFiles);

    return new QueryableIndexSegment(segment.getIdentifier(), index);
  }

  public File getSegmentFiles(DataSegment segment) throws StorageAdapterLoadingException {
    File cacheFile = getCacheFile(segment);
    if (cacheFile.exists()) {
      long localLastModified = cacheFile.lastModified();
      long remoteLastModified = segmentPuller.getLastModified(segment);
      if(remoteLastModified > 0 && localLastModified >= remoteLastModified){
        log.info(
            "Found cacheFile[%s] with modified[%s], which is same or after remote[%s].  Using.",
            cacheFile,
            localLastModified,
            remoteLastModified
        );
        return cacheFile.getParentFile();
      }
    }

    File pulledFile = segmentPuller.getSegmentFiles(segment);

    if(!cacheFile.getParentFile().mkdirs()){
      log.info("Unable to make parent file[%s]", cacheFile.getParentFile());
    }
    if (cacheFile.exists()) {
      cacheFile.delete();
    }

    if(pulledFile.getName().endsWith(".zip")){
      unzip(pulledFile, cacheFile.getParentFile());
    } else if(pulledFile.getName().endsWith(".gz")){
      gunzip(pulledFile, cacheFile);
    } else {
      moveToCache(pulledFile, cacheFile);
    }

    return cacheFile.getParentFile();
  }

  private File getCacheFile(DataSegment segment) {
    String outputKey = JOINER.join(
        segment.getDataSource(),
        String.format(
            "%s_%s",
            segment.getInterval().getStart(),
            segment.getInterval().getEnd()
        ),
        segment.getVersion(),
        segment.getShardSpec().getPartitionNum()
    );

    return new File(cacheDirectory, outputKey);
  }

  private void moveToCache(File pulledFile, File cacheFile) throws StorageAdapterLoadingException {
    log.info("Rename pulledFile[%s] to cacheFile[%s]", pulledFile, cacheFile);
    if(!pulledFile.renameTo(cacheFile)){
      log.warn("Error renaming pulledFile[%s] to cacheFile[%s].  Copying instead.", pulledFile, cacheFile);

      try {
        StreamUtils.copyToFileAndClose(new FileInputStream(pulledFile), cacheFile);
      } catch (IOException e) {
        throw new StorageAdapterLoadingException(e,"Problem moving pulledFile[%s] to cache[%s]", pulledFile, cacheFile);
      }
      if (!pulledFile.delete()) {
        log.error("Could not delete pulledFile[%s].", pulledFile);
      }
    }
  }

  private void unzip(File pulledFile, File cacheFile) throws StorageAdapterLoadingException {
    log.info("Unzipping file[%s] to [%s]", pulledFile, cacheFile);
    ZipInputStream zipIn = null;
    OutputStream out = null;
    ZipEntry entry = null;
    try {
      zipIn = new ZipInputStream(new BufferedInputStream(new FileInputStream(pulledFile)));
      while ((entry = zipIn.getNextEntry()) != null) {
        out = new FileOutputStream(new File(cacheFile, entry.getName()));
        IOUtils.copy(zipIn, out);
        zipIn.closeEntry();
        Closeables.closeQuietly(out);
        out = null;
      }
    } catch(IOException e) {
      throw new StorageAdapterLoadingException(e,"Problem unzipping[%s]", pulledFile);
    }
    finally {
      Closeables.closeQuietly(out);
      Closeables.closeQuietly(zipIn);
    }
  }

  private void gunzip(File pulledFile, File cacheFile) throws StorageAdapterLoadingException {
    log.info("Gunzipping file[%s] to [%s]", pulledFile, cacheFile);
    try {
      StreamUtils.copyToFileAndClose(
          new GZIPInputStream(new FileInputStream(pulledFile)),
          cacheFile
      );
    } catch (IOException e) {
      throw new StorageAdapterLoadingException(e,"Problem gunzipping[%s]", pulledFile);
    }
    if (!pulledFile.delete()) {
      log.error("Could not delete tmpFile[%s].", pulledFile);
    }
  }

  @Override
  public void cleanup(DataSegment segment) throws StorageAdapterLoadingException
  {
    File cacheFile = getCacheFile(segment).getParentFile();

    try {
      log.info("Deleting directory[%s]", cacheFile);
      FileUtils.deleteDirectory(cacheFile);
    }
    catch (IOException e) {
      throw new StorageAdapterLoadingException(e, e.getMessage());
    }
  }

}
