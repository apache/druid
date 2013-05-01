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

package com.metamx.druid.utils;

import com.google.common.io.ByteStreams;
import com.google.common.io.Closeables;
import com.google.common.io.Files;
import com.metamx.common.ISE;
import com.metamx.common.StreamUtils;
import com.metamx.common.logger.Logger;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

/**
 */
public class CompressionUtils
{
  private static final Logger log = new Logger(CompressionUtils.class);

  public static long zip(File directory, File outputZipFile) throws IOException
  {
    if (!outputZipFile.getName().endsWith(".zip")) {
      log.warn("No .zip suffix[%s], putting files from [%s] into it anyway.", outputZipFile, directory);
    }

    final FileOutputStream out = new FileOutputStream(outputZipFile);
    try {
      final long retVal = zip(directory, out);

      out.close();

      return retVal;
    }
    finally {
      Closeables.closeQuietly(out);
    }
  }

  public static long zip(File directory, OutputStream out) throws IOException
  {
    if (!directory.isDirectory()) {
      throw new IOException(String.format("directory[%s] is not a directory", directory));
    }

    long totalSize = 0;
    ZipOutputStream zipOut = null;
    try {
      zipOut = new ZipOutputStream(out);
      File[] files = directory.listFiles();
      for (File file : files) {
        log.info("Adding file[%s] with size[%,d].  Total size so far[%,d]", file, file.length(), totalSize);
        if (file.length() >= Integer.MAX_VALUE) {
          zipOut.finish();
          throw new IOException(String.format("file[%s] too large [%,d]", file, file.length()));
        }
        zipOut.putNextEntry(new ZipEntry(file.getName()));
        totalSize += ByteStreams.copy(Files.newInputStreamSupplier(file), zipOut);
      }
      zipOut.closeEntry();
    }
    finally {
      if (zipOut != null) {
        zipOut.finish();
      }
    }

    return totalSize;
  }

  public static void unzip(File pulledFile, File outDir) throws IOException
  {
    if (!(outDir.exists() && outDir.isDirectory())) {
      throw new ISE("outDir[%s] must exist and be a directory", outDir);
    }

    log.info("Unzipping file[%s] to [%s]", pulledFile, outDir);
    InputStream in = null;
    try {
      in = new BufferedInputStream(new FileInputStream(pulledFile));
      unzip(in, outDir);
    }
    finally {
      Closeables.closeQuietly(in);
    }
  }

  public static void unzip(InputStream in, File outDir) throws IOException
  {
    ZipInputStream zipIn = new ZipInputStream(in);

    ZipEntry entry;
    while ((entry = zipIn.getNextEntry()) != null) {
      FileOutputStream out = null;
      try {
        out = new FileOutputStream(new File(outDir, entry.getName()));
        ByteStreams.copy(zipIn, out);
        zipIn.closeEntry();
        out.close();
      }
      finally {
        Closeables.closeQuietly(out);
      }
    }
  }

  public static void gunzip(File pulledFile, File outDir) throws IOException
  {
    log.info("Gunzipping file[%s] to [%s]", pulledFile, outDir);
    StreamUtils.copyToFileAndClose(new GZIPInputStream(new FileInputStream(pulledFile)), outDir);
    if (!pulledFile.delete()) {
      log.error("Could not delete tmpFile[%s].", pulledFile);
    }
  }

}
