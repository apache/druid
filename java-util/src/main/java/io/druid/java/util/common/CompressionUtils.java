/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.java.util.common;

import com.google.common.base.Predicate;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.io.ByteSink;
import com.google.common.io.ByteSource;
import com.google.common.io.ByteStreams;
import com.google.common.io.Files;
import io.druid.java.util.common.io.NativeIO;
import io.druid.java.util.common.logger.Logger;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.commons.compress.compressors.xz.XZCompressorInputStream;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Enumeration;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

public class CompressionUtils
{
  private static final Logger log = new Logger(CompressionUtils.class);
  private static final int DEFAULT_RETRY_COUNT = 3;
  private static final String BZ2_SUFFIX = ".bz2";
  private static final String GZ_SUFFIX = ".gz";
  private static final String XZ_SUFFIX = ".xz";
  private static final String ZIP_SUFFIX = ".zip";

  /**
   * Zip the contents of directory into the file indicated by outputZipFile. Sub directories are skipped
   *
   * @param directory     The directory whose contents should be added to the zip in the output stream.
   * @param outputZipFile The output file to write the zipped data to
   * @param fsync         True if the output file should be fsynced to disk
   *
   * @return The number of bytes (uncompressed) read from the input directory.
   *
   * @throws IOException
   */
  public static long zip(File directory, File outputZipFile, boolean fsync) throws IOException
  {
    if (!isZip(outputZipFile.getName())) {
      log.warn("No .zip suffix[%s], putting files from [%s] into it anyway.", outputZipFile, directory);
    }

    try (final FileOutputStream out = new FileOutputStream(outputZipFile)) {
      long bytes = zip(directory, out);

      // For explanation of why fsyncing here is a good practice:
      // https://github.com/druid-io/druid/pull/5187#pullrequestreview-85188984
      if (fsync) {
        out.getChannel().force(true);
      }

      return bytes;
    }
  }

  /**
   * Zip the contents of directory into the file indicated by outputZipFile. Sub directories are skipped
   *
   * @param directory     The directory whose contents should be added to the zip in the output stream.
   * @param outputZipFile The output file to write the zipped data to
   *
   * @return The number of bytes (uncompressed) read from the input directory.
   *
   * @throws IOException
   */
  public static long zip(File directory, File outputZipFile) throws IOException
  {
    return zip(directory, outputZipFile, false);
  }

  /**
   * Zips the contents of the input directory to the output stream. Sub directories are skipped
   *
   * @param directory The directory whose contents should be added to the zip in the output stream.
   * @param out       The output stream to write the zip data to. Caller is responsible for closing this stream.
   *
   * @return The number of bytes (uncompressed) read from the input directory.
   *
   * @throws IOException
   */
  public static long zip(File directory, OutputStream out) throws IOException
  {
    if (!directory.isDirectory()) {
      throw new IOE("directory[%s] is not a directory", directory);
    }

    final ZipOutputStream zipOut = new ZipOutputStream(out);

    long totalSize = 0;
    for (File file : directory.listFiles()) {
      log.info("Adding file[%s] with size[%,d].  Total size so far[%,d]", file, file.length(), totalSize);
      if (file.length() >= Integer.MAX_VALUE) {
        zipOut.finish();
        throw new IOE("file[%s] too large [%,d]", file, file.length());
      }
      zipOut.putNextEntry(new ZipEntry(file.getName()));
      totalSize += Files.asByteSource(file).copyTo(zipOut);
    }
    zipOut.closeEntry();
    // Workaround for http://hg.openjdk.java.net/jdk8/jdk8/jdk/rev/759aa847dcaf
    zipOut.flush();
    zipOut.finish();

    return totalSize;
  }

  /**
   * Unzip the byteSource to the output directory. If cacheLocally is true, the byteSource is cached to local disk before unzipping.
   * This may cause more predictable behavior than trying to unzip a large file directly off a network stream, for example.
   * * @param byteSource The ByteSource which supplies the zip data
   *
   * @param byteSource   The ByteSource which supplies the zip data
   * @param outDir       The output directory to put the contents of the zip
   * @param shouldRetry  A predicate expression to determine if a new InputStream should be acquired from ByteSource and the copy attempted again
   * @param cacheLocally A boolean flag to indicate if the data should be cached locally
   *
   * @return A FileCopyResult containing the result of writing the zip entries to disk
   *
   * @throws IOException
   */
  public static FileUtils.FileCopyResult unzip(
      final ByteSource byteSource,
      final File outDir,
      final Predicate<Throwable> shouldRetry,
      boolean cacheLocally
  ) throws IOException
  {
    if (!cacheLocally) {
      try {
        return RetryUtils.retry(
            () -> unzip(byteSource.openStream(), outDir),
            shouldRetry,
            DEFAULT_RETRY_COUNT
        );
      }
      catch (Exception e) {
        throw Throwables.propagate(e);
      }
    } else {
      final File tmpFile = File.createTempFile("compressionUtilZipCache", ZIP_SUFFIX);
      try {
        FileUtils.retryCopy(
            byteSource,
            tmpFile,
            shouldRetry,
            DEFAULT_RETRY_COUNT
        );
        return unzip(tmpFile, outDir);
      }
      finally {
        if (!tmpFile.delete()) {
          log.warn("Could not delete zip cache at [%s]", tmpFile.toString());
        }
      }
    }
  }

  /**
   * Unzip the byteSource to the output directory. If cacheLocally is true, the byteSource is cached to local disk before unzipping.
   * This may cause more predictable behavior than trying to unzip a large file directly off a network stream, for example.
   *
   * @param byteSource   The ByteSource which supplies the zip data
   * @param outDir       The output directory to put the contents of the zip
   * @param cacheLocally A boolean flag to indicate if the data should be cached locally
   *
   * @return A FileCopyResult containing the result of writing the zip entries to disk
   *
   * @throws IOException
   */
  public static FileUtils.FileCopyResult unzip(
      final ByteSource byteSource,
      final File outDir,
      boolean cacheLocally
  ) throws IOException
  {
    return unzip(byteSource, outDir, FileUtils.IS_EXCEPTION, cacheLocally);
  }

  /**
   * Unzip the pulled file to an output directory. This is only expected to work on zips with lone files, and is not intended for zips with directory structures.
   *
   * @param pulledFile The file to unzip
   * @param outDir     The directory to store the contents of the file.
   *
   * @return a FileCopyResult of the files which were written to disk
   *
   * @throws IOException
   */
  public static FileUtils.FileCopyResult unzip(final File pulledFile, final File outDir) throws IOException
  {
    if (!(outDir.exists() && outDir.isDirectory())) {
      throw new ISE("outDir[%s] must exist and be a directory", outDir);
    }
    log.info("Unzipping file[%s] to [%s]", pulledFile, outDir);
    final FileUtils.FileCopyResult result = new FileUtils.FileCopyResult();
    try (final ZipFile zipFile = new ZipFile(pulledFile)) {
      final Enumeration<? extends ZipEntry> enumeration = zipFile.entries();
      while (enumeration.hasMoreElements()) {
        final ZipEntry entry = enumeration.nextElement();
        final File outFile = new File(outDir, entry.getName());
        result.addFiles(
            FileUtils.retryCopy(
                new ByteSource()
                {
                  @Override
                  public InputStream openStream() throws IOException
                  {
                    return new BufferedInputStream(zipFile.getInputStream(entry));
                  }
                },
                outFile,
                FileUtils.IS_EXCEPTION,
                DEFAULT_RETRY_COUNT
            ).getFiles()
        );
      }
    }
    return result;
  }

  /**
   * Unzip from the input stream to the output directory, using the entry's file name as the file name in the output directory.
   * The behavior of directories in the input stream's zip is undefined.
   * If possible, it is recommended to use unzip(ByteStream, File) instead
   *
   * @param in     The input stream of the zip data. This stream is closed
   * @param outDir The directory to copy the unzipped data to
   *
   * @return The FileUtils.FileCopyResult containing information on all the files which were written
   *
   * @throws IOException
   */
  public static FileUtils.FileCopyResult unzip(InputStream in, File outDir) throws IOException
  {
    try (final ZipInputStream zipIn = new ZipInputStream(in)) {
      final FileUtils.FileCopyResult result = new FileUtils.FileCopyResult();
      ZipEntry entry;
      while ((entry = zipIn.getNextEntry()) != null) {
        final File file = new File(outDir, entry.getName());

        NativeIO.chunkedCopy(zipIn, file);

        result.addFile(file);
        zipIn.closeEntry();
      }
      return result;
    }
  }

  /**
   * gunzip the file to the output file.
   *
   * @param pulledFile The source of the gz data
   * @param outFile    A target file to put the contents
   *
   * @return The result of the file copy
   *
   * @throws IOException
   */
  public static FileUtils.FileCopyResult gunzip(final File pulledFile, File outFile)
  {
    return gunzip(Files.asByteSource(pulledFile), outFile);
  }

  /**
   * Unzips the input stream via a gzip filter. use gunzip(ByteSource, File, Predicate) if possible
   *
   * @param in      The input stream to run through the gunzip filter. This stream is closed
   * @param outFile The file to output to
   *
   * @throws IOException
   */
  public static FileUtils.FileCopyResult gunzip(InputStream in, File outFile) throws IOException
  {
    try (GZIPInputStream gzipInputStream = gzipInputStream(in)) {
      NativeIO.chunkedCopy(gzipInputStream, outFile);
      return new FileUtils.FileCopyResult(outFile);
    }
  }

  /**
   * Fixes java bug 7036144 http://bugs.java.com/bugdatabase/view_bug.do?bug_id=7036144 which affects concatenated GZip
   *
   * @param in The raw input stream
   *
   * @return A GZIPInputStream that can handle concatenated gzip streams in the input
   */
  private static GZIPInputStream gzipInputStream(final InputStream in) throws IOException
  {
    return new GZIPInputStream(
        new FilterInputStream(in)
        {
          @Override
          public int available() throws IOException
          {
            final int otherAvailable = super.available();
            // Hack. Docs say available() should return an estimate,
            // so we estimate about 1KB to work around available == 0 bug in GZIPInputStream
            return otherAvailable == 0 ? 1 << 10 : otherAvailable;
          }
        }
    );
  }

  /**
   * gunzip from the source stream to the destination stream.
   *
   * @param in  The input stream which is to be decompressed. This stream is closed.
   * @param out The output stream to write to. This stream is closed
   *
   * @return The number of bytes written to the output stream.
   *
   * @throws IOException
   */
  public static long gunzip(InputStream in, OutputStream out) throws IOException
  {
    try (GZIPInputStream gzipInputStream = gzipInputStream(in)) {
      final long result = ByteStreams.copy(gzipInputStream, out);
      out.flush();
      return result;
    }
    finally {
      out.close();
    }
  }

  /**
   * A gunzip function to store locally
   *
   * @param in          The factory to produce input streams
   * @param outFile     The file to store the result into
   * @param shouldRetry A predicate to indicate if the Throwable is recoverable
   *
   * @return The count of bytes written to outFile
   */
  public static FileUtils.FileCopyResult gunzip(
      final ByteSource in,
      final File outFile,
      Predicate<Throwable> shouldRetry
  )
  {
    return FileUtils.retryCopy(
        new ByteSource()
        {
          @Override
          public InputStream openStream() throws IOException
          {
            return gzipInputStream(in.openStream());
          }
        },
        outFile,
        shouldRetry,
        DEFAULT_RETRY_COUNT
    );
  }


  /**
   * Gunzip from the input stream to the output file
   *
   * @param in      The compressed input stream to read from
   * @param outFile The file to write the uncompressed results to
   *
   * @return A FileCopyResult of the file written
   */
  public static FileUtils.FileCopyResult gunzip(final ByteSource in, File outFile)
  {
    return gunzip(in, outFile, FileUtils.IS_EXCEPTION);
  }

  /**
   * Copy inputStream to out while wrapping out in a GZIPOutputStream
   * Closes both input and output
   *
   * @param inputStream The input stream to copy data from. This stream is closed
   * @param out         The output stream to wrap in a GZIPOutputStream before copying. This stream is closed
   *
   * @return The size of the data copied
   *
   * @throws IOException
   */
  public static long gzip(InputStream inputStream, OutputStream out) throws IOException
  {
    try (GZIPOutputStream outputStream = new GZIPOutputStream(out)) {
      final long result = ByteStreams.copy(inputStream, outputStream);
      out.flush();
      return result;
    }
    finally {
      inputStream.close();
    }
  }

  /**
   * Gzips the input file to the output
   *
   * @param inFile      The file to gzip
   * @param outFile     A target file to copy the uncompressed contents of inFile to
   * @param shouldRetry Predicate on a potential throwable to determine if the copy should be attempted again.
   *
   * @return The result of the file copy
   *
   * @throws IOException
   */
  public static FileUtils.FileCopyResult gzip(final File inFile, final File outFile, Predicate<Throwable> shouldRetry)
  {
    gzip(Files.asByteSource(inFile), Files.asByteSink(outFile), shouldRetry);
    return new FileUtils.FileCopyResult(outFile);
  }

  public static long gzip(final ByteSource in, final ByteSink out, Predicate<Throwable> shouldRetry)
  {
    return StreamUtils.retryCopy(
        in,
        new ByteSink()
        {
          @Override
          public OutputStream openStream() throws IOException
          {
            return new GZIPOutputStream(out.openStream());
          }
        },
        shouldRetry,
        DEFAULT_RETRY_COUNT
    );
  }


  /**
   * GZip compress the contents of inFile into outFile
   *
   * @param inFile  The source of data
   * @param outFile The destination for compressed data
   *
   * @return A FileCopyResult of the resulting file at outFile
   *
   * @throws IOException
   */
  public static FileUtils.FileCopyResult gzip(final File inFile, final File outFile)
  {
    return gzip(inFile, outFile, FileUtils.IS_EXCEPTION);
  }

  /**
   * Checks to see if fName is a valid name for a "*.zip" file
   *
   * @param fName The name of the file in question
   *
   * @return True if fName is properly named for a .zip file, false otherwise
   */
  public static boolean isZip(String fName)
  {
    if (Strings.isNullOrEmpty(fName)) {
      return false;
    }
    return fName.endsWith(ZIP_SUFFIX); // Technically a file named `.zip` would be fine
  }

  /**
   * Checks to see if fName is a valid name for a "*.gz" file
   *
   * @param fName The name of the file in question
   *
   * @return True if fName is a properly named .gz file, false otherwise
   */
  public static boolean isGz(String fName)
  {
    if (Strings.isNullOrEmpty(fName)) {
      return false;
    }
    return fName.endsWith(GZ_SUFFIX) && fName.length() > GZ_SUFFIX.length();
  }

  /**
   * Get the file name without the .gz extension
   *
   * @param fname The name of the gzip file
   *
   * @return fname without the ".gz" extension
   *
   * @throws IAE if fname is not a valid "*.gz" file name
   */
  public static String getGzBaseName(String fname)
  {
    final String reducedFname = Files.getNameWithoutExtension(fname);
    if (isGz(fname) && !reducedFname.isEmpty()) {
      return reducedFname;
    }
    throw new IAE("[%s] is not a valid gz file name", fname);
  }

  /**
   * Decompress an input stream from a file, based on the filename.
   */
  public static InputStream decompress(final InputStream in, final String fileName) throws IOException
  {
    if (fileName.endsWith(GZ_SUFFIX)) {
      return gzipInputStream(in);
    } else if (fileName.endsWith(BZ2_SUFFIX)) {
      return new BZip2CompressorInputStream(in, true);
    } else if (fileName.endsWith(XZ_SUFFIX)) {
      return new XZCompressorInputStream(in, true);
    } else if (fileName.endsWith(ZIP_SUFFIX)) {
      // This reads the first file in the archive.
      final ZipInputStream zipIn = new ZipInputStream(in, StandardCharsets.UTF_8);
      try {
        final ZipEntry nextEntry = zipIn.getNextEntry();
        if (nextEntry == null) {
          zipIn.close();

          // No files in the archive - return an empty stream.
          return new ByteArrayInputStream(new byte[0]);
        }
        return zipIn;
      }
      catch (IOException e) {
        try {
          zipIn.close();
        }
        catch (IOException e2) {
          e.addSuppressed(e2);
        }
        throw e;
      }
    } else {
      return in;
    }
  }
}
