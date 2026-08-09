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

package org.apache.druid.java.util.common;

import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.ByteSink;
import com.google.common.io.ByteSource;
import com.google.common.io.ByteStreams;
import com.google.common.io.CountingInputStream;
import com.google.common.io.Files;
import net.jpountz.lz4.LZ4BlockOutputStream;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream;
import org.apache.commons.compress.compressors.snappy.FramedSnappyCompressorOutputStream;
import org.apache.commons.compress.compressors.xz.XZCompressorOutputStream;
import org.apache.commons.compress.compressors.zstandard.ZstdCompressorOutputStream;
import org.apache.druid.testing.JupiterAssertions;
import org.apache.druid.testing.TempDirExtension;
import org.apache.druid.utils.CompressionUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FilterInputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Scanner;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

public class CompressionUtilsTest
{
  private static final String CONTENT;
  private static final byte[] EXPECTED;
  private static final byte[] GZ_BYTES;

  static {
    final StringBuilder builder = new StringBuilder();
    try (
        final InputStream stream = Objects.requireNonNull(
            CompressionUtilsTest.class.getClassLoader().getResourceAsStream("white-rabbit.txt"),
            "Missing test resource: white-rabbit.txt"
        );
        final InputStreamReader reader = new InputStreamReader(stream, StandardCharsets.UTF_8);
        final Scanner scanner = new Scanner(reader).useDelimiter(Pattern.quote(System.lineSeparator()))
    ) {
      final Iterator<String> it = scanner;
      while (it.hasNext()) {
        builder.append(it.next());
      }
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
    CONTENT = builder.toString();
    EXPECTED = StringUtils.toUtf8(CONTENT);

    final ByteArrayOutputStream gzByteStream = new ByteArrayOutputStream(EXPECTED.length);
    try (GZIPOutputStream outputStream = new GZIPOutputStream(gzByteStream)) {
      try (ByteArrayInputStream in = new ByteArrayInputStream(EXPECTED)) {
        ByteStreams.copy(in, outputStream);
      }
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
    GZ_BYTES = gzByteStream.toByteArray();
  }

  @RegisterExtension
  public final TempDirExtension temporaryFolder = new TempDirExtension();
  private File testDir;
  private File testFile;

  public static void assertGoodDataStream(InputStream stream) throws IOException
  {
    try (final ByteArrayOutputStream bos = new ByteArrayOutputStream(EXPECTED.length)) {
      ByteStreams.copy(stream, bos);
      JupiterAssertions.assertArrayEquals(EXPECTED, bos.toByteArray());
    }
  }

  @BeforeEach
  public void setUp() throws IOException
  {
    testDir = temporaryFolder.newFolder("testDir");
    testFile = new File(testDir, "test.dat");
    try (OutputStream outputStream = new FileOutputStream(testFile)) {
      outputStream.write(StringUtils.toUtf8(CONTENT));
    }
    JupiterAssertions.assertTrue(testFile.getParentFile().equals(testDir));
  }

  @Test
  public void testGoodGzNameResolution()
  {
    JupiterAssertions.assertEquals("foo", CompressionUtils.getGzBaseName("foo.gz"));
  }

  @Test
  @org.apache.druid.testing.ExpectThrows(IAE.class)
  public void testBadGzName()
  {
    CompressionUtils.getGzBaseName("foo");
  }


  @Test
  @org.apache.druid.testing.ExpectThrows(IAE.class)
  public void testBadShortGzName()
  {
    CompressionUtils.getGzBaseName(".gz");
  }

  @Test
  public void testGoodZipCompressUncompress() throws IOException
  {
    final File tmpDir = temporaryFolder.newFolder("testGoodZipCompressUncompress");
    final File zipFile = new File(tmpDir, "compressionUtilTest.zip");

    CompressionUtils.zip(testDir, zipFile);
    final File newDir = new File(tmpDir, "newDir");
    newDir.mkdir();
    final FileUtils.FileCopyResult result = CompressionUtils.unzip(zipFile, newDir);

    verifyUnzip(newDir, result, ImmutableMap.of(testFile.getName(), StringUtils.toUtf8(CONTENT)));
  }


  @Test
  public void testGoodZipCompressUncompressWithLocalCopy() throws IOException
  {
    final File tmpDir = temporaryFolder.newFolder("testGoodZipCompressUncompressWithLocalCopy");
    final File zipFile = new File(tmpDir, "testGoodZipCompressUncompressWithLocalCopy.zip");
    CompressionUtils.zip(testDir, zipFile);
    final File newDir = new File(tmpDir, "newDir");
    newDir.mkdir();

    final FileUtils.FileCopyResult result = CompressionUtils.unzip(
        new ByteSource()
        {
          @Override
          public InputStream openStream() throws IOException
          {
            return new FileInputStream(zipFile);
          }
        },
        newDir,
        FileUtils.IS_EXCEPTION,
        true
    );

    verifyUnzip(newDir, result, ImmutableMap.of(testFile.getName(), StringUtils.toUtf8(CONTENT)));
  }

  @Test
  public void testGoodGZCompressUncompressToFile() throws Exception
  {
    final File tmpDir = temporaryFolder.newFolder("testGoodGZCompressUncompressToFile");
    final File gzFile = new File(tmpDir, testFile.getName() + ".gz");
    JupiterAssertions.assertFalse(gzFile.exists());
    CompressionUtils.gzip(testFile, gzFile);
    JupiterAssertions.assertTrue(gzFile.exists());
    try (
        final InputStream fileInputStream = new FileInputStream(gzFile);
        final InputStream inputStream = new GZIPInputStream(fileInputStream)
    ) {
      assertGoodDataStream(inputStream);
    }
    testFile.delete();
    JupiterAssertions.assertFalse(testFile.exists());
    CompressionUtils.gunzip(gzFile, testFile);
    JupiterAssertions.assertTrue(testFile.exists());
    try (final InputStream inputStream = new FileInputStream(testFile)) {
      assertGoodDataStream(inputStream);
    }
  }

  @Test
  public void testGoodZipStream() throws IOException
  {
    final File tmpDir = temporaryFolder.newFolder("testGoodZipStream");
    final File zipFile = new File(tmpDir, "compressionUtilTest.zip");
    try (final OutputStream outputStream = new FileOutputStream(zipFile)) {
      CompressionUtils.zip(testDir, outputStream);
    }
    final File newDir = new File(tmpDir, "newDir");
    newDir.mkdir();
    final FileUtils.FileCopyResult result;
    try (final InputStream inputStream = new FileInputStream(zipFile)) {
      result = CompressionUtils.unzip(inputStream, newDir);
    }
    verifyUnzip(newDir, result, ImmutableMap.of(testFile.getName(), StringUtils.toUtf8(CONTENT)));
  }

  private Map<String, byte[]> writeZipWithManyFiles(final File zipFile) throws IOException
  {
    final File srcDir = temporaryFolder.newFolder();

    final Map<String, byte[]> expectedFiles = new HashMap<>();

    for (int i = 0; i < 100; i++) {
      final String filePath = "file" + i;

      try (final FileOutputStream out = new FileOutputStream(new File(srcDir, filePath))) {
        out.write(i);
        expectedFiles.put(filePath, new byte[]{(byte) i});
      }
    }

    try (final OutputStream outputStream = new FileOutputStream(zipFile)) {
      CompressionUtils.zip(srcDir, outputStream);
    }

    return expectedFiles;
  }

  @Test
  public void testZipWithManyFiles() throws IOException
  {
    final File tmpDir = temporaryFolder.newFolder("testZipWithManyFilesStream");
    final File zipFile = new File(tmpDir, "compressionUtilTest.zip");

    final Map<String, byte[]> expectedFiles = writeZipWithManyFiles(zipFile);

    final File unzipDir = new File(tmpDir, "unzipDir");
    unzipDir.mkdir();

    final FileUtils.FileCopyResult result = CompressionUtils.unzip(zipFile, unzipDir);
    verifyUnzip(unzipDir, result, expectedFiles);
  }

  @Test
  public void testZipWithManyFilesStreamWithLocalCopy() throws IOException
  {
    final File tmpDir = temporaryFolder.newFolder("testZipWithManyFilesStream");
    final File zipFile = new File(tmpDir, "compressionUtilTest.zip");

    final Map<String, byte[]> expectedFiles = writeZipWithManyFiles(zipFile);

    final File unzipDir = new File(tmpDir, "unzipDir");
    unzipDir.mkdir();

    final FileUtils.FileCopyResult result = CompressionUtils.unzip(
        new ByteSource()
        {
          @Override
          public InputStream openStream() throws IOException
          {
            return new FileInputStream(zipFile);
          }
        },
        unzipDir,
        FileUtils.IS_EXCEPTION,
        true
    );
    verifyUnzip(unzipDir, result, expectedFiles);
  }

  @Test
  public void testZipWithManyFilesStream() throws IOException
  {
    final File tmpDir = temporaryFolder.newFolder("testZipWithManyFilesStream");
    final File zipFile = new File(tmpDir, "compressionUtilTest.zip");

    final Map<String, byte[]> expectedFiles = writeZipWithManyFiles(zipFile);

    final File unzipDir = new File(tmpDir, "unzipDir");
    unzipDir.mkdir();

    try (final CountingInputStream zipIn = new CountingInputStream(new FileInputStream(zipFile))) {
      final FileUtils.FileCopyResult result = CompressionUtils.unzip(zipIn, unzipDir);

      verifyUnzip(unzipDir, result, expectedFiles);

      // Check that all bytes were read from the stream
      JupiterAssertions.assertEquals(zipFile.length(), zipIn.getCount());
    }
  }

  @Test
  public void testGoodGzipByteSource() throws IOException
  {
    final File tmpDir = temporaryFolder.newFolder("testGoodGzipByteSource");
    final File gzFile = new File(tmpDir, testFile.getName() + ".gz");
    JupiterAssertions.assertFalse(gzFile.exists());
    CompressionUtils.gzip(Files.asByteSource(testFile), Files.asByteSink(gzFile), Predicates.alwaysTrue());
    JupiterAssertions.assertTrue(gzFile.exists());
    try (
        final InputStream fileInputStream = new FileInputStream(gzFile);
        final InputStream inputStream = CompressionUtils.decompress(fileInputStream, gzFile.getName())
    ) {
      assertGoodDataStream(inputStream);
    }
    if (!testFile.delete()) {
      throw new IOE("Unable to delete file [%s]", testFile.getAbsolutePath());
    }
    JupiterAssertions.assertFalse(testFile.exists());
    CompressionUtils.gunzip(Files.asByteSource(gzFile), testFile);
    JupiterAssertions.assertTrue(testFile.exists());
    try (final InputStream inputStream = new FileInputStream(testFile)) {
      assertGoodDataStream(inputStream);
    }
  }

  @Test
  public void testDecompressBzip2() throws IOException
  {
    final File tmpDir = temporaryFolder.newFolder("testDecompressBzip2");
    final File bzFile = new File(tmpDir, testFile.getName() + ".bz2");
    JupiterAssertions.assertFalse(bzFile.exists());
    try (
        final OutputStream fileOutputStream = new FileOutputStream(bzFile);
        final OutputStream out = new BZip2CompressorOutputStream(fileOutputStream);
        final InputStream in = new FileInputStream(testFile)
    ) {
      ByteStreams.copy(in, out);
    }
    try (
        final InputStream fileInputStream = new FileInputStream(bzFile);
        final InputStream inputStream = CompressionUtils.decompress(fileInputStream, bzFile.getName())
    ) {
      assertGoodDataStream(inputStream);
    }
  }

  @Test
  public void testDecompressXz() throws IOException
  {
    final File tmpDir = temporaryFolder.newFolder("testDecompressXz");
    final File xzFile = new File(tmpDir, testFile.getName() + ".xz");
    JupiterAssertions.assertFalse(xzFile.exists());
    try (
        final OutputStream fileOutputStream = new FileOutputStream(xzFile);
        final OutputStream out = new XZCompressorOutputStream(fileOutputStream);
        final InputStream in = new FileInputStream(testFile)
    ) {
      ByteStreams.copy(in, out);
    }
    try (
        final InputStream fileInputStream = new FileInputStream(xzFile);
        final InputStream inputStream = CompressionUtils.decompress(fileInputStream, xzFile.getName())
    ) {
      assertGoodDataStream(inputStream);
    }
  }

  @Test
  public void testDecompressSnappy() throws IOException
  {
    final File tmpDir = temporaryFolder.newFolder("testDecompressSnappy");
    final File snappyFile = new File(tmpDir, testFile.getName() + ".sz");
    JupiterAssertions.assertFalse(snappyFile.exists());
    try (
        final OutputStream fileOutputStream = new FileOutputStream(snappyFile);
        final OutputStream out = new FramedSnappyCompressorOutputStream(fileOutputStream);
        final InputStream in = new FileInputStream(testFile)
    ) {
      ByteStreams.copy(in, out);
    }
    try (
        final InputStream fileInputStream = new FileInputStream(snappyFile);
        final InputStream inputStream = CompressionUtils.decompress(fileInputStream, snappyFile.getName())
    ) {
      assertGoodDataStream(inputStream);
    }
  }

  @Test
  public void testDecompressZstd() throws IOException
  {
    final File tmpDir = temporaryFolder.newFolder("testDecompressZstd");
    final File zstdFile = new File(tmpDir, testFile.getName() + ".zst");
    JupiterAssertions.assertFalse(zstdFile.exists());
    try (
        final OutputStream fileOutputStream = new FileOutputStream(zstdFile);
        final OutputStream out = new ZstdCompressorOutputStream(fileOutputStream);
        final InputStream in = new FileInputStream(testFile)
    ) {
      ByteStreams.copy(in, out);
    }
    try (
        final InputStream fileInputStream = new FileInputStream(zstdFile);
        final InputStream inputStream = CompressionUtils.decompress(fileInputStream, zstdFile.getName())
    ) {
      assertGoodDataStream(inputStream);
    }
  }

  @Test
  public void testDecompressZip() throws IOException
  {
    final File tmpDir = temporaryFolder.newFolder("testDecompressZip");
    final File zipFile = new File(tmpDir, testFile.getName() + ".zip");
    JupiterAssertions.assertFalse(zipFile.exists());
    try (
        final OutputStream fileOutputStream = new FileOutputStream(zipFile);
        final ZipOutputStream out = new ZipOutputStream(fileOutputStream);
        final InputStream in = new FileInputStream(testFile)
    ) {
      out.putNextEntry(new ZipEntry("cool.file"));
      ByteStreams.copy(in, out);
      out.closeEntry();
    }
    try (
        final InputStream fileInputStream = new FileInputStream(zipFile);
        final InputStream inputStream = CompressionUtils.decompress(fileInputStream, zipFile.getName())
    ) {
      assertGoodDataStream(inputStream);
    }
  }

  @Test
  public void testDecompressZipWithManyFiles() throws IOException
  {
    final File tmpDir = temporaryFolder.newFolder("testDecompressZip");
    final File zipFile = new File(tmpDir, testFile.getName() + ".zip");
    writeZipWithManyFiles(zipFile);

    try (
        final InputStream fileInputStream = new FileInputStream(zipFile);
        final InputStream inputStream = CompressionUtils.decompress(fileInputStream, zipFile.getName())
    ) {
      // Should read the first file, which contains a single null byte.
      JupiterAssertions.assertArrayEquals(new byte[]{0}, ByteStreams.toByteArray(inputStream));
    }
  }

  @Test
  public void testGoodLz4CompressUncompressDirectory() throws IOException
  {
    final File tmpDir = temporaryFolder.newFolder("testGoodLz4CompressUncompressDirectory");
    final File lz4File = new File(tmpDir, "compressionUtilTest.lz4");

    try (final OutputStream out = new FileOutputStream(lz4File)) {
      CompressionUtils.Format.LZ4.compressDirectory(testDir, out);
    }

    final File newDir = new File(tmpDir, "newDir");
    JupiterAssertions.assertTrue(newDir.mkdir());

    final FileUtils.FileCopyResult result;
    try (final InputStream in = new FileInputStream(lz4File)) {
      result = CompressionUtils.Format.LZ4.decompressDirectory(in, newDir);
    }

    verifyUnzip(newDir, result, ImmutableMap.of(testFile.getName(), StringUtils.toUtf8(CONTENT)));
  }

  @Test
  public void testDecompressLz4() throws IOException
  {
    final File tmpDir = temporaryFolder.newFolder("testDecompressLz4");
    final File lz4File = new File(tmpDir, testFile.getName() + ".lz4");
    JupiterAssertions.assertFalse(lz4File.exists());

    try (
        final OutputStream out = new LZ4BlockOutputStream(new FileOutputStream(lz4File));
        final InputStream in = new FileInputStream(testFile)
    ) {
      ByteStreams.copy(in, out);
    }

    try (final InputStream inputStream = CompressionUtils.decompress(new FileInputStream(lz4File), lz4File.getName())) {
      assertGoodDataStream(inputStream);
    }
  }

  @Test
  public void testGoodGZStream() throws IOException
  {
    final File tmpDir = temporaryFolder.newFolder("testGoodGZStream");
    final File gzFile = new File(tmpDir, testFile.getName() + ".gz");
    JupiterAssertions.assertFalse(gzFile.exists());
    try (
        final InputStream inputStream = new FileInputStream(testFile);
        final OutputStream outputStream = new FileOutputStream(gzFile)
    ) {
      CompressionUtils.gzip(inputStream, outputStream);
    }
    JupiterAssertions.assertTrue(gzFile.exists());
    try (
        final InputStream fileInputStream = new FileInputStream(gzFile);
        final InputStream inputStream = new GZIPInputStream(fileInputStream)
    ) {
      assertGoodDataStream(inputStream);
    }
    if (!testFile.delete()) {
      throw new IOE("Unable to delete file [%s]", testFile.getAbsolutePath());
    }
    JupiterAssertions.assertFalse(testFile.exists());
    try (final InputStream inputStream = new FileInputStream(gzFile)) {
      CompressionUtils.gunzip(inputStream, testFile);
    }
    JupiterAssertions.assertTrue(testFile.exists());
    try (final InputStream inputStream = new FileInputStream(testFile)) {
      assertGoodDataStream(inputStream);
    }
  }

  @Test
  public void testEvilZip() throws IOException
  {
    final File tmpDir = temporaryFolder.newFolder("testEvilZip");

    final File evilResult = new File("/tmp/evil.txt");
    java.nio.file.Files.deleteIfExists(evilResult.toPath());

    File evilZip = new File(tmpDir, "evil.zip");
    java.nio.file.Files.deleteIfExists(evilZip.toPath());
    CompressionUtilsTest.makeEvilZip(evilZip);

    try {
      CompressionUtils.unzip(evilZip, tmpDir);
    }
    catch (ISE ise) {
      JupiterAssertions.assertTrue(ise.getMessage().contains("does not start with outDir"));
      JupiterAssertions.assertFalse("Zip exploit triggered, /tmp/evil.txt was written.", evilResult.exists());
      return;
    }
    JupiterAssertions.fail("Exception was not thrown for malicious zip file");
  }

  @Test
  public void testEvilZipInputStream() throws IOException
  {
    final File tmpDir = temporaryFolder.newFolder("testEvilZip");

    final File evilResult = new File("/tmp/evil.txt");
    java.nio.file.Files.deleteIfExists(evilResult.toPath());

    File evilZip = new File(tmpDir, "evil.zip");
    java.nio.file.Files.deleteIfExists(evilZip.toPath());
    CompressionUtilsTest.makeEvilZip(evilZip);

    try (final InputStream inputStream = new FileInputStream(evilZip)) {
      CompressionUtils.unzip(inputStream, tmpDir);
    }
    catch (ISE ise) {
      JupiterAssertions.assertTrue(ise.getMessage().contains("does not start with outDir"));
      JupiterAssertions.assertFalse("Zip exploit triggered, /tmp/evil.txt was written.", evilResult.exists());
      return;
    }
    JupiterAssertions.fail("Exception was not thrown for malicious zip file");
  }

  @Test
  public void testEvilZipInputStreamWithLocalCopy() throws IOException
  {
    final File tmpDir = temporaryFolder.newFolder("testEvilZip");

    final File evilResult = new File("/tmp/evil.txt");
    java.nio.file.Files.deleteIfExists(evilResult.toPath());

    File evilZip = new File(tmpDir, "evil.zip");
    java.nio.file.Files.deleteIfExists(evilZip.toPath());
    CompressionUtilsTest.makeEvilZip(evilZip);

    try {
      CompressionUtils.unzip(
          new ByteSource()
          {
            @Override
            public InputStream openStream() throws IOException
            {
              return new FileInputStream(evilZip);
            }
          },
          tmpDir,
          FileUtils.IS_EXCEPTION,
          true
      );
    }
    catch (ISE ise) {
      JupiterAssertions.assertTrue(ise.getMessage().contains("does not start with outDir"));
      JupiterAssertions.assertFalse("Zip exploit triggered, /tmp/evil.txt was written.", evilResult.exists());
      return;
    }
    JupiterAssertions.fail("Exception was not thrown for malicious zip file");
  }

  @Test
  // Sanity check to make sure the test class works as expected
  public void testZeroRemainingInputStream() throws IOException
  {
    try (OutputStream outputStream = new FileOutputStream(testFile)) {
      JupiterAssertions.assertEquals(
          GZ_BYTES.length,
          ByteStreams.copy(
              new ZeroRemainingInputStream(new ByteArrayInputStream(GZ_BYTES)),
              outputStream
          )
      );
      JupiterAssertions.assertEquals(
          GZ_BYTES.length,
          ByteStreams.copy(
              new ZeroRemainingInputStream(new ByteArrayInputStream(GZ_BYTES)),
              outputStream
          )
      );
      JupiterAssertions.assertEquals(
          GZ_BYTES.length,
          ByteStreams.copy(
              new ZeroRemainingInputStream(new ByteArrayInputStream(GZ_BYTES)),
              outputStream
          )
      );
    }
    JupiterAssertions.assertEquals(GZ_BYTES.length * 3, testFile.length());
    try (InputStream inputStream = new ZeroRemainingInputStream(new FileInputStream(testFile))) {
      for (int i = 0; i < 3; ++i) {
        final byte[] bytes = new byte[GZ_BYTES.length];
        JupiterAssertions.assertEquals(bytes.length, inputStream.read(bytes));
        JupiterAssertions.assertArrayEquals(
            StringUtils.format("Failed on range %d", i),
            GZ_BYTES,
            bytes
        );
      }
    }
  }

  @Test
  public void testGunzipConcatenatedByteSource() throws IOException
  {
    testFile.delete();
    JupiterAssertions.assertFalse(testFile.exists());

    final ByteArrayOutputStream tripleGzByteStream = new ByteArrayOutputStream(GZ_BYTES.length * 3);
    tripleGzByteStream.write(GZ_BYTES);
    tripleGzByteStream.write(GZ_BYTES);
    tripleGzByteStream.write(GZ_BYTES);

    final ByteSource inputStreamFactory = new ByteSource()
    {
      @Override
      public InputStream openStream()
      {
        return new ZeroRemainingInputStream(new ByteArrayInputStream(tripleGzByteStream.toByteArray()));
      }
    };

    JupiterAssertions.assertEquals((long) (EXPECTED.length * 3), CompressionUtils.gunzip(inputStreamFactory, testFile).size());

    try (final InputStream inputStream = new FileInputStream(testFile)) {
      try (final ByteArrayOutputStream outputStream = new ByteArrayOutputStream(EXPECTED.length * 3)) {
        JupiterAssertions.assertEquals(
            "Read terminated too soon",
            EXPECTED.length * 3,
            ByteStreams.copy(inputStream, outputStream)
        );
        final byte[] found = outputStream.toByteArray();
        JupiterAssertions.assertEquals(EXPECTED.length * 3, found.length);
        JupiterAssertions.assertArrayEquals(EXPECTED, Arrays.copyOfRange(found, EXPECTED.length * 0, EXPECTED.length * 1));
        JupiterAssertions.assertArrayEquals(EXPECTED, Arrays.copyOfRange(found, EXPECTED.length * 1, EXPECTED.length * 2));
        JupiterAssertions.assertArrayEquals(EXPECTED, Arrays.copyOfRange(found, EXPECTED.length * 2, EXPECTED.length * 3));
      }
    }
  }

  @Test
  public void testGunzipConcatenatedStream() throws IOException
  {

    final ByteArrayOutputStream tripleGzByteStream = new ByteArrayOutputStream(GZ_BYTES.length * 3);
    tripleGzByteStream.write(GZ_BYTES);
    tripleGzByteStream.write(GZ_BYTES);
    tripleGzByteStream.write(GZ_BYTES);

    try (ByteArrayOutputStream bos = new ByteArrayOutputStream(EXPECTED.length * 3)) {
      JupiterAssertions.assertEquals(
          EXPECTED.length * 3,
          CompressionUtils.gunzip(
              new ZeroRemainingInputStream(
                  new ByteArrayInputStream(tripleGzByteStream.toByteArray())
              ), bos
          )
      );
      final byte[] found = bos.toByteArray();
      JupiterAssertions.assertEquals(EXPECTED.length * 3, found.length);
      JupiterAssertions.assertArrayEquals(EXPECTED, Arrays.copyOfRange(found, EXPECTED.length * 0, EXPECTED.length * 1));
      JupiterAssertions.assertArrayEquals(EXPECTED, Arrays.copyOfRange(found, EXPECTED.length * 1, EXPECTED.length * 2));
      JupiterAssertions.assertArrayEquals(EXPECTED, Arrays.copyOfRange(found, EXPECTED.length * 2, EXPECTED.length * 3));
    }
  }

  @Test
  public void testZipName() throws IOException
  {
    final File tmpDir = temporaryFolder.newFolder("testZipName");
    final File zipDir = new File(tmpDir, "zipDir");
    zipDir.mkdir();
    final File file = new File(tmpDir, "zipDir.zip");
    final Path unzipPath = Paths.get(zipDir.getPath(), "test.dat");
    file.delete();
    JupiterAssertions.assertFalse(file.exists());
    JupiterAssertions.assertFalse(unzipPath.toFile().exists());
    CompressionUtils.zip(testDir, file);
    JupiterAssertions.assertTrue(file.exists());
    CompressionUtils.unzip(file, zipDir);
    JupiterAssertions.assertTrue(unzipPath.toFile().exists());
    try (final FileInputStream inputStream = new FileInputStream(unzipPath.toFile())) {
      assertGoodDataStream(inputStream);
    }
  }

  @Test
  public void testNewFileDoesntCreateFile()
  {
    final File tmpFile = new File(testDir, "fofooofodshfudhfwdjkfwf.dat");
    JupiterAssertions.assertFalse(tmpFile.exists());
  }

  @Test
  public void testGoodGzipName()
  {
    JupiterAssertions.assertEquals("foo", CompressionUtils.getGzBaseName("foo.gz"));
  }

  @Test
  public void testGoodGzipNameWithPath()
  {
    JupiterAssertions.assertEquals("foo", CompressionUtils.getGzBaseName("/tar/ball/baz/bock/foo.gz"));
  }

  @Test
  @org.apache.druid.testing.ExpectThrows(IAE.class)
  public void testBadShortName()
  {
    CompressionUtils.getGzBaseName(".gz");
  }

  @Test
  @org.apache.druid.testing.ExpectThrows(IAE.class)
  public void testBadName()
  {
    CompressionUtils.getGzBaseName("BANANAS");
  }

  @Test
  @org.apache.druid.testing.ExpectThrows(IAE.class)
  public void testBadNameWithPath()
  {
    CompressionUtils.getGzBaseName("/foo/big/.gz");
  }

  @Test
  public void testGoodGzipWithException() throws Exception
  {
    final AtomicLong flushes = new AtomicLong(0);
    final File tmpDir = temporaryFolder.newFolder("testGoodGzipByteSource");
    final File gzFile = new File(tmpDir, testFile.getName() + ".gz");
    JupiterAssertions.assertFalse(gzFile.exists());
    CompressionUtils.gzip(
        Files.asByteSource(testFile), new ByteSink()
        {
          @Override
          public OutputStream openStream() throws IOException
          {
            return new FilterOutputStream(new FileOutputStream(gzFile))
            {
              @Override
              public void flush() throws IOException
              {
                if (flushes.getAndIncrement() > 0) {
                  super.flush();
                } else {
                  throw new IOException("Haven't flushed enough");
                }
              }
            };
          }
        }, Predicates.alwaysTrue()
    );
    JupiterAssertions.assertTrue(gzFile.exists());
    try (
        final InputStream fileInputStream = new FileInputStream(gzFile);
        final InputStream inputStream = CompressionUtils.decompress(fileInputStream, "file.gz")
    ) {
      assertGoodDataStream(inputStream);
    }
    if (!testFile.delete()) {
      throw new IOE("Unable to delete file [%s]", testFile.getAbsolutePath());
    }
    JupiterAssertions.assertFalse(testFile.exists());
    CompressionUtils.gunzip(Files.asByteSource(gzFile), testFile);
    JupiterAssertions.assertTrue(testFile.exists());
    try (final InputStream inputStream = new FileInputStream(testFile)) {
      assertGoodDataStream(inputStream);
    }
    JupiterAssertions.assertEquals(4, flushes.get()); // 2 for suppressed closes, 2 for manual calls to shake out errors
  }

  @Test
  @org.apache.druid.testing.ExpectThrows(IOException.class)
  public void testStreamErrorGzip() throws Exception
  {
    final File tmpDir = temporaryFolder.newFolder("testGoodGzipByteSource");
    final File gzFile = new File(tmpDir, testFile.getName() + ".gz");
    JupiterAssertions.assertFalse(gzFile.exists());
    final AtomicLong flushes = new AtomicLong(0L);
    try (
        final InputStream inputStream = new FileInputStream(testFile);
        final OutputStream outputStream = new FileOutputStream(gzFile)
        {
          @Override
          public void flush() throws IOException
          {
            if (flushes.getAndIncrement() > 0) {
              super.flush();
            } else {
              throw new IOException("Test exception");
            }
          }
        }
    ) {
      CompressionUtils.gzip(inputStream, outputStream);
    }
  }

  @Test
  @org.apache.druid.testing.ExpectThrows(IOException.class)
  public void testStreamErrorGunzip() throws Exception
  {
    final File tmpDir = temporaryFolder.newFolder("testGoodGzipByteSource");
    final File gzFile = new File(tmpDir, testFile.getName() + ".gz");
    JupiterAssertions.assertFalse(gzFile.exists());
    CompressionUtils.gzip(Files.asByteSource(testFile), Files.asByteSink(gzFile), Predicates.alwaysTrue());
    JupiterAssertions.assertTrue(gzFile.exists());
    try (
        final InputStream fileInputStream = new FileInputStream(gzFile);
        final InputStream inputStream = CompressionUtils.decompress(fileInputStream, "file.gz")
    ) {
      assertGoodDataStream(inputStream);
    }
    if (testFile.exists() && !testFile.delete()) {
      throw new RE("Unable to delete file [%s]", testFile.getAbsolutePath());
    }
    JupiterAssertions.assertFalse(testFile.exists());
    final AtomicLong flushes = new AtomicLong(0L);
    try (
        final InputStream inputStream = new FileInputStream(gzFile);
        final OutputStream outputStream = new FilterOutputStream(
            new FileOutputStream(testFile)
            {
              @Override
              public void flush() throws IOException
              {
                if (flushes.getAndIncrement() > 0) {
                  super.flush();
                } else {
                  throw new IOException("Test exception");
                }
              }
            }
        )
    ) {
      CompressionUtils.gunzip(inputStream, outputStream);
    }
  }

  private void verifyUnzip(
      final File unzipDir,
      final FileUtils.FileCopyResult result,
      final Map<String, byte[]> expectedFiles
  ) throws IOException
  {
    final List<String> filePaths = expectedFiles.keySet().stream().sorted().collect(Collectors.toList());

    // Check the FileCopyResult
    JupiterAssertions.assertEquals(expectedFiles.values().stream().mapToLong(arr -> arr.length).sum(), result.size());
    JupiterAssertions.assertEquals(
        filePaths.stream().map(filePath -> new File(unzipDir, filePath)).collect(Collectors.toList()),
        result.getFiles().stream().sorted().collect(Collectors.toList())
    );

    // Check the actual file list
    JupiterAssertions.assertEquals(
        filePaths,
        Arrays.stream(unzipDir.listFiles()).map(File::getName).sorted().collect(Collectors.toList())
    );

    // Check actual file contents
    for (Map.Entry<String, byte[]> entry : expectedFiles.entrySet()) {
      try (final FileInputStream in = new FileInputStream(new File(unzipDir, entry.getKey()))) {
        final byte[] bytes = ByteStreams.toByteArray(in);
        JupiterAssertions.assertArrayEquals(entry.getValue(), bytes);
      }
    }
  }

  private static class ZeroRemainingInputStream extends FilterInputStream
  {
    private final AtomicInteger pos = new AtomicInteger(0);

    protected ZeroRemainingInputStream(InputStream in)
    {
      super(in);
    }

    @Override
    public synchronized void reset() throws IOException
    {
      super.reset();
      pos.set(0);
    }

    @Override
    public int read(byte[] b) throws IOException
    {
      final int len = Math.min(b.length, GZ_BYTES.length - pos.get() % GZ_BYTES.length);
      pos.addAndGet(len);
      return read(b, 0, len);
    }

    @Override
    public int read() throws IOException
    {
      pos.incrementAndGet();
      return super.read();
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException
    {
      final int l = Math.min(len, GZ_BYTES.length - pos.get() % GZ_BYTES.length);
      pos.addAndGet(l);
      return super.read(b, off, l);
    }

    @Override
    public int available()
    {
      return 0;
    }
  }

  // Helper method for unit tests (for checking that we fixed https://snyk.io/research/zip-slip-vulnerability)
  public static void makeEvilZip(File outputFile) throws IOException
  {
    ZipOutputStream zipOutputStream = new ZipOutputStream(new FileOutputStream(outputFile));
    ZipEntry zipEntry = new ZipEntry("../../../../../../../../../../../../../../../tmp/evil.txt");
    zipOutputStream.putNextEntry(zipEntry);
    byte[] output = StringUtils.toUtf8("evil text");
    zipOutputStream.write(output);
    zipOutputStream.closeEntry();
    zipOutputStream.close();
  }
}
