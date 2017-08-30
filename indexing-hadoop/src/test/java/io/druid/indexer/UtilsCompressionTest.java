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

package io.druid.indexer;

import com.google.common.io.ByteStreams;
import io.druid.java.util.common.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class UtilsCompressionTest
{

  private static final String DUMMY_STRING = "Very important string";
  private static final String TMP_FILE_NAME = "test_file";
  private static final Class<? extends CompressionCodec> DEFAULT_COMPRESSION_CODEC = GzipCodec.class;
  private static final String CODEC_CLASS = "org.apache.hadoop.io.compress.GzipCodec";
  private Configuration jobConfig;
  private JobContext mockJobContext;
  private FileSystem defaultFileSystem;
  private CompressionCodec codec;
  private File tmpFile;
  private Path tmpPathWithoutExtension;
  private Path tmpPathWithExtension;

  @Rule
  public TemporaryFolder tmpFolder = new TemporaryFolder();

  @Before
  public void setUp() throws IOException
  {
    jobConfig = new Configuration();
    mockJobContext = EasyMock.createMock(JobContext.class);
    EasyMock.expect(mockJobContext.getConfiguration()).andReturn(jobConfig).anyTimes();
    EasyMock.replay(mockJobContext);

    jobConfig.setBoolean(FileOutputFormat.COMPRESS, true);
    jobConfig.set(FileOutputFormat.COMPRESS_CODEC, CODEC_CLASS);
    Class<? extends CompressionCodec> codecClass = FileOutputFormat
        .getOutputCompressorClass(mockJobContext, DEFAULT_COMPRESSION_CODEC);
    codec = ReflectionUtils.newInstance(codecClass, jobConfig);

    tmpFile = tmpFolder.newFile(TMP_FILE_NAME + codec.getDefaultExtension());
    tmpPathWithExtension = new Path(tmpFile.getAbsolutePath());
    tmpPathWithoutExtension = new Path(tmpFile.getParent(), TMP_FILE_NAME);
    defaultFileSystem = tmpPathWithoutExtension.getFileSystem(jobConfig);
  }

  @After
  public void tearDown()
  {
    tmpFolder.delete();
  }

  @Test
  public void testExistsCompressedFile() throws IOException
  {
    boolean expected = Utils.exists(mockJobContext, defaultFileSystem, tmpPathWithoutExtension);
    Assert.assertTrue("Should be true since file is created", expected);
    tmpFolder.delete();
    expected = Utils.exists(mockJobContext, defaultFileSystem, tmpPathWithoutExtension);
    Assert.assertFalse("Should be false since file is deleted", expected);
  }

  @Test
  public void testCompressedOpenInputStream() throws IOException
  {
    boolean overwrite = true;
    OutputStream outStream = codec.createOutputStream(defaultFileSystem.create(tmpPathWithExtension, overwrite));
    writeStingToOutputStream(DUMMY_STRING, outStream);
    InputStream inStream = Utils.openInputStream(mockJobContext, tmpPathWithoutExtension);
    Assert.assertNotNull("Input stream should not be Null", inStream);
    String actual = StringUtils.fromUtf8(ByteStreams.toByteArray(inStream));
    Assert.assertEquals("Strings not matching", DUMMY_STRING, actual);
    inStream.close();
  }

  @Test
  public void testCompressedMakePathAndOutputStream() throws IOException
  {
    boolean overwrite = true;
    OutputStream outStream = Utils.makePathAndOutputStream(mockJobContext, tmpPathWithoutExtension, overwrite);
    Assert.assertNotNull("Output stream should not be null", outStream);
    writeStingToOutputStream(DUMMY_STRING, outStream);
    InputStream inStream = codec.createInputStream(defaultFileSystem.open(tmpPathWithExtension));
    String actual = StringUtils.fromUtf8(ByteStreams.toByteArray(inStream));
    Assert.assertEquals("Strings not matching", DUMMY_STRING, actual);
    inStream.close();
  }

  private void writeStingToOutputStream(String string, OutputStream outStream) throws IOException
  {
    outStream.write(StringUtils.toUtf8(string));
    outStream.flush();
    outStream.close();
  }
}
