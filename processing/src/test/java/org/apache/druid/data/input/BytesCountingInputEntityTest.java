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

package org.apache.druid.data.input;

import org.apache.druid.data.input.impl.ByteEntity;
import org.apache.druid.data.input.impl.FileEntity;
import org.apache.druid.data.input.impl.InputStatsImpl;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class BytesCountingInputEntityTest
{
  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  private InputStats inputStats;

  @Before
  public void setup()
  {
    inputStats = new InputStatsImpl();
  }

  @Test
  public void testFetch() throws IOException
  {
    final int fileSize = 200;
    final File sourceFile = folder.newFile("testWithFileEntity");
    writeBytesToFile(sourceFile, fileSize);

    final BytesCountingInputEntity inputEntity = new BytesCountingInputEntity(new FileEntity(sourceFile), inputStats);
    inputEntity.fetch(folder.newFolder(), new byte[50]);
    Assert.assertEquals(fileSize, inputStats.getProcessedBytes());
  }

  @Test
  public void testFetchFromPartiallyReadFile() throws IOException
  {
    final int fileSize = 200;
    final File sourceFile = folder.newFile("testWithFileEntity");
    writeBytesToFile(sourceFile, fileSize);

    final int bufferSize = 50;
    final byte[] intermediateBuffer = new byte[bufferSize];

    // Read the file partially
    final BytesCountingInputEntity inputEntity = new BytesCountingInputEntity(new FileEntity(sourceFile), inputStats);
    inputEntity.open().read(intermediateBuffer);
    Assert.assertEquals(bufferSize, inputStats.getProcessedBytes());

    // Read the whole file again
    inputEntity.fetch(folder.newFolder(), intermediateBuffer);
    Assert.assertEquals(fileSize + bufferSize, inputStats.getProcessedBytes());
  }

  @Test
  public void testFetchFromDirectory() throws IOException
  {
    final File sourceDir = folder.newFolder("testWithDirectory");

    final int fileSize1 = 100;
    final File sourceFile1 = new File(sourceDir, "file1");
    writeBytesToFile(sourceFile1, fileSize1);

    final int fileSize2 = 200;
    final File sourceFile2 = new File(sourceDir, "file2");
    writeBytesToFile(sourceFile2, fileSize2);

    final BytesCountingInputEntity inputEntity = new BytesCountingInputEntity(new FileEntity(sourceDir), inputStats);
    inputEntity.fetch(folder.newFolder(), new byte[1000]);
    Assert.assertEquals(fileSize1 + fileSize2, inputStats.getProcessedBytes());
  }

  @Test
  public void testOpen() throws IOException
  {
    final int entitySize = 100;

    final BytesCountingInputEntity inputEntity = new BytesCountingInputEntity(
        new ByteEntity(new byte[entitySize]),
        inputStats
    );
    inputEntity.open().read(new byte[200]);
    Assert.assertEquals(entitySize, inputStats.getProcessedBytes());
  }

  @Test
  public void testOpenWithSmallBuffer() throws IOException
  {
    final int entitySize = 100;
    final int bufferSize = 50;

    final BytesCountingInputEntity inputEntity = new BytesCountingInputEntity(
        new ByteEntity(new byte[entitySize]),
        inputStats
    );
    inputEntity.open().read(new byte[bufferSize]);
    Assert.assertEquals(bufferSize, inputStats.getProcessedBytes());
  }

  private void writeBytesToFile(File sourceFile, int numBytes) throws IOException
  {
    if (!sourceFile.exists()) {
      sourceFile.createNewFile();
    }

    final OutputStreamWriter outputStreamWriter = new OutputStreamWriter(
        new FileOutputStream(sourceFile),
        StandardCharsets.UTF_8
    );
    char[] chars = new char[numBytes];
    Arrays.fill(chars, ' ');
    outputStreamWriter.write(chars);
    outputStreamWriter.flush();
    outputStreamWriter.close();
  }

}
