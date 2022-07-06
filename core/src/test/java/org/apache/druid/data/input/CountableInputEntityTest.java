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

public class CountableInputEntityTest
{
  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  private CountableInputEntity countableInputEntity;
  private InputStats inputStats;
  private byte[] bytes;
  private final int numBytes = 100;

  @Before
  public void setUp()
  {
    inputStats = new InputStats();
    bytes = new byte[numBytes];
  }

  @Test
  public void testWithFileEntity() throws IOException
  {
    final File sourceFile = folder.newFile("testWithFileEntity");
    final OutputStreamWriter outputStreamWriter = new OutputStreamWriter(
        new FileOutputStream(sourceFile),
        StandardCharsets.UTF_8
    );
    char[] chars = new char[numBytes];
    Arrays.fill(chars, ' ');
    outputStreamWriter.write(chars);
    outputStreamWriter.flush();
    outputStreamWriter.close();
    final FileEntity fileEntity = new FileEntity(sourceFile);
    countableInputEntity = new CountableInputEntity(fileEntity, inputStats);

    final byte[] intermediateBuffer = new byte[numBytes / 2];
    countableInputEntity.open().read(intermediateBuffer);
    Assert.assertEquals(numBytes / 2, inputStats.getProcessedBytes().intValue());

    countableInputEntity.fetch(folder.newFolder(), intermediateBuffer);
    Assert.assertEquals((numBytes / 2) + numBytes, inputStats.getProcessedBytes().intValue());
  }

  @Test
  public void testWithByteEntity() throws IOException
  {
    final byte[] intermediateBuffer = new byte[numBytes];
    final ByteEntity byteEntity = new ByteEntity(bytes);
    countableInputEntity = new CountableInputEntity(byteEntity, inputStats);
    countableInputEntity.open().read(intermediateBuffer);
    Assert.assertEquals(numBytes, inputStats.getProcessedBytes().intValue());

    final byte[] smallIntermediateBuffer = new byte[25];
    final ByteEntity byteEntity1 = new ByteEntity(bytes);
    countableInputEntity = new CountableInputEntity(byteEntity1, inputStats);
    countableInputEntity.fetch(folder.newFolder(), smallIntermediateBuffer);
    Assert.assertEquals(numBytes + numBytes, inputStats.getProcessedBytes().intValue());
  }
}
