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

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

public class FileUtilsTest
{
  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  @Test
  public void testMap() throws IOException
  {
    File dataFile = folder.newFile("data");
    long buffersMemoryBefore = BufferUtils.totalMemoryUsedByDirectAndMappedBuffers();
    try (RandomAccessFile raf = new RandomAccessFile(dataFile, "rw")) {
      raf.write(42);
      raf.setLength(1 << 20); // 1 MB
    }
    try (MappedByteBufferHandler mappedByteBufferHandler = FileUtils.map(dataFile)) {
      Assert.assertEquals(42, mappedByteBufferHandler.get().get(0));
    }
    long buffersMemoryAfter = BufferUtils.totalMemoryUsedByDirectAndMappedBuffers();
    Assert.assertEquals(buffersMemoryBefore, buffersMemoryAfter);
  }
}
