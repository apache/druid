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

package io.druid.java.util.common.io;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;

public class NativeIOTest
{
  @Rule
  public final TemporaryFolder tempFolder = new TemporaryFolder();

  @Test
  public void testChunkedCopy() throws Exception
  {
    File f = tempFolder.newFile();
    byte[] bytes = new byte[]{(byte) 0x8, (byte) 0x9};

    ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
    NativeIO.chunkedCopy(bis, f);

    byte[] data = Files.readAllBytes(f.toPath());
    Assert.assertTrue(Arrays.equals(bytes, data));
  }

  @Test(expected = IOException.class)
  public void testException() throws Exception
  {
    File dir = tempFolder.newFolder();
    NativeIO.chunkedCopy(null, dir);
  }

  @Test
  public void testDisabledFadviseChunkedCopy() throws Exception
  {
    boolean possible = NativeIO.isFadvisePossible();

    NativeIO.setFadvisePossible(false);
    File f = tempFolder.newFile();
    byte[] bytes = new byte[]{(byte) 0x8, (byte) 0x9};

    ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
    NativeIO.chunkedCopy(bis, f);

    byte[] data = Files.readAllBytes(f.toPath());

    NativeIO.setFadvisePossible(possible);
    Assert.assertTrue(Arrays.equals(bytes, data));
  }

  @Test
  public void testDisabledSyncFileRangePossible() throws Exception
  {
    boolean possible = NativeIO.isSyncFileRangePossible();

    NativeIO.setSyncFileRangePossible(false);
    File f = tempFolder.newFile();
    byte[] bytes = new byte[]{(byte) 0x8, (byte) 0x9};

    ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
    NativeIO.chunkedCopy(bis, f);

    byte[] data = Files.readAllBytes(f.toPath());

    NativeIO.setSyncFileRangePossible(possible);
    Assert.assertTrue(Arrays.equals(bytes, data));
  }

}
