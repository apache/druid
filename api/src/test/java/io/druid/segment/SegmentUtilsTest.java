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

package io.druid.segment;

import com.google.common.primitives.Ints;
import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;

/**
 */
public class SegmentUtilsTest
{
  @Rule
  public final TemporaryFolder tempFolder = new TemporaryFolder();

  @Test
  public void testVersionBin() throws Exception
  {
    File dir = tempFolder.newFolder();
    byte[] bytes = Ints.toByteArray(9);
    FileUtils.writeByteArrayToFile(new File(dir, "version.bin"), Ints.toByteArray(9));
    Assert.assertEquals(9, SegmentUtils.getVersionFromDir(dir));
  }

  @Test
  public void testIndexDrd() throws Exception
  {
    File dir = tempFolder.newFolder();
    FileUtils.writeByteArrayToFile(new File(dir, "index.drd"), new byte[]{(byte) 0x8});
    Assert.assertEquals(8, SegmentUtils.getVersionFromDir(dir));
  }

  @Test(expected = IOException.class)
  public void testException() throws Exception
  {
    SegmentUtils.getVersionFromDir(tempFolder.newFolder());
  }
}
