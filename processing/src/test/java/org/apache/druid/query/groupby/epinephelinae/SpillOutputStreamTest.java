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

package org.apache.druid.query.groupby.epinephelinae;

import org.apache.druid.query.groupby.GroupByStatsProvider;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;

public class SpillOutputStreamTest
{
  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void testSmallWriteStaysInMemory() throws IOException
  {
    try (SpillOutputStream out = makeStream(1024)) {
      out.write(new byte[]{1, 2, 3});
      Assert.assertTrue(out.isInMemory());
      Assert.assertArrayEquals(new byte[]{1, 2, 3}, out.toByteArray());
    }
  }

  @Test
  public void testExactlyAtThresholdStaysInMemory() throws IOException
  {
    try (SpillOutputStream out = makeStream(4)) {
      out.write(new byte[]{1, 2, 3, 4});
      Assert.assertTrue(out.isInMemory());
      Assert.assertArrayEquals(new byte[]{1, 2, 3, 4}, out.toByteArray());
    }
  }

  @Test
  public void testExceedingThresholdSwitchesToDisk() throws IOException
  {
    try (SpillOutputStream out = makeStream(4)) {
      out.write(new byte[]{1, 2, 3, 4, 5});
      Assert.assertFalse(out.isInMemory());
      Assert.assertTrue(out.getFile().exists());
      byte[] fileContent = Files.readAllBytes(out.getFile().toPath());
      Assert.assertArrayEquals(new byte[]{1, 2, 3, 4, 5}, fileContent);
    }
  }

  @Test
  public void testSwitchesToDiskOnSecondWrite() throws IOException
  {
    try (SpillOutputStream out = makeStream(4)) {
      out.write(new byte[]{1, 2});
      Assert.assertTrue(out.isInMemory());

      out.write(new byte[]{3, 4, 5});
      Assert.assertFalse(out.isInMemory());
      byte[] fileContent = Files.readAllBytes(out.getFile().toPath());
      Assert.assertArrayEquals(new byte[]{1, 2, 3, 4, 5}, fileContent);
    }
  }

  @Test
  public void testSingleByteWriteStaysInMemory() throws IOException
  {
    try (SpillOutputStream out = makeStream(1024)) {
      out.write(42);
      Assert.assertTrue(out.isInMemory());
      Assert.assertArrayEquals(new byte[]{42}, out.toByteArray());
    }
  }

  @Test
  public void testSingleByteWriteTriggersSwitch() throws IOException
  {
    try (SpillOutputStream out = makeStream(2)) {
      out.write(1);
      out.write(2);
      Assert.assertTrue(out.isInMemory());

      out.write(3);
      Assert.assertFalse(out.isInMemory());
      byte[] fileContent = Files.readAllBytes(out.getFile().toPath());
      Assert.assertArrayEquals(new byte[]{1, 2, 3}, fileContent);
    }
  }

  @Test
  public void testDataIntegrityAcrossSwitch() throws IOException
  {
    try (SpillOutputStream out = makeStream(10)) {
      byte[] beforeSwitch = new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
      byte[] afterSwitch = new byte[]{11, 12, 13, 14, 15};
      out.write(beforeSwitch);
      Assert.assertTrue(out.isInMemory());

      out.write(afterSwitch);
      Assert.assertFalse(out.isInMemory());
      out.flush();

      byte[] expected = new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15};
      byte[] fileContent = Files.readAllBytes(out.getFile().toPath());
      Assert.assertArrayEquals(expected, fileContent);
    }
  }

  @Test
  public void testWriteWithOffsetAndLength() throws IOException
  {
    try (SpillOutputStream out = makeStream(1024)) {
      byte[] data = new byte[]{0, 0, 1, 2, 3, 0, 0};
      out.write(data, 2, 3);
      Assert.assertTrue(out.isInMemory());
      Assert.assertArrayEquals(new byte[]{1, 2, 3}, out.toByteArray());
    }
  }

  @Test
  public void testWriteWithOffsetAndLengthTriggersDiskSwitch() throws IOException
  {
    try (SpillOutputStream out = makeStream(2)) {
      byte[] data = new byte[]{0, 1, 2, 3, 0};
      out.write(data, 1, 3);
      Assert.assertFalse(out.isInMemory());
      byte[] fileContent = Files.readAllBytes(out.getFile().toPath());
      Assert.assertArrayEquals(new byte[]{1, 2, 3}, fileContent);
    }
  }

  @Test
  public void testLargeWrite() throws IOException
  {
    try (SpillOutputStream out = makeStream(100)) {
      byte[] data = new byte[10_000];
      Arrays.fill(data, (byte) 0xAB);
      out.write(data);
      Assert.assertFalse(out.isInMemory());
      out.flush();
      byte[] fileContent = Files.readAllBytes(out.getFile().toPath());
      Assert.assertArrayEquals(data, fileContent);
    }
  }

  @Test
  public void testZeroThresholdAlwaysGoesToDisk() throws IOException
  {
    try (SpillOutputStream out = makeStream(0)) {
      out.write(new byte[]{1});
      Assert.assertFalse(out.isInMemory());
      byte[] fileContent = Files.readAllBytes(out.getFile().toPath());
      Assert.assertArrayEquals(new byte[]{1}, fileContent);
    }
  }

  @Test
  public void testEmptyStreamIsInMemory() throws IOException
  {
    try (SpillOutputStream out = makeStream(1024)) {
      Assert.assertTrue(out.isInMemory());
      Assert.assertArrayEquals(new byte[0], out.toByteArray());
    }
  }

  @Test
  public void testMultipleWritesAccumulateInMemory() throws IOException
  {
    try (SpillOutputStream out = makeStream(1024)) {
      out.write(new byte[]{1, 2});
      out.write(new byte[]{3, 4});
      out.write(5);
      Assert.assertTrue(out.isInMemory());
      Assert.assertArrayEquals(new byte[]{1, 2, 3, 4, 5}, out.toByteArray());
    }
  }

  @Test
  public void testMultipleWritesAfterDiskSwitch() throws IOException
  {
    try (SpillOutputStream out = makeStream(4)) {
      out.write(new byte[]{1, 2, 3, 4, 5});
      Assert.assertFalse(out.isInMemory());

      out.write(new byte[]{6, 7});
      out.write(8);
      out.flush();

      byte[] fileContent = Files.readAllBytes(out.getFile().toPath());
      Assert.assertArrayEquals(new byte[]{1, 2, 3, 4, 5, 6, 7, 8}, fileContent);
    }
  }

  @Test
  public void testDiskStorageBytesTracked() throws IOException
  {
    LimitedTemporaryStorage storage = makeStorage(1024 * 1024);

    try (SpillOutputStream out = new SpillOutputStream(storage, 4)) {
      out.write(new byte[]{1, 2, 3, 4, 5});
      Assert.assertFalse(out.isInMemory());
      out.flush();
      Assert.assertTrue(storage.currentSize() > 0);
    }
  }

  @Test(expected = NullPointerException.class)
  public void testToByteArrayThrowsAfterDiskSwitch() throws IOException
  {
    try (SpillOutputStream out = makeStream(4)) {
      out.write(new byte[]{1, 2, 3, 4, 5});
      Assert.assertFalse(out.isInMemory());
      out.toByteArray();
    }
  }

  @Test(expected = NullPointerException.class)
  public void testGetFileThrowsWhenInMemory() throws IOException
  {
    try (SpillOutputStream out = makeStream(1024)) {
      out.write(new byte[]{1, 2, 3});
      Assert.assertTrue(out.isInMemory());
      out.getFile();
    }
  }

  @Test(expected = TemporaryStorageFullException.class)
  public void testDiskStorageLimitEnforced() throws IOException
  {
    LimitedTemporaryStorage storage = makeStorage(10);

    try (SpillOutputStream out = new SpillOutputStream(storage, 4)) {
      byte[] data = new byte[100];
      Arrays.fill(data, (byte) 1);
      out.write(data);
    }
  }

  private SpillOutputStream makeStream(long threshold) throws IOException
  {
    return new SpillOutputStream(makeStorage(1024 * 1024), threshold);
  }

  private LimitedTemporaryStorage makeStorage(long maxBytes) throws IOException
  {
    return new LimitedTemporaryStorage(
        temporaryFolder.newFolder(),
        maxBytes,
        100,
        new GroupByStatsProvider.PerQueryStats()
    );
  }
}
