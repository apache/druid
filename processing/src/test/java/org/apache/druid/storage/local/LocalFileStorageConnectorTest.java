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

package org.apache.druid.storage.local;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.storage.StorageConnector;
import org.apache.druid.storage.StorageConnectorProvider;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Set;
import java.util.UUID;

public class LocalFileStorageConnectorTest
{
  @TempDir
  public File temporaryFolder;

  private File storageDir;
  private StorageConnector storageConnector;

  @BeforeEach
  public void init() throws IOException
  {
    storageDir = temporaryFolder;
    storageConnector = new LocalFileStorageConnectorProvider(storageDir).createStorageConnector(null);
  }

  @Test
  public void sanityCheck() throws IOException
  {
    String uuid = UUID.randomUUID().toString();

    //create file
    createAndPopulateFile(uuid);

    // check if file is created
    Assertions.assertTrue(storageConnector.pathExists(uuid));
    Assertions.assertTrue(new File(storageDir.getAbsolutePath(), uuid).exists());

    // check contents
    checkContents(uuid);

    // delete file
    storageConnector.deleteFile(uuid);
    Assertions.assertFalse(new File(storageDir.getAbsolutePath(), uuid).exists());
  }

  @Test
  public void deleteRecursivelyTest() throws IOException
  {
    String topLevelDir = "top" + UUID.randomUUID();
    String uuid_base = topLevelDir + "/" + UUID.randomUUID();
    String uuid1 = uuid_base + "/" + UUID.randomUUID();
    String uuid2 = uuid_base + "/" + UUID.randomUUID();

    createAndPopulateFile(uuid1);
    createAndPopulateFile(uuid2);

    Assertions.assertTrue(storageConnector.pathExists(uuid1));
    Assertions.assertTrue(storageConnector.pathExists(uuid2));

    checkContents(uuid1);
    checkContents(uuid2);

    File baseFile = new File(storageDir.getAbsolutePath(), uuid_base);
    Assertions.assertTrue(baseFile.exists());
    Assertions.assertTrue(baseFile.isDirectory());
    Assertions.assertEquals(2, baseFile.listFiles().length);

    storageConnector.deleteRecursively(uuid_base);
    Assertions.assertFalse(baseFile.exists());
    Assertions.assertTrue(new File(storageDir.getAbsolutePath(), topLevelDir).exists());
  }

  @Test
  public void batchDelete() throws IOException
  {
    String uuid1 = UUID.randomUUID().toString();
    String uuid2 = UUID.randomUUID().toString();

    //create file
    createAndPopulateFile(uuid1);
    createAndPopulateFile(uuid2);

    // delete file
    storageConnector.deleteFiles(ImmutableList.of(uuid1, uuid2));
    Assertions.assertFalse(new File(storageDir.getAbsolutePath(), uuid1).exists());
    Assertions.assertFalse(new File(storageDir.getAbsolutePath(), uuid2).exists());
  }

  @Test
  public void incorrectBasePath() throws IOException
  {
    File file = File.createTempFile("test", ".tmp", temporaryFolder);
    StorageConnectorProvider storageConnectorProvider = new LocalFileStorageConnectorProvider(file);
    Assertions.assertThrows(IAE.class, () -> storageConnectorProvider.createStorageConnector(null));
  }

  @Test
  public void listFilesTest() throws Exception
  {
    String topLevelDir = "top" + UUID.randomUUID();
    String uuid_base = topLevelDir + "/" + UUID.randomUUID();
    String uuid1 = uuid_base + "/" + UUID.randomUUID();
    String uuid2 = uuid_base + "/" + UUID.randomUUID();

    createAndPopulateFile(uuid1);
    createAndPopulateFile(uuid2);

    List<String> topLevelDirContents = Lists.newArrayList(storageConnector.listDir(topLevelDir));
    List<String> expectedTopLevelDirContents = ImmutableList.of(new File(uuid_base).getName());
    Assertions.assertEquals(expectedTopLevelDirContents, topLevelDirContents);

    // Converted to a set since the output of the listDir can be shuffled
    Set<String> nextLevelDirContents = Sets.newHashSet(storageConnector.listDir(uuid_base));
    Set<String> expectedNextLevelDirContents = ImmutableSet.of(new File(uuid1).getName(), new File(uuid2).getName());
    Assertions.assertEquals(expectedNextLevelDirContents, nextLevelDirContents);

    // Check if listDir throws if an unknown path is passed as an argument
    Assertions.assertThrows(
        IAE.class,
        () -> storageConnector.listDir("unknown_top_path")
    );

    // Check if listDir throws if a file path is passed as an argument
    Assertions.assertThrows(
        IAE.class,
        () -> storageConnector.listDir(uuid1)
    );
  }

  @Test
  public void testReadRange() throws Exception
  {
    String uuid = UUID.randomUUID().toString();
    String data = "Hello";
    try (OutputStream outputStream = storageConnector.write(uuid)) {
      outputStream.write(data.getBytes(StandardCharsets.UTF_8));
    }

    // non empty reads
    for (int start = 0; start < data.length(); start++) {
      for (int length = 1; length <= data.length() - start; length++) {
        InputStream is = storageConnector.readRange(uuid, start, length);
        byte[] dataBytes = new byte[length];
        Assertions.assertEquals(is.read(dataBytes), length);
        Assertions.assertEquals(is.read(), -1); // reading further produces no data
        Assertions.assertEquals(data.substring(start, start + length), new String(dataBytes, StandardCharsets.UTF_8));
      }
    }

    // empty read
    InputStream is = storageConnector.readRange(uuid, 0, 0);
    byte[] dataBytes = new byte[0];
    Assertions.assertEquals(is.read(dataBytes), -1);
    Assertions.assertEquals(data.substring(0, 0), new String(dataBytes, StandardCharsets.UTF_8));
  }

  private void checkContents(String uuid) throws IOException
  {
    try (InputStream inputStream = storageConnector.read(uuid)) {
      Assertions.assertEquals(1, inputStream.read());
      Assertions.assertEquals(0, inputStream.available());
    }
  }

  private void createAndPopulateFile(String uuid) throws IOException
  {
    try (OutputStream os = storageConnector.write(uuid)) {
      os.write(1);
    }
  }
}
