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

import org.apache.druid.java.util.common.IAE;
import org.apache.druid.storage.StorageConnector;
import org.apache.druid.storage.StorageConnectorProvider;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.UUID;

public class LocalFileStorageConnectorTest
{
  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public ExpectedException expectedException = ExpectedException.none();
  private File tempDir;
  private StorageConnector storageConnector;

  @Before
  public void init() throws IOException
  {
    tempDir = temporaryFolder.newFolder();
    storageConnector = new LocalFileStorageConnectorProvider(tempDir).get();
  }

  @Test
  public void sanityCheck() throws IOException
  {
    String uuid = UUID.randomUUID().toString();

    //create file
    createAndPopulateFile(uuid);

    // check if file is created
    Assert.assertTrue(storageConnector.pathExists(uuid));
    Assert.assertTrue(new File(tempDir.getAbsolutePath(), uuid).exists());

    // check contents
    checkContents(uuid);

    // delete file
    storageConnector.deleteFile(uuid);
    Assert.assertFalse(new File(tempDir.getAbsolutePath(), uuid).exists());
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

    Assert.assertTrue(storageConnector.pathExists(uuid1));
    Assert.assertTrue(storageConnector.pathExists(uuid2));

    checkContents(uuid1);
    checkContents(uuid2);

    File baseFile = new File(tempDir.getAbsolutePath(), uuid_base);
    Assert.assertTrue(baseFile.exists());
    Assert.assertTrue(baseFile.isDirectory());
    Assert.assertEquals(2, baseFile.listFiles().length);

    storageConnector.deleteRecursively(uuid_base);
    Assert.assertFalse(baseFile.exists());
    Assert.assertTrue(new File(tempDir.getAbsolutePath(), topLevelDir).exists());

  }

  @Test
  public void incorrectBasePath() throws IOException
  {
    File file = temporaryFolder.newFile();
    expectedException.expect(IAE.class);
    StorageConnectorProvider storageConnectorProvider = new LocalFileStorageConnectorProvider(file);
    storageConnectorProvider.get();
  }

  private void checkContents(String uuid) throws IOException
  {
    try (InputStream inputStream = storageConnector.read(uuid)) {
      Assert.assertEquals(1, inputStream.read());
      Assert.assertEquals(0, inputStream.available());
    }
  }

  private void createAndPopulateFile(String uuid) throws IOException
  {
    try (OutputStream os = storageConnector.write(uuid)) {
      os.write(1);
    }
  }
}
