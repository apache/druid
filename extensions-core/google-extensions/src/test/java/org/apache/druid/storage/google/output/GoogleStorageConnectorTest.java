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

package org.apache.druid.storage.google.output;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.commons.io.IOUtils;
import org.apache.druid.java.util.common.HumanReadableBytes;
import org.apache.druid.storage.google.GoogleInputDataConfig;
import org.apache.druid.storage.google.GoogleStorage;
import org.apache.druid.storage.google.GoogleStorageObjectMetadata;
import org.apache.druid.storage.google.GoogleStorageObjectPage;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class GoogleStorageConnectorTest
{
  private static final String BUCKET = "BUCKET";
  private static final String PREFIX = "PREFIX";
  private static final String TEST_FILE = "TEST_FILE";
  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private static final int MAX_LISTING_LEN = 10;

  private static final HumanReadableBytes CHUNK_SIZE = new HumanReadableBytes("4MiB");

  GoogleStorageConnector googleStorageConnector;
  private final GoogleStorage googleStorage = EasyMock.createMock(GoogleStorage.class);

  @Before
  public void setUp() throws IOException
  {
    GoogleOutputConfig config = new GoogleOutputConfig(BUCKET, PREFIX, temporaryFolder.newFolder(), CHUNK_SIZE, null);
    GoogleInputDataConfig inputDataConfig = new GoogleInputDataConfig();
    inputDataConfig.setMaxListingLength(MAX_LISTING_LEN);
    googleStorageConnector = new GoogleStorageConnector(config, googleStorage, inputDataConfig);
  }

  @Test
  public void testPathExistsSuccess()
  {
    final Capture<String> bucket = Capture.newInstance();
    final Capture<String> path = Capture.newInstance();
    EasyMock.expect(googleStorage.exists(EasyMock.capture(bucket), EasyMock.capture(path))).andReturn(true);
    EasyMock.replay(googleStorage);
    Assert.assertTrue(googleStorageConnector.pathExists(TEST_FILE));
    Assert.assertEquals(BUCKET, bucket.getValue());
    Assert.assertEquals(PREFIX + "/" + TEST_FILE, path.getValue());
    EasyMock.verify(googleStorage);
  }

  @Test
  public void testPathExistsFailure()
  {
    final Capture<String> bucket = Capture.newInstance();
    final Capture<String> path = Capture.newInstance();
    EasyMock.expect(googleStorage.exists(EasyMock.capture(bucket), EasyMock.capture(path))).andReturn(false);
    EasyMock.replay(googleStorage);
    Assert.assertFalse(googleStorageConnector.pathExists(TEST_FILE));
    Assert.assertEquals(BUCKET, bucket.getValue());
    Assert.assertEquals(PREFIX + "/" + TEST_FILE, path.getValue());
    EasyMock.verify(googleStorage);
  }

  @Test
  public void testDeleteFile() throws IOException
  {
    Capture<String> bucketCapture = EasyMock.newCapture();
    Capture<String> pathCapture = EasyMock.newCapture();
    googleStorage.delete(
        EasyMock.capture(bucketCapture),
        EasyMock.capture(pathCapture)
    );

    EasyMock.replay(googleStorage);
    googleStorageConnector.deleteFile(TEST_FILE);
    Assert.assertEquals(BUCKET, bucketCapture.getValue());
    Assert.assertEquals(PREFIX + "/" + TEST_FILE, pathCapture.getValue());
  }

  @Test
  public void testDeleteFiles() throws IOException
  {
    Capture<String> containerCapture = EasyMock.newCapture();
    Capture<Iterable<String>> pathsCapture = EasyMock.newCapture();
    googleStorage.batchDelete(EasyMock.capture(containerCapture), EasyMock.capture(pathsCapture));
    EasyMock.replay(googleStorage);
    googleStorageConnector.deleteFiles(ImmutableList.of(TEST_FILE + "_1.part", TEST_FILE + "_2.json"));
    Assert.assertEquals(BUCKET, containerCapture.getValue());
    Assert.assertEquals(
        ImmutableList.of(
            PREFIX + "/" + TEST_FILE + "_1.part",
            PREFIX + "/" + TEST_FILE + "_2.json"
        ),
        Lists.newArrayList(pathsCapture.getValue())
    );
    EasyMock.reset(googleStorage);
  }

  @Test
  public void testListDir() throws IOException
  {
    GoogleStorageObjectMetadata objectMetadata1 = new GoogleStorageObjectMetadata(
        BUCKET,
        PREFIX + "/x/y" + TEST_FILE,
        (long) 3,
        null
    );
    GoogleStorageObjectMetadata objectMetadata2 = new GoogleStorageObjectMetadata(
        BUCKET,
        PREFIX + "/p/q/r/" + TEST_FILE,
        (long) 4,
        null
    );
    Capture<Long> maxListingCapture = EasyMock.newCapture();
    Capture<String> pageTokenCapture = EasyMock.newCapture();
    EasyMock.expect(googleStorage.list(
                EasyMock.anyString(),
                EasyMock.anyString(),
                EasyMock.capture(maxListingCapture),
                EasyMock.capture(pageTokenCapture)
            ))
            .andReturn(new GoogleStorageObjectPage(ImmutableList.of(objectMetadata1, objectMetadata2), null));
    EasyMock.replay(googleStorage);
    List<String> ret = Lists.newArrayList(googleStorageConnector.listDir(""));
    Assert.assertEquals(ImmutableList.of("x/y" + TEST_FILE, "p/q/r/" + TEST_FILE), ret);
    Assert.assertEquals(MAX_LISTING_LEN, maxListingCapture.getValue().intValue());
    Assert.assertEquals(null, pageTokenCapture.getValue());

  }

  @Test
  public void testRead() throws IOException
  {
    String data = "test";
    EasyMock.expect(googleStorage.size(EasyMock.anyString(), EasyMock.anyString()))
            .andReturn(4L);
    EasyMock.expect(
        googleStorage.getInputStream(
            EasyMock.anyString(),
            EasyMock.anyString(),
            EasyMock.anyLong(),
            EasyMock.anyLong()
        )
    ).andReturn(IOUtils.toInputStream(data, StandardCharsets.UTF_8));

    EasyMock.replay(googleStorage);
    InputStream is = googleStorageConnector.read(TEST_FILE);
    byte[] dataBytes = new byte[data.length()];
    Assert.assertEquals(data.length(), is.read(dataBytes));
    Assert.assertEquals(-1, is.read());
    Assert.assertEquals(data, new String(dataBytes, StandardCharsets.UTF_8));
  }

  @Test
  public void testReadRange() throws IOException
  {
    String data = "test";

    for (int start = 0; start < data.length(); ++start) {
      for (long length = 1; length <= data.length() - start; ++length) {
        String dataQueried = data.substring(start, start + ((Long) length).intValue());
        EasyMock.expect(googleStorage.getInputStream(
                    EasyMock.anyString(),
                    EasyMock.anyString(),
                    EasyMock.anyLong(),
                    EasyMock.anyLong()
                ))
                .andReturn(IOUtils.toInputStream(dataQueried, StandardCharsets.UTF_8));
        EasyMock.replay(googleStorage);

        InputStream is = googleStorageConnector.readRange(TEST_FILE, start, length);
        byte[] dataBytes = new byte[((Long) length).intValue()];
        Assert.assertEquals(length, is.read(dataBytes));
        Assert.assertEquals(-1, is.read());
        Assert.assertEquals(dataQueried, new String(dataBytes, StandardCharsets.UTF_8));
        EasyMock.reset(googleStorage);
      }
    }

  }
}
