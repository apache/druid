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

package org.apache.druid.storage.cloudfiles;

import org.easymock.EasyMock;
import org.easymock.EasyMockSupport;
import org.jclouds.io.Payload;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;

public class CloudFilesByteSourceTest extends EasyMockSupport
{
  @Test
  public void openStreamTest() throws IOException
  {
    final String path = "path";

    CloudFilesObjectApiProxy objectApi = createMock(CloudFilesObjectApiProxy.class);
    CloudFilesObject cloudFilesObject = createMock(CloudFilesObject.class);
    Payload payload = createMock(Payload.class);
    InputStream stream = createMock(InputStream.class);

    EasyMock.expect(objectApi.get(path, 0)).andReturn(cloudFilesObject);
    EasyMock.expect(cloudFilesObject.getPayload()).andReturn(payload);
    EasyMock.expect(payload.openStream()).andReturn(stream);
    payload.close();

    replayAll();

    CloudFilesByteSource byteSource = new CloudFilesByteSource(objectApi, path);
    Assert.assertEquals(stream, byteSource.openStream());
    byteSource.closeStream();

    verifyAll();
  }

  @Test()
  public void openStreamWithRecoverableErrorTest() throws IOException
  {
    final String path = "path";

    CloudFilesObjectApiProxy objectApi = createMock(CloudFilesObjectApiProxy.class);
    CloudFilesObject cloudFilesObject = createMock(CloudFilesObject.class);
    Payload payload = createMock(Payload.class);
    InputStream stream = createMock(InputStream.class);

    EasyMock.expect(objectApi.get(path, 0)).andReturn(cloudFilesObject);
    EasyMock.expect(cloudFilesObject.getPayload()).andReturn(payload);
    EasyMock.expect(payload.openStream()).andThrow(new IOException()).andReturn(stream);
    payload.close();

    replayAll();

    CloudFilesByteSource byteSource = new CloudFilesByteSource(objectApi, path);
    try {
      byteSource.openStream();
    }
    catch (Exception e) {
      Assert.assertEquals("Recoverable exception", e.getMessage());
    }

    Assert.assertEquals(stream, byteSource.openStream());
    byteSource.closeStream();

    verifyAll();
  }
}
