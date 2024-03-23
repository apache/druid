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

package org.apache.druid.storage.azure;

import com.google.common.collect.ImmutableList;
import org.apache.druid.storage.azure.blob.CloudBlobHolder;
import org.easymock.EasyMock;
import org.easymock.EasyMockSupport;
import org.easymock.IExpectationSetters;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

public class AzureTestUtils extends EasyMockSupport
{
  public static File createZipTempFile(final String segmentFileName, final String content) throws IOException
  {
    final File zipFile = Files.createTempFile("index", ".zip").toFile();
    final byte[] value = content.getBytes(StandardCharsets.UTF_8);

    try (ZipOutputStream zipStream = new ZipOutputStream(new FileOutputStream(zipFile))) {
      zipStream.putNextEntry(new ZipEntry(segmentFileName));
      zipStream.write(value);
    }

    return zipFile;
  }

  public static AzureCloudBlobIterable expectListObjects(
      AzureCloudBlobIterableFactory azureCloudBlobIterableFactory,
      int maxListingLength,
      URI PREFIX_URI,
      List<CloudBlobHolder> objects,
      AzureStorage azureStorage
  )
  {
    AzureCloudBlobIterable azureCloudBlobIterable = EasyMock.createMock(AzureCloudBlobIterable.class);
    EasyMock.expect(azureCloudBlobIterable.iterator()).andReturn(objects.iterator());
    EasyMock.expect(azureCloudBlobIterableFactory.create(ImmutableList.of(PREFIX_URI), maxListingLength, azureStorage)).andReturn(azureCloudBlobIterable);
    return azureCloudBlobIterable;
  }

  public static void expectDeleteObjects(
      AzureStorage storage,
      List<CloudBlobHolder> deleteRequestsExpected,
      Map<CloudBlobHolder, Exception> deleteRequestToException,
      Integer maxTries
  )
  {
    Map<CloudBlobHolder, IExpectationSetters<CloudBlobHolder>> requestToResultExpectationSetter = new HashMap<>();

    for (Map.Entry<CloudBlobHolder, Exception> requestsAndErrors : deleteRequestToException.entrySet()) {
      CloudBlobHolder deleteObject = requestsAndErrors.getKey();
      Exception exception = requestsAndErrors.getValue();
      IExpectationSetters<CloudBlobHolder> resultExpectationSetter = requestToResultExpectationSetter.get(deleteObject);
      if (resultExpectationSetter == null) {
        storage.emptyCloudBlobDirectory(deleteObject.getContainerName(), deleteObject.getName(), maxTries);
        resultExpectationSetter = EasyMock.<CloudBlobHolder>expectLastCall().andThrow(exception);
        requestToResultExpectationSetter.put(deleteObject, resultExpectationSetter);
      } else {
        resultExpectationSetter.andThrow(exception);
      }
    }

    for (CloudBlobHolder deleteObject : deleteRequestsExpected) {
      IExpectationSetters<CloudBlobHolder> resultExpectationSetter = requestToResultExpectationSetter.get(deleteObject);
      if (resultExpectationSetter == null) {
        storage.emptyCloudBlobDirectory(deleteObject.getContainerName(), deleteObject.getName(), maxTries);
        resultExpectationSetter = EasyMock.expectLastCall();
        requestToResultExpectationSetter.put(deleteObject, resultExpectationSetter);
      }
      resultExpectationSetter.andReturn(null);
    }
  }

  public static CloudBlobHolder newCloudBlobHolder(
      String container,
      String prefix,
      long lastModifiedTimestamp)
  {
    CloudBlobHolder object = EasyMock.createMock(CloudBlobHolder.class);
    EasyMock.expect(object.getContainerName()).andReturn(container).anyTimes();
    EasyMock.expect(object.getName()).andReturn(prefix).anyTimes();
    EasyMock.expect(object.getLastModifed()).andReturn(new Date(lastModifiedTimestamp)).anyTimes();
    return object;
  }
}
