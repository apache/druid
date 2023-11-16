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

package org.apache.druid.storage.google;

import com.google.api.services.storage.model.StorageObject;
import org.apache.commons.io.IOUtils;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.StringUtils;
import org.easymock.EasyMock;
import org.easymock.EasyMockSupport;
import org.easymock.IExpectationSetters;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GoogleTestUtils extends EasyMockSupport
{
  private static final org.joda.time.DateTime NOW = DateTimes.nowUtc();
  private static final byte[] CONTENT =
      StringUtils.toUtf8(StringUtils.format("%d,hello,world", NOW.getMillis()));

  public static GoogleStorage.GoogleStorageObjectMetadata newStorageObject(
      String bucket,
      String key,
      long lastModifiedTimestamp
  )
  {
    GoogleStorage.GoogleStorageObjectMetadata object = new GoogleStorage.GoogleStorageObjectMetadata(bucket, key, (long) CONTENT.length,
                                                                                                     lastModifiedTimestamp);
    return object;
  }

  public static void expectListObjectsPageRequest(
      GoogleStorage storage,
      URI prefix,
      long maxListingLength,
      List<GoogleStorage.GoogleStorageObjectMetadata> objectMetadataList
  ) throws IOException
  {
    GoogleStorage.GoogleStorageObjectPage objectMetadataPage = new GoogleStorage.GoogleStorageObjectPage(objectMetadataList, null);
    String bucket = prefix.getAuthority();
    EasyMock.expect(storage.list(bucket, StringUtils.maybeRemoveLeadingSlash(prefix.getPath()), maxListingLength, null)).andReturn(objectMetadataPage).once();
//    return objectMetadataPage;
  }

  /*public static void expectListObjects(
      GoogleStorage.GoogleStorageObjectPage objectPage,
      List<GoogleStorage.GoogleStorageObjectMetadata> objects
  ) throws IOException
  {
//    EasyMock.expect(objectPage.get(StringUtils.maybeRemoveLeadingSlash(prefix.getPath()))).andReturn(blobPage);
//    EasyMock.expect(objectPage.setMaxResults(maxListingLength)).andReturn(blobPage);
//    EasyMock.expect(objectPage.setPageToken(EasyMock.anyString())).andReturn(blobPage).anyTimes();

    Objects resultObjects = new Objects();
    resultObjects.setItems(objects);

    EasyMock.expect(
        blobPage.execute()
    ).andReturn(resultObjects).once();
  }*/

  public static void expectDeleteObjects(
      GoogleStorage storage,
      List<GoogleStorage.GoogleStorageObjectMetadata> deleteObjectExpected,
      Map<GoogleStorage.GoogleStorageObjectMetadata, Exception> deleteObjectToException
  ) throws IOException
  {
    Map<GoogleStorage.GoogleStorageObjectMetadata, IExpectationSetters<GoogleStorage.GoogleStorageObjectMetadata>> requestToResultExpectationSetter = new HashMap<>();
    for (Map.Entry<GoogleStorage.GoogleStorageObjectMetadata, Exception> deleteObjectAndException : deleteObjectToException.entrySet()) {
      GoogleStorage.GoogleStorageObjectMetadata deleteObject = deleteObjectAndException.getKey();
      Exception exception = deleteObjectAndException.getValue();
      IExpectationSetters<GoogleStorage.GoogleStorageObjectMetadata> resultExpectationSetter = requestToResultExpectationSetter.get(deleteObject);
      if (resultExpectationSetter == null) {
        storage.delete(deleteObject.getBucket(), deleteObject.getName());
        resultExpectationSetter = EasyMock.<GoogleStorage.GoogleStorageObjectMetadata>expectLastCall().andThrow(exception);
        requestToResultExpectationSetter.put(deleteObject, resultExpectationSetter);
      } else {
        resultExpectationSetter.andThrow(exception);
      }
    }

    for (GoogleStorage.GoogleStorageObjectMetadata deleteObject : deleteObjectExpected) {
      IExpectationSetters<GoogleStorage.GoogleStorageObjectMetadata> resultExpectationSetter = requestToResultExpectationSetter.get(deleteObject);
      if (resultExpectationSetter == null) {
        storage.delete(deleteObject.getBucket(), deleteObject.getName());
        resultExpectationSetter = EasyMock.expectLastCall();
        requestToResultExpectationSetter.put(deleteObject, resultExpectationSetter);
      }
      resultExpectationSetter.andVoid();
    }
  }

  public static String readAsString(InputStream is) throws IOException
  {
    final StringWriter writer = new StringWriter();
    IOUtils.copy(is, writer, "UTF-8");
    return writer.toString();
  }
}
