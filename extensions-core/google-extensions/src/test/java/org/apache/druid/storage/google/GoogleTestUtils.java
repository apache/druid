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

import com.google.api.client.util.DateTime;
import com.google.api.services.storage.Storage;
import com.google.api.services.storage.model.Objects;
import com.google.api.services.storage.model.StorageObject;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.StringUtils;
import org.easymock.EasyMock;
import org.easymock.EasyMockSupport;
import org.easymock.IExpectationSetters;

import java.io.IOException;
import java.math.BigInteger;
import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GoogleTestUtils extends EasyMockSupport
{
  private static final org.joda.time.DateTime NOW = DateTimes.nowUtc();
  private static final byte[] CONTENT =
      StringUtils.toUtf8(StringUtils.format("%d,hello,world", NOW.getMillis()));

  public static StorageObject newStorageObject(
      String bucket,
      String key,
      long lastModifiedTimestamp
  )
  {
    StorageObject object = new StorageObject();
    object.setBucket(bucket);
    object.setName(key);
    object.setUpdated(new DateTime(lastModifiedTimestamp));
    object.setEtag("etag");
    object.setSize(BigInteger.valueOf(CONTENT.length));
    return object;
  }

  public static Storage.Objects.List expectListRequest(
      GoogleStorage storage,
      URI prefix
  ) throws IOException
  {
    Storage.Objects.List listRequest = EasyMock.createMock(Storage.Objects.List.class);
    String bucket = prefix.getAuthority();
    EasyMock.expect(
        storage.list(bucket)
    ).andReturn(listRequest).once();
    return listRequest;
  }

  public static void expectListObjects(
      Storage.Objects.List listRequest,
      URI prefix,
      long maxListingLength,
      List<StorageObject> objects
  ) throws IOException
  {
    EasyMock.expect(listRequest.setPrefix(StringUtils.maybeRemoveLeadingSlash(prefix.getPath()))).andReturn(listRequest);
    EasyMock.expect(listRequest.setMaxResults(maxListingLength)).andReturn(listRequest);
    EasyMock.expect(listRequest.setPageToken(EasyMock.anyString())).andReturn(listRequest).anyTimes();

    Objects resultObjects = new Objects();
    resultObjects.setItems(objects);

    EasyMock.expect(
        listRequest.execute()
    ).andReturn(resultObjects).once();
  }

  public static void expectDeleteObjects(
      GoogleStorage storage,
      List<StorageObject> deleteObjectExpected,
      Map<StorageObject, Exception> deleteObjectToException
  ) throws IOException
  {
    Map<StorageObject, IExpectationSetters<StorageObject>> requestToResultExpectationSetter = new HashMap<>();
    for (Map.Entry<StorageObject, Exception> deleteObjectAndException : deleteObjectToException.entrySet()) {
      StorageObject deleteObject = deleteObjectAndException.getKey();
      Exception exception = deleteObjectAndException.getValue();
      IExpectationSetters<StorageObject> resultExpectationSetter = requestToResultExpectationSetter.get(deleteObject);
      if (resultExpectationSetter == null) {
        storage.delete(deleteObject.getBucket(), deleteObject.getName());
        resultExpectationSetter = EasyMock.<StorageObject>expectLastCall().andThrow(exception);
        requestToResultExpectationSetter.put(deleteObject, resultExpectationSetter);
      } else {
        resultExpectationSetter.andThrow(exception);
      }
    }

    for (StorageObject deleteObject : deleteObjectExpected) {
      IExpectationSetters<StorageObject> resultExpectationSetter = requestToResultExpectationSetter.get(deleteObject);
      if (resultExpectationSetter == null) {
        storage.delete(deleteObject.getBucket(), deleteObject.getName());
        resultExpectationSetter = EasyMock.expectLastCall();
        requestToResultExpectationSetter.put(deleteObject, resultExpectationSetter);
      }
      resultExpectationSetter.andVoid();
    }
  }
}
