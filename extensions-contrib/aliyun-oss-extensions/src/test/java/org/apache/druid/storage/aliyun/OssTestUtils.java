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

package org.apache.druid.storage.aliyun;

import com.aliyun.oss.OSS;
import com.aliyun.oss.model.DeleteObjectsRequest;
import com.aliyun.oss.model.DeleteObjectsResult;
import com.aliyun.oss.model.ListObjectsRequest;
import com.aliyun.oss.model.OSSObjectSummary;
import com.aliyun.oss.model.ObjectListing;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.StringUtils;
import org.easymock.EasyMock;
import org.easymock.EasyMockSupport;
import org.easymock.IArgumentMatcher;
import org.easymock.IExpectationSetters;
import org.joda.time.DateTime;

import java.net.URI;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class OssTestUtils extends EasyMockSupport
{
  private static final DateTime NOW = DateTimes.nowUtc();
  private static final byte[] CONTENT =
      StringUtils.toUtf8(StringUtils.format("%d,hello,world", NOW.getMillis()));

  public static DeleteObjectsRequest deleteObjectsRequestArgumentMatcher(DeleteObjectsRequest deleteObjectsRequest)
  {
    EasyMock.reportMatcher(new IArgumentMatcher()
    {
      @Override
      public boolean matches(Object argument)
      {

        boolean matches = argument instanceof DeleteObjectsRequest
                          && deleteObjectsRequest.getBucketName()
                                                 .equals(((DeleteObjectsRequest) argument).getBucketName())
                          && deleteObjectsRequest.getKeys().size() == ((DeleteObjectsRequest) argument).getKeys()
                                                                                                       .size();
        if (matches) {
          List<String> expectedKeysAndVersions = deleteObjectsRequest.getKeys();
          List<String> actualKeysAndVersions = ((DeleteObjectsRequest) argument).getKeys();
          matches = expectedKeysAndVersions.equals(actualKeysAndVersions);
        }
        return matches;
      }

      @Override
      public void appendTo(StringBuffer buffer)
      {
        String str = "DeleteObjectsRequest(\"bucketName:\" \""
                     + deleteObjectsRequest.getBucketName()
                     + "\", \"keys:\""
                     + deleteObjectsRequest.getKeys()
                     + "\")";
        buffer.append(str);
      }
    });
    return null;
  }

  public static void expectListObjects(
      OSS client,
      URI prefix,
      List<OSSObjectSummary> objectSummaries
  )
  {
    final ObjectListing result = new ObjectListing();
    result.setBucketName(prefix.getAuthority());
    //result.setsetKeyCount(objectSummaries.size());
    for (OSSObjectSummary objectSummary : objectSummaries) {
      result.getObjectSummaries().add(objectSummary);
    }

    EasyMock.expect(
        client.listObjects(matchListObjectsRequest(prefix))
    ).andReturn(result).once();
  }

  public static void mockClientDeleteObjects(
      OSS client,
      List<DeleteObjectsRequest> deleteRequestsExpected,
      Map<DeleteObjectsRequest, Exception> requestToException
  )
  {
    Map<DeleteObjectsRequest, IExpectationSetters<DeleteObjectsResult>> requestToResultExpectationSetter = new HashMap<>();

    for (Map.Entry<DeleteObjectsRequest, Exception> requestsAndErrors : requestToException.entrySet()) {
      DeleteObjectsRequest request = requestsAndErrors.getKey();
      Exception exception = requestsAndErrors.getValue();
      IExpectationSetters<DeleteObjectsResult> resultExpectationSetter = requestToResultExpectationSetter.get(request);
      if (resultExpectationSetter == null) {
        client.deleteObjects(
            OssTestUtils.deleteObjectsRequestArgumentMatcher(request));
        resultExpectationSetter = EasyMock.<DeleteObjectsResult>expectLastCall().andThrow(exception);
        requestToResultExpectationSetter.put(request, resultExpectationSetter);
      } else {
        resultExpectationSetter.andThrow(exception);
      }
    }

    for (DeleteObjectsRequest request : deleteRequestsExpected) {
      IExpectationSetters<DeleteObjectsResult> resultExpectationSetter = requestToResultExpectationSetter.get(request);
      if (resultExpectationSetter == null) {
        client.deleteObjects(OssTestUtils.deleteObjectsRequestArgumentMatcher(request));
        resultExpectationSetter = EasyMock.expectLastCall();
        requestToResultExpectationSetter.put(request, resultExpectationSetter);
      }
      resultExpectationSetter.andReturn(new DeleteObjectsResult());
    }
  }

  public static ListObjectsRequest matchListObjectsRequest(final URI prefixUri)
  {
    // Use an IArgumentMatcher to verify that the request has the correct bucket and prefix.
    EasyMock.reportMatcher(
        new IArgumentMatcher()
        {
          @Override
          public boolean matches(Object argument)
          {
            if (!(argument instanceof ListObjectsRequest)) {
              return false;
            }

            final ListObjectsRequest request = (ListObjectsRequest) argument;
            return prefixUri.getAuthority().equals(request.getBucketName())
                   && OssUtils.extractKey(prefixUri).equals(request.getPrefix());
          }

          @Override
          public void appendTo(StringBuffer buffer)
          {
            buffer.append("<request for prefix [").append(prefixUri).append("]>");
          }
        }
    );

    return null;
  }

  public static OSSObjectSummary newOSSObjectSummary(
      String bucket,
      String key,
      long lastModifiedTimestamp
  )
  {
    OSSObjectSummary objectSummary = new OSSObjectSummary();
    objectSummary.setBucketName(bucket);
    objectSummary.setKey(key);
    objectSummary.setLastModified(new Date(lastModifiedTimestamp));
    objectSummary.setETag("etag");
    objectSummary.setSize(CONTENT.length);
    return objectSummary;
  }
}
