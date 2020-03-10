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

package org.apache.druid.storage.s3;

import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3ObjectSummary;
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
import java.util.stream.Collectors;

public class S3TestUtils extends EasyMockSupport
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
          Map<String, String> expectedKeysAndVersions = deleteObjectsRequest.getKeys().stream().collect(
              Collectors.toMap(DeleteObjectsRequest.KeyVersion::getKey, x -> {
                return x.getVersion() == null ? "null" : x.getVersion();
              }));
          Map<String, String> actualKeysAndVersions = ((DeleteObjectsRequest) argument).getKeys().stream().collect(
              Collectors.toMap(DeleteObjectsRequest.KeyVersion::getKey, x -> {
                return x.getVersion() == null ? "null" : x.getVersion();
              }));
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
      ServerSideEncryptingAmazonS3 s3Client,
      URI prefix,
      List<S3ObjectSummary> objectSummaries)
  {
    final ListObjectsV2Result result = new ListObjectsV2Result();
    result.setBucketName(prefix.getAuthority());
    result.setKeyCount(objectSummaries.size());
    for (S3ObjectSummary objectSummary : objectSummaries) {
      result.getObjectSummaries().add(objectSummary);
    }

    EasyMock.expect(
        s3Client.listObjectsV2(matchListObjectsRequest(prefix))
    ).andReturn(result).once();
  }

  public static void mockS3ClientDeleteObjects(
      ServerSideEncryptingAmazonS3 s3Client,
      List<DeleteObjectsRequest> deleteRequestsExpected,
      Map<DeleteObjectsRequest, Exception> requestToException
  )
  {
    Map<DeleteObjectsRequest, IExpectationSetters<DeleteObjectsRequest>> requestToResultExpectationSetter = new HashMap<>();

    for (Map.Entry<DeleteObjectsRequest, Exception> requestsAndErrors : requestToException.entrySet()) {
      DeleteObjectsRequest request = requestsAndErrors.getKey();
      Exception exception = requestsAndErrors.getValue();
      IExpectationSetters<DeleteObjectsRequest> resultExpectationSetter = requestToResultExpectationSetter.get(request);
      if (resultExpectationSetter == null) {
        s3Client.deleteObjects(
            S3TestUtils.deleteObjectsRequestArgumentMatcher(request));
        resultExpectationSetter = EasyMock.<DeleteObjectsRequest>expectLastCall().andThrow(exception);
        requestToResultExpectationSetter.put(request, resultExpectationSetter);
      } else {
        resultExpectationSetter.andThrow(exception);
      }
    }

    for (DeleteObjectsRequest request : deleteRequestsExpected) {
      IExpectationSetters<DeleteObjectsRequest> resultExpectationSetter = requestToResultExpectationSetter.get(request);
      if (resultExpectationSetter == null) {
        s3Client.deleteObjects(S3TestUtils.deleteObjectsRequestArgumentMatcher(request));
        resultExpectationSetter = EasyMock.expectLastCall();
        requestToResultExpectationSetter.put(request, resultExpectationSetter);
      }
      resultExpectationSetter.andVoid();
    }
  }

  public static ListObjectsV2Request matchListObjectsRequest(final URI prefixUri)
  {
    // Use an IArgumentMatcher to verify that the request has the correct bucket and prefix.
    EasyMock.reportMatcher(
        new IArgumentMatcher()
        {
          @Override
          public boolean matches(Object argument)
          {
            if (!(argument instanceof ListObjectsV2Request)) {
              return false;
            }

            final ListObjectsV2Request request = (ListObjectsV2Request) argument;
            return prefixUri.getAuthority().equals(request.getBucketName())
                   && S3Utils.extractS3Key(prefixUri).equals(request.getPrefix());
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

  public static S3ObjectSummary newS3ObjectSummary(
      String bucket,
      String key,
      long lastModifiedTimestamp)
  {
    S3ObjectSummary objectSummary = new S3ObjectSummary();
    objectSummary.setBucketName(bucket);
    objectSummary.setKey(key);
    objectSummary.setLastModified(new Date(lastModifiedTimestamp));
    objectSummary.setETag("etag");
    objectSummary.setSize(CONTENT.length);
    return objectSummary;
  }
}
