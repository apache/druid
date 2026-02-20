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

import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.StringUtils;
import org.easymock.EasyMock;
import org.easymock.EasyMockSupport;
import org.easymock.IArgumentMatcher;
import org.easymock.IExpectationSetters;
import org.joda.time.DateTime;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectsResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.net.URI;
import java.time.Instant;
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
                          && deleteObjectsRequest.bucket()
                                                 .equals(((DeleteObjectsRequest) argument).bucket())
                          && deleteObjectsRequest.delete().objects().size() == ((DeleteObjectsRequest) argument).delete().objects()
                                                                                                       .size();
        if (matches) {
          Map<String, String> expectedKeysAndVersions = deleteObjectsRequest.delete().objects().stream().collect(
              Collectors.toMap(ObjectIdentifier::key, x -> {
                return x.versionId() == null ? "null" : x.versionId();
              }));
          Map<String, String> actualKeysAndVersions = ((DeleteObjectsRequest) argument).delete().objects().stream().collect(
              Collectors.toMap(ObjectIdentifier::key, x -> {
                return x.versionId() == null ? "null" : x.versionId();
              }));
          matches = expectedKeysAndVersions.equals(actualKeysAndVersions);
        }
        return matches;
      }

      @Override
      public void appendTo(StringBuffer buffer)
      {
        String str = "DeleteObjectsRequest(\"bucketName:\" \""
                     + deleteObjectsRequest.bucket()
                     + "\", \"keys:\""
                     + deleteObjectsRequest.delete().objects()
                     + "\")";
        buffer.append(str);
      }
    });
    return null;
  }

  public static void expectListObjects(
      ServerSideEncryptingAmazonS3 s3Client,
      URI prefix,
      List<S3Object> objectSummaries)
  {
    final ListObjectsV2Response result = ListObjectsV2Response.builder()
        .keyCount(objectSummaries.size())
        .contents(objectSummaries)
        .isTruncated(false)
        .build();

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
    Map<DeleteObjectsRequest, IExpectationSetters<DeleteObjectsResponse>> requestToResultExpectationSetter = new HashMap<>();

    for (Map.Entry<DeleteObjectsRequest, Exception> requestsAndErrors : requestToException.entrySet()) {
      DeleteObjectsRequest request = requestsAndErrors.getKey();
      Exception exception = requestsAndErrors.getValue();
      IExpectationSetters<DeleteObjectsResponse> resultExpectationSetter = requestToResultExpectationSetter.get(request);
      if (resultExpectationSetter == null) {
        EasyMock.expect(s3Client.deleteObjects(
            S3TestUtils.deleteObjectsRequestArgumentMatcher(request))).andThrow(exception);
        resultExpectationSetter = EasyMock.expectLastCall();
        requestToResultExpectationSetter.put(request, resultExpectationSetter);
      } else {
        resultExpectationSetter.andThrow(exception);
      }
    }

    for (DeleteObjectsRequest request : deleteRequestsExpected) {
      IExpectationSetters<DeleteObjectsResponse> resultExpectationSetter = requestToResultExpectationSetter.get(request);
      if (resultExpectationSetter == null) {
        EasyMock.expect(s3Client.deleteObjects(S3TestUtils.deleteObjectsRequestArgumentMatcher(request)))
            .andReturn(DeleteObjectsResponse.builder().build());
        resultExpectationSetter = EasyMock.expectLastCall();
        requestToResultExpectationSetter.put(request, resultExpectationSetter);
      } else {
        // Chain a successful return after the exception for retry scenarios
        resultExpectationSetter.andReturn(DeleteObjectsResponse.builder().build());
      }
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
            return prefixUri.getAuthority().equals(request.bucket())
                   && S3Utils.extractS3Key(prefixUri).equals(request.prefix());
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

  public static S3Object newS3Object(
      String key,
      long lastModifiedTimestamp)
  {
    return S3Object.builder()
        .key(key)
        .lastModified(Instant.ofEpochMilli(lastModifiedTimestamp))
        .eTag("etag")
        .size((long) CONTENT.length)
        .build();
  }

  /**
   * Alias for newS3Object for backward compatibility with tests that used v1 SDK's S3ObjectSummary.
   * In v2 SDK, S3ObjectSummary has been replaced with S3Object.
   */
  public static S3Object newS3ObjectSummary(
      String key,
      long lastModifiedTimestamp)
  {
    return newS3Object(key, lastModifiedTimestamp);
  }
}
