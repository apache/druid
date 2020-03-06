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
import junit.framework.AssertionFailedError;
import org.apache.commons.collections4.map.HashedMap;
import org.easymock.EasyMock;
import org.easymock.EasyMockSupport;
import org.easymock.IArgumentMatcher;
import org.easymock.IExpectationSetters;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class S3TestUtils extends EasyMockSupport
{
  public static ListObjectsV2Request listObjectsV2RequestArgumentMatcher(ListObjectsV2Request listObjectsV2Request)
  {
    EasyMock.reportMatcher(new IArgumentMatcher()
    {
      @Override
      public boolean matches(Object argument)
      {

        return argument instanceof ListObjectsV2Request
               && listObjectsV2Request.getBucketName().equals(((ListObjectsV2Request) argument).getBucketName())
               && listObjectsV2Request.getPrefix().equals(((ListObjectsV2Request) argument).getPrefix())
               && ((listObjectsV2Request.getContinuationToken() == null
                    && ((ListObjectsV2Request) argument).getContinuationToken() == null)
                   || (listObjectsV2Request.getContinuationToken()
                                           .equals(((ListObjectsV2Request) argument).getContinuationToken())))
               && listObjectsV2Request.getMaxKeys().equals(((ListObjectsV2Request) argument).getMaxKeys());
      }

      @Override
      public void appendTo(StringBuffer buffer)
      {
        String str = "ListObjectsV2Request(\"bucketName:\" \""
                     + listObjectsV2Request.getBucketName()
                     + "\", \"prefix:\""
                     + listObjectsV2Request.getPrefix()
                     + "\", \"continuationToken:\""
                     + listObjectsV2Request.getContinuationToken()
                     + "\", \"maxKeys:\""
                     + listObjectsV2Request.getMaxKeys()
                     + "\")";
        buffer.append(str);
      }
    });
    return null;
  }

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

  public static S3ObjectSummary mockS3ObjectSummary(long lastModified, String key)
  {
    S3ObjectSummary objectSummary = EasyMock.createMock(S3ObjectSummary.class);
    EasyMock.expect(objectSummary.getLastModified()).andReturn(new Date(lastModified));
    EasyMock.expectLastCall().anyTimes();
    EasyMock.expect(objectSummary.getKey()).andReturn(key);
    EasyMock.expectLastCall().anyTimes();
    return objectSummary;
  }

  public static ListObjectsV2Request mockRequest(
      String bucket,
      String prefix,
      int maxKeys,
      String continuationToken
  )
  {
    ListObjectsV2Request request = EasyMock.createMock(ListObjectsV2Request.class);
    EasyMock.expect(request.getBucketName()).andReturn(bucket);
    EasyMock.expectLastCall().anyTimes();
    EasyMock.expect(request.getPrefix()).andReturn(prefix);
    EasyMock.expectLastCall().anyTimes();
    EasyMock.expect(request.getMaxKeys()).andReturn(maxKeys);
    EasyMock.expectLastCall().anyTimes();
    EasyMock.expect(request.getContinuationToken()).andReturn(continuationToken);
    EasyMock.expectLastCall().anyTimes();
    return request;
  }

  public static ListObjectsV2Result mockResult(
      String continuationToken,
      boolean isTruncated,
      List<S3ObjectSummary> objectSummaries
  )
  {
    ListObjectsV2Result result = EasyMock.createMock(ListObjectsV2Result.class);
    EasyMock.expect(result.getNextContinuationToken()).andReturn(continuationToken);
    EasyMock.expectLastCall().anyTimes();
    EasyMock.expect(result.isTruncated()).andReturn(isTruncated);
    EasyMock.expectLastCall().anyTimes();
    EasyMock.expect(result.getObjectSummaries()).andReturn(objectSummaries);
    return result;
  }

  public static ServerSideEncryptingAmazonS3 mockS3ClientListObjectsV2(
      Map<ListObjectsV2Request, ListObjectsV2Result> requestsToResults,
      Map<ListObjectsV2Request, Exception> requestsToExceptions
  )
  {
    Map<ListObjectsV2Request, IExpectationSetters<ListObjectsV2Result>> requestToResultExpectationSetter = new HashedMap<>();
    ServerSideEncryptingAmazonS3 s3Client = EasyMock.createMock(ServerSideEncryptingAmazonS3.class);
    for (Map.Entry<ListObjectsV2Request, Exception> requestsAndErrors : requestsToExceptions.entrySet()) {
      ListObjectsV2Request request = requestsAndErrors.getKey();
      Exception exception = requestsAndErrors.getValue();
      IExpectationSetters<ListObjectsV2Result> resultExpectationSetter = requestToResultExpectationSetter.get(request);
      if (resultExpectationSetter == null) {
        s3Client.listObjectsV2(
            S3TestUtils.listObjectsV2RequestArgumentMatcher(request));
        resultExpectationSetter = EasyMock.<ListObjectsV2Result>expectLastCall().andThrow(exception);
        requestToResultExpectationSetter.put(request, resultExpectationSetter);
      } else {
        resultExpectationSetter.andThrow(exception);
      }
    }

    for (Map.Entry<ListObjectsV2Request, ListObjectsV2Result> requestsAndResults : requestsToResults.entrySet()) {
      ListObjectsV2Request request = requestsAndResults.getKey();
      ListObjectsV2Result result = requestsAndResults.getValue();
      IExpectationSetters<ListObjectsV2Result> resultExpectationSetter = requestToResultExpectationSetter.get(request);
      if (resultExpectationSetter == null) {
        resultExpectationSetter = EasyMock.expect(s3Client.listObjectsV2(
            S3TestUtils.listObjectsV2RequestArgumentMatcher(request)));
        requestToResultExpectationSetter.put(request, resultExpectationSetter);
      }
      resultExpectationSetter.andReturn(result);
    }
    return s3Client;
  }

  public static void mockS3ClientDeleteObjects(
      ServerSideEncryptingAmazonS3 s3Client,
      List<DeleteObjectsRequest> deleteRequestsExpected,
      List<DeleteObjectsRequest> deleteRequestsNotExpected
  )
  {
    for (DeleteObjectsRequest deleteRequestExpected : deleteRequestsExpected) {
      s3Client.deleteObjects(S3TestUtils.deleteObjectsRequestArgumentMatcher(deleteRequestExpected));
      EasyMock.expectLastCall();
    }

    for (DeleteObjectsRequest deleteRequestNotExpected : deleteRequestsNotExpected) {
      s3Client.deleteObjects(S3TestUtils.deleteObjectsRequestArgumentMatcher(deleteRequestNotExpected));
      EasyMock.expectLastCall().andThrow(new AssertionFailedError()).anyTimes();
    }
  }
}
