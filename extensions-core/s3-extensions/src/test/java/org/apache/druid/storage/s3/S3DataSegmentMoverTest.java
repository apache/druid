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

import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.MapUtils;
import org.apache.druid.segment.loading.SegmentLoadingException;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.NoneShardSpec;
import org.junit.Assert;
import org.junit.Test;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.CopyObjectResponse;
import software.amazon.awssdk.services.s3.model.GetBucketAclResponse;
import software.amazon.awssdk.services.s3.model.Grant;
import software.amazon.awssdk.services.s3.model.Grantee;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.Owner;
import software.amazon.awssdk.services.s3.model.Permission;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.model.StorageClass;
import software.amazon.awssdk.services.s3.model.Type;

import java.io.File;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class S3DataSegmentMoverTest
{
  private static final DataSegment SOURCE_SEGMENT = new DataSegment(
      "test",
      Intervals.of("2013-01-01/2013-01-02"),
      "1",
      ImmutableMap.of(
          "key",
          "baseKey/test/2013-01-01T00:00:00.000Z_2013-01-02T00:00:00.000Z/1/0/index.zip",
          "bucket",
          "main"
      ),
      ImmutableList.of("dim1", "dim1"),
      ImmutableList.of("metric1", "metric2"),
      NoneShardSpec.instance(),
      0,
      1
  );

  @Test
  public void testMove() throws Exception
  {
    MockAmazonS3Client mockS3Client = new MockAmazonS3Client();
    S3DataSegmentMover mover = new S3DataSegmentMover(
        Suppliers.ofInstance(mockS3Client),
        new S3DataSegmentPusherConfig()
    );

    mockS3Client.putObject(
        "main",
        "baseKey/test/2013-01-01T00:00:00.000Z_2013-01-02T00:00:00.000Z/1/0/index.zip"
    );

    DataSegment movedSegment = mover.move(
        SOURCE_SEGMENT,
        ImmutableMap.of("baseKey", "targetBaseKey", "bucket", "archive")
    );

    Map<String, Object> targetLoadSpec = movedSegment.getLoadSpec();
    Assert.assertEquals(
        "targetBaseKey/test/2013-01-01T00:00:00.000Z_2013-01-02T00:00:00.000Z/1/0/index.zip",
        MapUtils.getString(targetLoadSpec, "key")
    );
    Assert.assertEquals("archive", MapUtils.getString(targetLoadSpec, "bucket"));
    Assert.assertTrue(mockS3Client.didMove());
  }

  @Test
  public void testMoveNoop() throws Exception
  {
    MockAmazonS3Client mockS3Client = new MockAmazonS3Client();
    S3DataSegmentMover mover = new S3DataSegmentMover(
        Suppliers.ofInstance(mockS3Client),
        new S3DataSegmentPusherConfig()
    );

    mockS3Client.putObject(
        "archive",
        "targetBaseKey/test/2013-01-01T00:00:00.000Z_2013-01-02T00:00:00.000Z/1/0/index.zip"
    );

    DataSegment movedSegment = mover.move(
        SOURCE_SEGMENT,
        ImmutableMap.of("baseKey", "targetBaseKey", "bucket", "archive")
    );

    Map<String, Object> targetLoadSpec = movedSegment.getLoadSpec();

    Assert.assertEquals(
        "targetBaseKey/test/2013-01-01T00:00:00.000Z_2013-01-02T00:00:00.000Z/1/0/index.zip",
        MapUtils.getString(targetLoadSpec, "key")
    );
    Assert.assertEquals("archive", MapUtils.getString(targetLoadSpec, "bucket"));
    Assert.assertFalse(mockS3Client.didMove());
  }

  @Test(expected = SegmentLoadingException.class)
  public void testMoveException() throws Exception
  {
    MockAmazonS3Client mockS3Client = new MockAmazonS3Client();
    S3DataSegmentMover mover = new S3DataSegmentMover(
        Suppliers.ofInstance(mockS3Client),
        new S3DataSegmentPusherConfig()
    );

    mover.move(
        SOURCE_SEGMENT,
        ImmutableMap.of("baseKey", "targetBaseKey", "bucket", "archive")
    );
  }

  @Test
  public void testIgnoresGoneButAlreadyMoved() throws Exception
  {
    MockAmazonS3Client mockS3Client = new MockAmazonS3Client();
    S3DataSegmentMover mover = new S3DataSegmentMover(
        Suppliers.ofInstance(mockS3Client),
        new S3DataSegmentPusherConfig()
    );
    mover.move(new DataSegment(
        "test",
        Intervals.of("2013-01-01/2013-01-02"),
        "1",
        ImmutableMap.of(
            "key",
            "baseKey/test/2013-01-01T00:00:00.000Z_2013-01-02T00:00:00.000Z/1/0/index.zip",
            "bucket",
            "DOES NOT EXIST"
        ),
        ImmutableList.of("dim1", "dim1"),
        ImmutableList.of("metric1", "metric2"),
        NoneShardSpec.instance(),
        0,
        1
    ), ImmutableMap.of("bucket", "DOES NOT EXIST", "baseKey", "baseKey"));
  }

  @Test(expected = SegmentLoadingException.class)
  public void testFailsToMoveMissing() throws Exception
  {
    MockAmazonS3Client mockS3Client = new MockAmazonS3Client();
    S3DataSegmentMover mover = new S3DataSegmentMover(
        Suppliers.ofInstance(mockS3Client),
        new S3DataSegmentPusherConfig()
    );
    mover.move(new DataSegment(
        "test",
        Intervals.of("2013-01-01/2013-01-02"),
        "1",
        ImmutableMap.of(
            "key",
            "baseKey/test/2013-01-01T00:00:00.000Z_2013-01-02T00:00:00.000Z/1/0/index.zip",
            "bucket",
            "DOES NOT EXIST"
        ),
        ImmutableList.of("dim1", "dim1"),
        ImmutableList.of("metric1", "metric2"),
        NoneShardSpec.instance(),
        0,
        1
    ), ImmutableMap.of("bucket", "DOES NOT EXIST", "baseKey", "baseKey2"));
  }

  private static class MockAmazonS3Client extends ServerSideEncryptingAmazonS3
  {
    Map<String, Set<String>> storage = new HashMap<>();
    boolean copied = false;
    boolean deletedOld = false;

    private MockAmazonS3Client()
    {
      super(createMockS3Client(), null, new NoopServerSideEncryption(), new S3TransferConfig());
    }

    private static S3Client createMockS3Client()
    {
      // Return a minimal mock - actual operations are overridden in this class
      return S3Client.builder()
          .region(software.amazon.awssdk.regions.Region.US_EAST_1)
          .credentialsProvider(software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider.create())
          .build();
    }

    public boolean didMove()
    {
      return copied && deletedOld;
    }

    @Override
    public GetBucketAclResponse getBucketAcl(String bucketName)
    {
      Owner owner = Owner.builder()
          .id("ownerId")
          .displayName("owner")
          .build();
      Grantee grantee = Grantee.builder()
          .id(owner.id())
          .type(Type.CANONICAL_USER)
          .build();
      Grant grant = Grant.builder()
          .grantee(grantee)
          .permission(Permission.FULL_CONTROL)
          .build();
      return GetBucketAclResponse.builder()
          .owner(owner)
          .grants(grant)
          .build();
    }

    @Override
    public boolean doesObjectExist(String bucketName, String objectKey)
    {
      Set<String> objects = storage.get(bucketName);
      return (objects != null && objects.contains(objectKey));
    }

    @Override
    public ListObjectsV2Response listObjectsV2(ListObjectsV2Request listObjectsV2Request)
    {
      final String bucketName = listObjectsV2Request.bucket();
      final String objectKey = listObjectsV2Request.prefix();
      if (doesObjectExist(bucketName, objectKey)) {
        final S3Object s3Object = S3Object.builder()
            .key(objectKey)
            .storageClass(StorageClass.STANDARD.toString())
            .build();

        return ListObjectsV2Response.builder()
            .name(bucketName)
            .prefix(objectKey)
            .keyCount(1)
            .contents(s3Object)
            .isTruncated(true)
            .build();
      } else {
        return ListObjectsV2Response.builder().build();
      }
    }

    @Override
    public CopyObjectResponse copyObject(CopyObjectRequest.Builder requestBuilder)
    {
      CopyObjectRequest request = requestBuilder.build();
      final String sourceBucketName = request.sourceBucket();
      final String sourceObjectKey = request.sourceKey();
      final String destinationBucketName = request.destinationBucket();
      final String destinationObjectKey = request.destinationKey();
      copied = true;
      if (doesObjectExist(sourceBucketName, sourceObjectKey)) {
        storage.computeIfAbsent(destinationBucketName, k -> new HashSet<>())
               .add(destinationObjectKey);
        return CopyObjectResponse.builder().build();
      } else {
        throw (S3Exception) S3Exception.builder()
            .message("S3DataSegmentMoverTest")
            .awsErrorDetails(software.amazon.awssdk.awscore.exception.AwsErrorDetails.builder()
                .errorCode("NoSuchKey")
                .errorMessage("S3DataSegmentMoverTest")
                .build())
            .statusCode(404)
            .build();
      }
    }

    @Override
    public void deleteObject(String bucket, String objectKey)
    {
      deletedOld = true;
      storage.get(bucket).remove(objectKey);
    }

    public PutObjectResponse putObject(String bucketName, String key)
    {
      return putObject(bucketName, key, (File) null);
    }

    @Override
    public PutObjectResponse putObject(String bucketName, String key, File file)
    {
      storage.computeIfAbsent(bucketName, bName -> new HashSet<>()).add(key);
      return PutObjectResponse.builder().build();
    }
  }
}
