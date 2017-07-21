/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.storage.s3;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.druid.java.util.common.MapUtils;
import io.druid.segment.loading.SegmentLoadingException;
import io.druid.timeline.DataSegment;
import io.druid.timeline.partition.NoneShardSpec;
import org.jets3t.service.S3ServiceException;
import org.jets3t.service.ServiceException;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;
import org.jets3t.service.model.S3Object;
import org.jets3t.service.model.StorageObject;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;
import java.util.Set;

public class S3DataSegmentMoverTest
{
  private static final DataSegment sourceSegment = new DataSegment(
      "test",
      new Interval("2013-01-01/2013-01-02"),
      "1",
      ImmutableMap.<String, Object>of(
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
    MockStorageService mockS3Client = new MockStorageService();
    S3DataSegmentMover mover = new S3DataSegmentMover(mockS3Client, new S3DataSegmentPusherConfig());

    mockS3Client.putObject("main", new S3Object("baseKey/test/2013-01-01T00:00:00.000Z_2013-01-02T00:00:00.000Z/1/0/index.zip"));
    mockS3Client.putObject("main", new S3Object("baseKey/test/2013-01-01T00:00:00.000Z_2013-01-02T00:00:00.000Z/1/0/descriptor.json"));

    DataSegment movedSegment = mover.move(
        sourceSegment,
        ImmutableMap.<String, Object>of("baseKey", "targetBaseKey", "bucket", "archive")
    );

    Map<String, Object> targetLoadSpec = movedSegment.getLoadSpec();
    Assert.assertEquals("targetBaseKey/test/2013-01-01T00:00:00.000Z_2013-01-02T00:00:00.000Z/1/0/index.zip", MapUtils.getString(targetLoadSpec, "key"));
    Assert.assertEquals("archive", MapUtils.getString(targetLoadSpec, "bucket"));
    Assert.assertTrue(mockS3Client.didMove());
  }

  @Test
  public void testMoveNoop() throws Exception
  {
    MockStorageService mockS3Client = new MockStorageService();
    S3DataSegmentMover mover = new S3DataSegmentMover(mockS3Client, new S3DataSegmentPusherConfig());

    mockS3Client.putObject("archive", new S3Object("targetBaseKey/test/2013-01-01T00:00:00.000Z_2013-01-02T00:00:00.000Z/1/0/index.zip"));
    mockS3Client.putObject("archive", new S3Object("targetBaseKey/test/2013-01-01T00:00:00.000Z_2013-01-02T00:00:00.000Z/1/0/descriptor.json"));

    DataSegment movedSegment = mover.move(
        sourceSegment,
        ImmutableMap.<String, Object>of("baseKey", "targetBaseKey", "bucket", "archive")
    );

    Map<String, Object> targetLoadSpec = movedSegment.getLoadSpec();

    Assert.assertEquals("targetBaseKey/test/2013-01-01T00:00:00.000Z_2013-01-02T00:00:00.000Z/1/0/index.zip", MapUtils.getString(targetLoadSpec, "key"));
    Assert.assertEquals("archive", MapUtils.getString(targetLoadSpec, "bucket"));
    Assert.assertFalse(mockS3Client.didMove());
  }

  @Test(expected = SegmentLoadingException.class)
  public void testMoveException() throws Exception
  {
    MockStorageService mockS3Client = new MockStorageService();
    S3DataSegmentMover mover = new S3DataSegmentMover(mockS3Client, new S3DataSegmentPusherConfig());

    mover.move(
        sourceSegment,
        ImmutableMap.<String, Object>of("baseKey", "targetBaseKey", "bucket", "archive")
    );
  }
  
  @Test
  public void testIgnoresGoneButAlreadyMoved() throws Exception
  {
    MockStorageService mockS3Client = new MockStorageService();
    S3DataSegmentMover mover = new S3DataSegmentMover(mockS3Client, new S3DataSegmentPusherConfig());
    mover.move(new DataSegment(
        "test",
        new Interval("2013-01-01/2013-01-02"),
        "1",
        ImmutableMap.<String, Object>of(
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
    ), ImmutableMap.<String, Object>of("bucket", "DOES NOT EXIST", "baseKey", "baseKey"));
  }

  @Test(expected = SegmentLoadingException.class)
  public void testFailsToMoveMissing() throws Exception
  {
    MockStorageService mockS3Client = new MockStorageService();
    S3DataSegmentMover mover = new S3DataSegmentMover(mockS3Client, new S3DataSegmentPusherConfig());
    mover.move(new DataSegment(
        "test",
        new Interval("2013-01-01/2013-01-02"),
        "1",
        ImmutableMap.<String, Object>of(
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
    ), ImmutableMap.<String, Object>of("bucket", "DOES NOT EXIST", "baseKey", "baseKey2"));
  }

  private static class MockStorageService extends RestS3Service
  {
    Map<String, Set<String>> storage = Maps.newHashMap();
    boolean moved = false;

    private MockStorageService() throws S3ServiceException
    {
      super(null);
    }

    public boolean didMove()
    {
      return moved;
    }

    @Override
    public boolean isObjectInBucket(String bucketName, String objectKey) throws ServiceException
    {
      Set<String> objects = storage.get(bucketName);
      return (objects != null && objects.contains(objectKey));
    }

    @Override
    public S3Object[] listObjects(String bucketName, String objectKey, String separator)
    {
      try {
        if (isObjectInBucket(bucketName, objectKey)) {
          final S3Object object = new S3Object(objectKey);
          object.setStorageClass(S3Object.STORAGE_CLASS_STANDARD);
          return new S3Object[]{object};
        }
      }
      catch (ServiceException e) {
        // return empty list
      }
      return new S3Object[]{};
    }

    @Override
    public Map<String, Object> moveObject(
        String sourceBucketName,
        String sourceObjectKey,
        String destinationBucketName,
        StorageObject destinationObject,
        boolean replaceMetadata
    ) throws ServiceException
    {
      moved = true;
      if(isObjectInBucket(sourceBucketName, sourceObjectKey)) {
        this.putObject(destinationBucketName, new S3Object(destinationObject.getKey()));
        storage.get(sourceBucketName).remove(sourceObjectKey);
      }
      return null;
    }

    @Override
    public S3Object putObject(String bucketName, S3Object object) throws S3ServiceException
    {
      if (!storage.containsKey(bucketName)) {
        storage.put(bucketName, Sets.<String>newHashSet());
      }
      storage.get(bucketName).add(object.getKey());
      return object;
    }
  }
}
