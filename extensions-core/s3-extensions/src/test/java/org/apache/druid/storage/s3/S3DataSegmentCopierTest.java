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

import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3ObjectSummary;
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

import java.util.Map;

public class S3DataSegmentCopierTest
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
  public void testCopy() throws Exception
  {
    S3TestUtils.MockAmazonS3Client mockS3Client = new S3TestUtils.MockAmazonS3Client();
    S3DataSegmentCopier copier = new S3DataSegmentCopier(
        Suppliers.ofInstance(mockS3Client),
        new S3DataSegmentPusherConfig()
    );

    mockS3Client.putObject(
        "main",
        "baseKey/test/2013-01-01T00:00:00.000Z_2013-01-02T00:00:00.000Z/1/0/index.zip"
    );

    DataSegment copiedSegment = copier.copy(
        SOURCE_SEGMENT,
        ImmutableMap.of("baseKey", "targetBaseKey", "bucket", "archive")
    );

    Map<String, Object> targetLoadSpec = copiedSegment.getLoadSpec();
    Assert.assertEquals(
        "targetBaseKey/test/2013-01-01T00:00:00.000Z_2013-01-02T00:00:00.000Z/1/0/index.zip",
        MapUtils.getString(targetLoadSpec, "key")
    );
    Assert.assertEquals("archive", MapUtils.getString(targetLoadSpec, "bucket"));
    Assert.assertTrue(mockS3Client.didCopy());
  }

  @Test
  public void testCopyNoop() throws Exception
  {
    S3TestUtils.MockAmazonS3Client mockS3Client = new S3TestUtils.MockAmazonS3Client();
    S3DataSegmentCopier copier = new S3DataSegmentCopier(
        Suppliers.ofInstance(mockS3Client),
        new S3DataSegmentPusherConfig()
    );

    mockS3Client.putObject(
        "archive",
        "targetBaseKey/test/2013-01-01T00:00:00.000Z_2013-01-02T00:00:00.000Z/1/0/index.zip"
    );

    DataSegment copiedSegment = copier.copy(
        SOURCE_SEGMENT,
        ImmutableMap.of("baseKey", "targetBaseKey", "bucket", "archive")
    );

    Map<String, Object> targetLoadSpec = copiedSegment.getLoadSpec();

    Assert.assertEquals(
        "targetBaseKey/test/2013-01-01T00:00:00.000Z_2013-01-02T00:00:00.000Z/1/0/index.zip",
        MapUtils.getString(targetLoadSpec, "key")
    );
    Assert.assertEquals("archive", MapUtils.getString(targetLoadSpec, "bucket"));
    Assert.assertFalse(mockS3Client.didCopy());
  }

  @Test(expected = SegmentLoadingException.class)
  public void testCopyException() throws Exception
  {
    S3TestUtils.MockAmazonS3Client mockS3Client = new S3TestUtils.MockAmazonS3Client();
    S3DataSegmentCopier copier = new S3DataSegmentCopier(
        Suppliers.ofInstance(mockS3Client),
        new S3DataSegmentPusherConfig()
    );

    copier.copy(
        SOURCE_SEGMENT,
        ImmutableMap.of("baseKey", "targetBaseKey", "bucket", "archive")
    );
  }

  @Test
  public void testIgnoresGoneButAlreadyCopied() throws Exception
  {
    S3TestUtils.MockAmazonS3Client mockS3Client = new S3TestUtils.MockAmazonS3Client();
    S3DataSegmentCopier copier = new S3DataSegmentCopier(
        Suppliers.ofInstance(mockS3Client),
        new S3DataSegmentPusherConfig()
    );
    copier.copy(new DataSegment(
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
  public void testFailsToCopyMissing() throws Exception
  {
    S3TestUtils.MockAmazonS3Client mockS3Client = new S3TestUtils.MockAmazonS3Client();
    S3DataSegmentCopier copier = new S3DataSegmentCopier(
        Suppliers.ofInstance(mockS3Client),
        new S3DataSegmentPusherConfig()
    );
    copier.copy(new DataSegment(
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

  @Test
  public void testCopyWithTransferManagerForLargeSegments() throws Exception
  {
    final long bigSegmentSize = 10 * (2L << 30); // 10GB
    S3TestUtils.MockAmazonS3Client mockS3Client = new S3TestUtils.MockAmazonS3Client()
    {
      @Override
      public ListObjectsV2Result listObjectsV2(ListObjectsV2Request listObjectsV2Request)
      {
        final ListObjectsV2Result res = super.listObjectsV2(listObjectsV2Request);
        for (S3ObjectSummary summary : res.getObjectSummaries()) {
          summary.setSize(bigSegmentSize);
        }
        return res;
      }
    };

    S3DataSegmentCopier copier = new S3DataSegmentCopier(
        Suppliers.ofInstance(mockS3Client),
        new S3DataSegmentPusherConfig()
    );

    mockS3Client.putObject(
        "main",
        "baseKey/test/2013-01-01T00:00:00.000Z_2013-01-02T00:00:00.000Z/1/0/index.zip"
    );

    DataSegment copiedSegment = copier.copy(
        SOURCE_SEGMENT.withSize(bigSegmentSize),
        ImmutableMap.of("baseKey", "targetBaseKey", "bucket", "archive")
    );

    Map<String, Object> targetLoadSpec = copiedSegment.getLoadSpec();
    Assert.assertEquals(
        "targetBaseKey/test/2013-01-01T00:00:00.000Z_2013-01-02T00:00:00.000Z/1/0/index.zip",
        MapUtils.getString(targetLoadSpec, "key")
    );
    Assert.assertEquals("archive", MapUtils.getString(targetLoadSpec, "bucket"));
    Assert.assertTrue(mockS3Client.didCopy());
    Assert.assertTrue(mockS3Client.didUseTransferManager());
  }
}
