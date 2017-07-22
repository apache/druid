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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.google.common.base.Predicate;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.segment.loading.SegmentLoadingException;
import io.druid.timeline.DataSegment;
import io.druid.timeline.partition.NumberedShardSpec;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.jets3t.service.S3ServiceException;
import org.jets3t.service.ServiceException;
import org.jets3t.service.StorageObjectsChunk;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;
import org.jets3t.service.model.S3Object;
import org.jets3t.service.model.StorageObject;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class S3DataSegmentFinderTest
{
  private static final ObjectMapper mapper = new DefaultObjectMapper();

  private static final DataSegment SEGMENT_1 = DataSegment.builder()
                                                          .dataSource("wikipedia")
                                                          .interval(
                                                              new Interval(
                                                                  "2013-08-31T00:00:00.000Z/2013-09-01T00:00:00.000Z"
                                                              )
                                                          )
                                                          .version("2015-10-21T22:07:57.074Z")
                                                          .loadSpec(
                                                              ImmutableMap.<String, Object>of(
                                                                  "type",
                                                                  "s3_zip",
                                                                  "bucket",
                                                                  "bucket1",
                                                                  "key",
                                                                  "abc/somewhere/index.zip"
                                                              )
                                                          )
                                                          .dimensions(ImmutableList.of("language", "page"))
                                                          .metrics(ImmutableList.of("count"))
                                                          .build();

  private static final DataSegment SEGMENT_2 = DataSegment.builder(SEGMENT_1)
                                                          .interval(
                                                              new Interval(
                                                                  "2013-09-01T00:00:00.000Z/2013-09-02T00:00:00.000Z"
                                                              )
                                                          )
                                                          .build();

  private static final DataSegment SEGMENT_3 = DataSegment.builder(SEGMENT_1)
                                                          .interval(
                                                              new Interval(
                                                                  "2013-09-02T00:00:00.000Z/2013-09-03T00:00:00.000Z"
                                                              )
                                                          )
                                                          .version("2015-10-22T22:07:57.074Z")
                                                          .build();

  private static final DataSegment SEGMENT_4_0 = DataSegment.builder(SEGMENT_1)
                                                            .interval(
                                                                new Interval(
                                                                    "2013-09-02T00:00:00.000Z/2013-09-03T00:00:00.000Z"
                                                                )
                                                            )
                                                            .shardSpec(new NumberedShardSpec(0, 2))
                                                            .build();

  private static final DataSegment SEGMENT_4_1 = DataSegment.builder(SEGMENT_1)
                                                            .interval(
                                                                new Interval(
                                                                    "2013-09-02T00:00:00.000Z/2013-09-03T00:00:00.000Z"
                                                                )
                                                            )
                                                            .shardSpec(new NumberedShardSpec(1, 2))
                                                            .build();

  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  RestS3Service mockS3Client;
  S3DataSegmentPusherConfig config;

  private String bucket;
  private String baseKey;

  private String descriptor1;
  private String descriptor2;
  private String descriptor3;
  private String descriptor4_0;
  private String descriptor4_1;
  private String indexZip1;
  private String indexZip2;
  private String indexZip3;
  private String indexZip4_0;
  private String indexZip4_1;



  @BeforeClass
  public static void setUpStatic()
  {
    mapper.registerSubtypes(new NamedType(NumberedShardSpec.class, "numbered"));
  }

  @Before
  public void setUp() throws Exception
  {
    bucket = "bucket1";
    baseKey = "dataSource1";

    config = new S3DataSegmentPusherConfig();
    config.setBucket(bucket);
    config.setBaseKey(baseKey);

    mockS3Client = new MockStorageService(temporaryFolder.newFolder());


    descriptor1 = S3Utils.descriptorPathForSegmentPath(baseKey + "/interval1/v1/0/");
    descriptor2 = S3Utils.descriptorPathForSegmentPath(baseKey + "/interval2/v1/0/");
    descriptor3 = S3Utils.descriptorPathForSegmentPath(baseKey + "/interval3/v2/0/");
    descriptor4_0 = S3Utils.descriptorPathForSegmentPath(baseKey + "/interval4/v1/0/");
    descriptor4_1 = S3Utils.descriptorPathForSegmentPath(baseKey + "/interval4/v1/1/");
    indexZip1 = S3Utils.indexZipForSegmentPath(descriptor1);
    indexZip2 = S3Utils.indexZipForSegmentPath(descriptor2);
    indexZip3 = S3Utils.indexZipForSegmentPath(descriptor3);
    indexZip4_0 = S3Utils.indexZipForSegmentPath(descriptor4_0);
    indexZip4_1 = S3Utils.indexZipForSegmentPath(descriptor4_1);

    mockS3Client.putObject(bucket, new S3Object(descriptor1, mapper.writeValueAsString(SEGMENT_1)));
    mockS3Client.putObject(bucket, new S3Object(descriptor2, mapper.writeValueAsString(SEGMENT_2)));
    mockS3Client.putObject(bucket, new S3Object(descriptor3, mapper.writeValueAsString(SEGMENT_3)));
    mockS3Client.putObject(bucket, new S3Object(descriptor4_0, mapper.writeValueAsString(SEGMENT_4_0)));
    mockS3Client.putObject(bucket, new S3Object(descriptor4_1, mapper.writeValueAsString(SEGMENT_4_1)));

    mockS3Client.putObject(bucket, new S3Object(indexZip1, "dummy"));
    mockS3Client.putObject(bucket, new S3Object(indexZip2, "dummy"));
    mockS3Client.putObject(bucket, new S3Object(indexZip3, "dummy"));
    mockS3Client.putObject(bucket, new S3Object(indexZip4_0, "dummy"));
    mockS3Client.putObject(bucket, new S3Object(indexZip4_1, "dummy"));
  }

  @Test
  public void testFindSegments() throws Exception
  {
    final S3DataSegmentFinder s3DataSegmentFinder = new S3DataSegmentFinder(mockS3Client, config, mapper);
    final Set<DataSegment> segments = s3DataSegmentFinder.findSegments("", false);

    Assert.assertEquals(5, segments.size());

    DataSegment updatedSegment1 = null;
    DataSegment updatedSegment2 = null;
    DataSegment updatedSegment3 = null;
    DataSegment updatedSegment4_0 = null;
    DataSegment updatedSegment4_1 = null;

    for (DataSegment dataSegment : segments) {
      if (dataSegment.getIdentifier().equals(SEGMENT_1.getIdentifier())) {
        updatedSegment1 = dataSegment;
      } else if (dataSegment.getIdentifier().equals(SEGMENT_2.getIdentifier())) {
        updatedSegment2 = dataSegment;
      } else if (dataSegment.getIdentifier().equals(SEGMENT_3.getIdentifier())) {
        updatedSegment3 = dataSegment;
      } else if (dataSegment.getIdentifier().equals(SEGMENT_4_0.getIdentifier())) {
        updatedSegment4_0 = dataSegment;
      } else if (dataSegment.getIdentifier().equals(SEGMENT_4_1.getIdentifier())) {
        updatedSegment4_1 = dataSegment;
      } else {
        Assert.fail("Unexpected segment identifier : " + dataSegment.getIdentifier());
      }
    }

    Assert.assertEquals(descriptor1, getDescriptorPath(updatedSegment1));
    Assert.assertEquals(descriptor2, getDescriptorPath(updatedSegment2));
    Assert.assertEquals(descriptor3, getDescriptorPath(updatedSegment3));
    Assert.assertEquals(descriptor4_0, getDescriptorPath(updatedSegment4_0));
    Assert.assertEquals(descriptor4_1, getDescriptorPath(updatedSegment4_1));

    final String serializedSegment1 = mapper.writeValueAsString(updatedSegment1);
    final String serializedSegment2 = mapper.writeValueAsString(updatedSegment2);
    final String serializedSegment3 = mapper.writeValueAsString(updatedSegment3);
    final String serializedSegment4_0 = mapper.writeValueAsString(updatedSegment4_0);
    final String serializedSegment4_1 = mapper.writeValueAsString(updatedSegment4_1);

    Assert.assertNotEquals(serializedSegment1,
                           IOUtils.toString(mockS3Client.getObject(bucket, descriptor1).getDataInputStream()));
    Assert.assertNotEquals(serializedSegment2,
                           IOUtils.toString(mockS3Client.getObject(bucket, descriptor2).getDataInputStream()));
    Assert.assertNotEquals(serializedSegment3,
                           IOUtils.toString(mockS3Client.getObject(bucket, descriptor3).getDataInputStream()));
    Assert.assertNotEquals(serializedSegment4_0,
                           IOUtils.toString(mockS3Client.getObject(bucket, descriptor4_0).getDataInputStream()));
    Assert.assertNotEquals(serializedSegment4_1,
                           IOUtils.toString(mockS3Client.getObject(bucket, descriptor4_1).getDataInputStream()));

    final Set<DataSegment> segments2 = s3DataSegmentFinder.findSegments("", true);

    Assert.assertEquals(segments, segments2);

    Assert.assertEquals(serializedSegment1,
                           IOUtils.toString(mockS3Client.getObject(bucket, descriptor1).getDataInputStream()));
    Assert.assertEquals(serializedSegment2,
                           IOUtils.toString(mockS3Client.getObject(bucket, descriptor2).getDataInputStream()));
    Assert.assertEquals(serializedSegment3,
                           IOUtils.toString(mockS3Client.getObject(bucket, descriptor3).getDataInputStream()));
    Assert.assertEquals(serializedSegment4_0,
                           IOUtils.toString(mockS3Client.getObject(bucket, descriptor4_0).getDataInputStream()));
    Assert.assertEquals(serializedSegment4_1,
                           IOUtils.toString(mockS3Client.getObject(bucket, descriptor4_1).getDataInputStream()));
  }

  @Test(expected = SegmentLoadingException.class)
  public void testFindSegmentsFail() throws SegmentLoadingException, ServiceException
  {
    mockS3Client.deleteObject(bucket, indexZip4_1);

    final S3DataSegmentFinder s3DataSegmentFinder = new S3DataSegmentFinder(mockS3Client, config, mapper);
    s3DataSegmentFinder.findSegments("", false);
  }

  @Test(expected = SegmentLoadingException.class)
  public void testFindSegmentsFail2() throws SegmentLoadingException
  {
    final S3DataSegmentFinder s3DataSegmentFinder = new S3DataSegmentFinder(
        mockS3Client, config, new DefaultObjectMapper());

    try {
      s3DataSegmentFinder.findSegments("", false);
    }
    catch (SegmentLoadingException e) {
      Assert.assertTrue(e.getCause() instanceof IOException);
      throw e;
    }
  }

  @Test
  public void testFindSegmentsWithmaxListingLength() throws SegmentLoadingException
  {
    config.setMaxListingLength(3);
    final S3DataSegmentFinder s3DataSegmentFinder = new S3DataSegmentFinder(mockS3Client, config, mapper);
    final Set<DataSegment> segments = s3DataSegmentFinder.findSegments("", false);
    Assert.assertEquals(5, segments.size());
  }

  @Test
  public void testFindSegmentsWithworkingDirPath() throws SegmentLoadingException
  {
    config.setBaseKey("");
    final S3DataSegmentFinder s3DataSegmentFinder = new S3DataSegmentFinder(mockS3Client, config, mapper);
    final Set<DataSegment> segments = s3DataSegmentFinder.findSegments(baseKey, false);
    Assert.assertEquals(5, segments.size());
  }

  @Test
  public void testFindSegmentsUpdateLoadSpec() throws Exception
  {
    config.setBucket("amazing");
    final DataSegment segmentMissingLoadSpec = DataSegment.builder(SEGMENT_1)
        .loadSpec(ImmutableMap.of())
        .build();
    final S3DataSegmentFinder s3DataSegmentFinder = new S3DataSegmentFinder(mockS3Client, config, mapper);
    final String segmentPath = baseKey + "/interval_missing_load_spec/v1/1/";
    final String descriptorPath = S3Utils.descriptorPathForSegmentPath(segmentPath);
    final String indexPath = S3Utils.indexZipForSegmentPath(segmentPath);

    mockS3Client.putObject(
        config.getBucket(),
        new S3Object(
            descriptorPath,
            mapper.writeValueAsString(segmentMissingLoadSpec)
        )
    );

    mockS3Client.putObject(
        config.getBucket(),
        new S3Object(
            indexPath,
            "dummy"
        )
    );

    Set<DataSegment> segments = s3DataSegmentFinder.findSegments(segmentPath, false);
    Assert.assertEquals(1, segments.size());

    // Guaranteed there's only 1 element due to prior assert
    DataSegment testSegment = segments.iterator().next();
    Map<String, Object> testLoadSpec = testSegment.getLoadSpec();

    Assert.assertEquals("amazing", testLoadSpec.get("bucket"));
    Assert.assertEquals("s3_zip", testLoadSpec.get("type"));
    Assert.assertEquals(indexPath, testLoadSpec.get("key"));
  }

  private String getDescriptorPath(DataSegment segment)
  {
    return S3Utils.descriptorPathForSegmentPath(String.valueOf(segment.getLoadSpec().get("key")));
  }

  private static class MockStorageService extends RestS3Service
  {
    private final File baseDir;
    private final Map<String, Set<String>> storage = Maps.newHashMap();

    public MockStorageService(File baseDir)
    {
      super(null);
      this.baseDir = baseDir;
    }

    @Override
    public StorageObjectsChunk listObjectsChunked(
        final String bucketName, final String prefix, final String delimiter,
        final long maxListingLength, final String priorLastKey
    ) throws ServiceException
    {
      List<String> keysOrigin = Lists.newArrayList(storage.get(bucketName));

      Predicate<String> prefixFilter = new Predicate<String>()
      {
        @Override
        public boolean apply(@Nullable String input)
        {
          return input.startsWith(prefix);
        }
      };

      ImmutableList<String> keys = ImmutableList.copyOf(
        Ordering.natural().sortedCopy(Iterables.filter(keysOrigin, prefixFilter)
        )
      );

      int startOffset = 0;
      if (priorLastKey != null) {
        startOffset = keys.indexOf(priorLastKey) + 1;
      }

      int endOffset = startOffset + (int)maxListingLength; // exclusive
      if (endOffset > keys.size()) {
        endOffset = keys.size();
      }

      String newPriorLastkey = keys.get(endOffset - 1);
      if (endOffset == (keys.size())) {
        newPriorLastkey = null;
      }

      List<StorageObject> objects = Lists.newArrayList();
      for(String objectKey : keys.subList(startOffset, endOffset)) {
        objects.add(getObjectDetails(bucketName, objectKey));
      }

      return new StorageObjectsChunk(
          prefix, delimiter, objects.toArray(new StorageObject[]{}), null, newPriorLastkey);
    }

    @Override
    public StorageObject getObjectDetails(String bucketName, String objectKey) throws ServiceException
    {

      if (!storage.containsKey(bucketName)) {
        ServiceException ex = new ServiceException();
        ex.setResponseCode(404);
        ex.setErrorCode("NoSuchBucket");
        throw ex;
      }

      if (!storage.get(bucketName).contains(objectKey)) {
        ServiceException ex = new ServiceException();
        ex.setResponseCode(404);
        ex.setErrorCode("NoSuchKey");
        throw ex;
      }

      final File objectPath = new File(baseDir, objectKey);
      StorageObject storageObject = new StorageObject();
      storageObject.setBucketName(bucketName);
      storageObject.setKey(objectKey);
      storageObject.setDataInputFile(objectPath);

      return storageObject;
    }

    @Override
    public S3Object getObject(String bucketName, String objectKey) throws S3ServiceException
    {
      final File objectPath = new File(baseDir, objectKey);
      S3Object s3Object = new S3Object();
      s3Object.setBucketName(bucketName);
      s3Object.setKey(objectKey);
      s3Object.setDataInputFile(objectPath);

      return s3Object;

    }

    @Override
    public S3Object putObject(final String bucketName, final S3Object object) throws S3ServiceException
    {
      if (!storage.containsKey(bucketName)) {
        storage.put(bucketName, Sets.<String>newHashSet());
      }
      storage.get(bucketName).add(object.getKey());

      final File objectPath = new File(baseDir, object.getKey());

      if (!objectPath.getParentFile().exists()) {
        objectPath.getParentFile().mkdirs();
      }

      try {
        try (
            InputStream in = object.getDataInputStream()
        ) {
          FileUtils.copyInputStreamToFile(in, objectPath);
        }
      }
      catch (Exception e) {
        throw Throwables.propagate(e);
      }

      return object;
    }

    @Override
    public void deleteObject(String bucketName, String objectKey) throws ServiceException
    {
      storage.get(bucketName).remove(objectKey);
      final File objectPath = new File(baseDir, objectKey);
      objectPath.delete();
    }
  }
}
