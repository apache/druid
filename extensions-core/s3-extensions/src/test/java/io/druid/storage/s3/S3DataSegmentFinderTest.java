/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Metamarkets licenses this file
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

package io.druid.storage.s3;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.segment.loading.SegmentLoadingException;
import io.druid.timeline.DataSegment;
import io.druid.timeline.partition.NumberedShardSpec;
import org.jets3t.service.S3ServiceException;
import org.jets3t.service.ServiceException;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;
import org.jets3t.service.model.S3Bucket;
import org.jets3t.service.model.S3Object;
import org.jets3t.service.utils.ServiceUtils;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class S3DataSegmentFinderTest
{
  private static final String DESCRIPTOR_JSON = "descriptor.json";
  private static final String INDEX_ZIP = "index.zip";
  private static final String DUMMY_PATH = "1234/somewhere/";

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
                                                                  "abc.com",
                                                                  "key",
                                                                  DUMMY_PATH + INDEX_ZIP
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

  private static final Map<String, DataSegment> segmentsByIdentifier = Maps.newHashMap();

  static {
    segmentsByIdentifier.put(SEGMENT_1.getIdentifier(), SEGMENT_1);
    segmentsByIdentifier.put(SEGMENT_2.getIdentifier(), SEGMENT_2);
    segmentsByIdentifier.put(SEGMENT_3.getIdentifier(), SEGMENT_3);
    segmentsByIdentifier.put(SEGMENT_4_0.getIdentifier(), SEGMENT_4_0);
    segmentsByIdentifier.put(SEGMENT_4_1.getIdentifier(), SEGMENT_4_1);
  }

  @BeforeClass
  public static void setupStatic()
  {
    mapper.registerSubtypes(new NamedType(NumberedShardSpec.class, "numbered"));
  }

  @Test
  public void testFindSegments() throws Exception
  {
    final S3DataSegmentFinder s3DataSegmentFinder = new S3DataSegmentFinder(getS3Mock(true), mapper);
    final Set<DataSegment> actualSegments = s3DataSegmentFinder.findSegments("s3://abc.com/", false);

    Assert.assertEquals(5, actualSegments.size());

    for (DataSegment actualSegment : actualSegments) {
      String actualIdentifier = String.valueOf(actualSegment.getIdentifier());
      Assert.assertTrue(segmentsByIdentifier.containsKey(actualIdentifier));
    }

    // since updateDescriptor was not enabled, descriptor.json still has stale information
    Assert.assertEquals(actualSegments, Sets.newHashSet(segmentsByIdentifier.values()));
  }

  @Test
  public void testFindSegmentsWithDescriptorUpdate() throws Exception
  {
    final Set<DataSegment> updatedSegments =
        new S3DataSegmentFinder(getS3Mock(true), mapper).findSegments("s3://abc.com/", true);
    Assert.assertEquals(5, updatedSegments.size());
    final Map<String, String> updatedKeysByIdentifier = Maps.newHashMap();
    for (DataSegment updatedSegment : updatedSegments) {
      String identifier = String.valueOf(updatedSegment.getIdentifier());
      Assert.assertTrue(segmentsByIdentifier.containsKey(identifier));
      String key = String.valueOf(updatedSegment.getLoadSpec().get("key"));
      updatedKeysByIdentifier.put(identifier, key);
    }
    // enable updateDescriptor so that descriptors.json will be updated to relfect the new loadSpec
    final Set<DataSegment> segments =
        new S3DataSegmentFinder(getS3Mock(true), mapper).findSegments("s3://abc.com/", false);
    final Map<String, String> keysByIdentifier = Maps.newHashMap();
    for (DataSegment segment : segments) {
      String identifier = String.valueOf(segment.getIdentifier());
      Assert.assertTrue(segmentsByIdentifier.containsKey(identifier));
      String key = String.valueOf(segment.getLoadSpec().get("key"));
      keysByIdentifier.put(identifier, key);
    }
    for (String identifier : keysByIdentifier.keySet()) {
      String key = keysByIdentifier.get(identifier);
      String updatedKey = updatedKeysByIdentifier.get(identifier);
      Assert.assertNotEquals(key, updatedKey);
    }
  }

  @Test(expected = SegmentLoadingException.class)
  public void testFindSegmentsFail() throws Exception
  {
    // don't include index.zip files while keeping its descriptor.json
    new S3DataSegmentFinder(getS3Mock(false), mapper).findSegments("s3://abc.com/", false);
  }

  @Test(expected = SegmentLoadingException.class)
  public void testFindSegmentsFail2() throws Exception
  {
    // will fail to desierialize descriptor.json because DefaultObjectMapper doesn't recognize NumberedShardSpec
    try {
      new S3DataSegmentFinder(getS3Mock(true), new DefaultObjectMapper()).findSegments("s3://abc.com/", false);
    }
    catch (SegmentLoadingException e) {
      Assert.assertTrue(e.getCause() instanceof IOException);
      throw e;
    }
  }

  private RestS3Service getS3Mock(Boolean includeIndexZips) throws Exception
  {
    RestS3Service s3Mock = new MockStorageService();
    for (String identifier : segmentsByIdentifier.keySet()) {
      String segmentContent = mapper.writeValueAsString(segmentsByIdentifier.get(identifier));
      S3Object descriptorObj = new S3Object(
          new S3Bucket("abc.com"),
          DUMMY_PATH + identifier + "/" + DESCRIPTOR_JSON,
          segmentContent
      );
      S3Object indexObj = new S3Object(new S3Bucket("abc.com"), DUMMY_PATH + identifier + "/" + INDEX_ZIP, "");
      s3Mock.putObject("abc.com", descriptorObj);
      if (includeIndexZips) {
        s3Mock.putObject("abc.com", indexObj);
      }
    }
    return s3Mock;
  }

  private class MockStorageService extends RestS3Service
  {
    final Map<String, Map<String, String>> storage = Maps.newHashMap();

    private MockStorageService()
    {
      super(null);
    }

    @Override
    public boolean isObjectInBucket(String bucketName, String objectKey) throws ServiceException
    {
      final Map<String, String> objects = storage.get(bucketName);
      return (objects != null && objects.containsKey(objectKey));
    }

    @Override
    public S3Object[] listObjects(String bucketName, String objectKey, String separator)
    {
      final List<S3Object> result = new ArrayList<>();
      final Map<String, String> objects = storage.get(bucketName);
      for (String key : objects.keySet()) {
        try {
          result.add(new S3Object(new S3Bucket(bucketName), key, objects.get(key)));
        }
        catch (Exception e) {
          throw new RuntimeException("Unexpected error !!!", e);
        }
      }
      return result.toArray(new S3Object[result.size()]);
    }

    @Override
    public S3Object putObject(String bucketName, S3Object object) throws S3ServiceException
    {
      if (!storage.containsKey(bucketName)) {
        storage.put(bucketName, Maps.<String, String>newHashMap());
      }
      try {
        String data = ServiceUtils.readInputStreamToString(object.getDataInputStream(), "UTF-8");
        storage.get(bucketName).put(object.getKey(), data);
        return object;
      }
      catch (Exception e) {
        throw new RuntimeException("Unexpected error !!!", e);
      }
    }
  }

}
