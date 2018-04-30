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

package io.druid.storage.google;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.google.api.client.http.InputStreamContent;
import com.google.api.services.storage.Storage;
import com.google.api.services.storage.model.Objects;
import com.google.api.services.storage.model.StorageObject;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.druid.java.util.common.Intervals;
import io.druid.segment.TestHelper;
import io.druid.segment.loading.SegmentLoadingException;
import io.druid.timeline.DataSegment;
import io.druid.timeline.partition.NumberedShardSpec;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.easymock.EasyMockSupport;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;

public class GoogleDataSegmentFinderTest extends EasyMockSupport
{
  private static final String SEGMENT_1_ZIP = "prefix/test/index.zip";
  private static final String SEGMENT_1_DESCRIPTOR = "prefix/test/descriptor.json";
  private static final String TEST_BUCKET = "testbucket";

  private static final DataSegment SEGMENT_1 = DataSegment
      .builder()
      .dataSource("wikipedia")
      .interval(Intervals.of("2013-08-31T00:00:00.000Z/2013-09-01T00:00:00.000Z"))
      .version("2015-10-21T22:07:57.074Z")
      .loadSpec(
          ImmutableMap.<String, Object>of(
              "type",
              GoogleStorageDruidModule.SCHEME,
              "bucket",
              TEST_BUCKET,
              "path",
              SEGMENT_1_ZIP
          )
      )
      .dimensions(ImmutableList.of("language", "page"))
      .metrics(ImmutableList.of("count"))
      .build();

  private static final ObjectMapper mapper = TestHelper.makeJsonMapper();

  @BeforeClass
  public static void setUpStatic()
  {
    mapper.registerSubtypes(new NamedType(NumberedShardSpec.class, "numbered"));
  }

  @Test
  public void testFindSegments() throws Exception
  {
    GoogleStorage storage = createMock(GoogleStorage.class);

    GoogleAccountConfig config = new GoogleAccountConfig();
    config.setBucket(TEST_BUCKET);

    final GoogleDataSegmentFinder googleDataSegmentFinder = new GoogleDataSegmentFinder(storage, config, mapper);

    StorageObject item1 = new StorageObject();
    item1.setName(SEGMENT_1_DESCRIPTOR);
    item1.setBucket(TEST_BUCKET);

    Storage.Objects.List segmentList = createMock(Storage.Objects.List.class);
    List<StorageObject> items = new ArrayList<StorageObject>();

    items.add(item1);

    Objects objects = new Objects();
    objects.setItems(items);
    objects.setNextPageToken(null);

    InputStream stream1 = new ByteArrayInputStream(mapper.writeValueAsBytes(SEGMENT_1));

    expect(storage.list(TEST_BUCKET)).andReturn(segmentList);
    expect(segmentList.setPrefix("prefix")).andReturn(segmentList);
    expect(segmentList.execute()).andReturn(objects);
    expect(storage.exists(TEST_BUCKET, SEGMENT_1_ZIP)).andReturn(true);
    expect(storage.get(TEST_BUCKET, SEGMENT_1_DESCRIPTOR)).andReturn(stream1);

    replayAll();

    final Set<DataSegment> segments = googleDataSegmentFinder.findSegments("prefix", false);

    Assert.assertEquals(1, segments.size());

    verifyAll();
  }

  @Test(expected = SegmentLoadingException.class)
  public void testFindSegmentsFail() throws Exception
  {
    GoogleStorage storage = createMock(GoogleStorage.class);

    GoogleAccountConfig config = new GoogleAccountConfig();
    config.setBucket(TEST_BUCKET);

    final GoogleDataSegmentFinder googleDataSegmentFinder = new GoogleDataSegmentFinder(storage, config, mapper);

    StorageObject item1 = new StorageObject();
    item1.setName(SEGMENT_1_DESCRIPTOR);
    item1.setBucket(TEST_BUCKET);

    Storage.Objects.List segmentList = createMock(Storage.Objects.List.class);
    List<StorageObject> items = new ArrayList<StorageObject>();

    items.add(item1);

    Objects objects = new Objects();
    objects.setItems(items);
    objects.setNextPageToken(null);

    expect(storage.list(TEST_BUCKET)).andReturn(segmentList);
    expect(segmentList.setPrefix("")).andReturn(segmentList);
    expect(segmentList.execute()).andReturn(objects);
    expect(storage.exists(TEST_BUCKET, SEGMENT_1_ZIP)).andReturn(false);

    replayAll();

    googleDataSegmentFinder.findSegments("", false);

    verifyAll();
  }

  @Test
  public void testFindSegmentsUpdateLoadSpec() throws Exception
  {
    GoogleStorage storage = createMock(GoogleStorage.class);

    GoogleAccountConfig config = new GoogleAccountConfig();
    config.setBucket(TEST_BUCKET);

    final GoogleDataSegmentFinder googleDataSegmentFinder = new GoogleDataSegmentFinder(storage, config, mapper);

    StorageObject item1 = new StorageObject();
    item1.setName(SEGMENT_1_DESCRIPTOR);
    item1.setBucket(TEST_BUCKET);

    Storage.Objects.List segmentList = createMock(Storage.Objects.List.class);
    List<StorageObject> items = new ArrayList<StorageObject>();

    items.add(item1);

    Objects objects = new Objects();
    objects.setItems(items);
    objects.setNextPageToken(null);

    final DataSegment segmentMissingLoadSpec = DataSegment
        .builder(SEGMENT_1)
        .loadSpec(ImmutableMap.of())
        .build();

    InputStream stream1 = new ByteArrayInputStream(mapper.writeValueAsBytes(segmentMissingLoadSpec));

    expect(storage.list(TEST_BUCKET)).andReturn(segmentList);
    expect(segmentList.setPrefix("")).andReturn(segmentList);
    expect(segmentList.execute()).andReturn(objects);
    expect(storage.exists(TEST_BUCKET, SEGMENT_1_ZIP)).andReturn(true);
    expect(storage.get(TEST_BUCKET, SEGMENT_1_DESCRIPTOR)).andReturn(stream1);

    Capture<InputStreamContent> capture1 = Capture.newInstance();
    storage.insert(EasyMock.eq(TEST_BUCKET), EasyMock.eq(SEGMENT_1_DESCRIPTOR), EasyMock.capture(capture1));
    expectLastCall();

    replayAll();

    final Set<DataSegment> segments = googleDataSegmentFinder.findSegments("", true);

    Assert.assertEquals(1, segments.size());

    // Guaranteed there's only 1 element due to prior assert
    DataSegment testSegment = segments.iterator().next();
    Map<String, Object> testLoadSpec = testSegment.getLoadSpec();

    Assert.assertEquals(GoogleStorageDruidModule.SCHEME, testLoadSpec.get("type"));
    Assert.assertEquals(TEST_BUCKET, testLoadSpec.get("bucket"));
    Assert.assertEquals(SEGMENT_1_ZIP, testLoadSpec.get("path"));

    testSegment = mapper.readValue(capture1.getValue().getInputStream(), DataSegment.class);
    testLoadSpec = testSegment.getLoadSpec();

    Assert.assertEquals(GoogleStorageDruidModule.SCHEME, testLoadSpec.get("type"));
    Assert.assertEquals(TEST_BUCKET, testLoadSpec.get("bucket"));
    Assert.assertEquals(SEGMENT_1_ZIP, testLoadSpec.get("path"));

    verifyAll();
  }
}
