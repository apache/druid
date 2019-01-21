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

package org.apache.druid.segment.loading;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.io.FileUtils;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.NumberedShardSpec;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Set;

/**
 */
public class LocalDataSegmentFinderTest
{

  private static final ObjectMapper mapper = TestHelper.makeJsonMapper();
  private static final String DESCRIPTOR_JSON = "descriptor.json";
  private static final String INDEX_ZIP = "index.zip";
  private static final DataSegment SEGMENT_1 = DataSegment
      .builder()
      .dataSource("wikipedia")
      .interval(Intervals.of("2013-08-31T00:00:00.000Z/2013-09-01T00:00:00.000Z"))
      .version("2015-10-21T22:07:57.074Z")
      .loadSpec(
          ImmutableMap.of(
              "type",
              "local",
              "path",
              "/tmp/somewhere/index.zip"
          )
      )
      .dimensions(ImmutableList.of("language", "page"))
      .metrics(ImmutableList.of("count"))
      .build();

  private static final DataSegment SEGMENT_2 = DataSegment
      .builder(SEGMENT_1)
      .interval(Intervals.of("2013-09-01T00:00:00.000Z/2013-09-02T00:00:00.000Z"))
      .build();

  private static final DataSegment SEGMENT_3 = DataSegment
      .builder(SEGMENT_1)
      .interval(Intervals.of("2013-09-02T00:00:00.000Z/2013-09-03T00:00:00.000Z"))
      .version("2015-10-22T22:07:57.074Z")
      .build();

  private static final DataSegment SEGMENT_4_0 = DataSegment
      .builder(SEGMENT_1)
      .interval(Intervals.of("2013-09-02T00:00:00.000Z/2013-09-03T00:00:00.000Z"))
      .shardSpec(new NumberedShardSpec(0, 2))
      .build();

  private static final DataSegment SEGMENT_4_1 = DataSegment
      .builder(SEGMENT_1)
      .interval(Intervals.of("2013-09-02T00:00:00.000Z/2013-09-03T00:00:00.000Z"))
      .shardSpec(new NumberedShardSpec(1, 2))
      .build();

  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  private File dataSourceDir;
  private File descriptor1;
  private File descriptor2;
  private File descriptor3;
  private File descriptor4_0;
  private File descriptor4_1;
  private File indexZip1;
  private File indexZip2;
  private File indexZip3;
  private File indexZip4_0;
  private File indexZip4_1;

  @BeforeClass
  public static void setUpStatic()
  {
    mapper.registerSubtypes(new NamedType(NumberedShardSpec.class, "numbered"));
  }

  @Before
  public void setUp() throws Exception
  {

    dataSourceDir = temporaryFolder.newFolder();
    descriptor1 = new File(dataSourceDir.getAbsolutePath() + "/interval1/v1/0", DESCRIPTOR_JSON);
    descriptor2 = new File(dataSourceDir.getAbsolutePath() + "/interval2/v1/0", DESCRIPTOR_JSON);
    descriptor3 = new File(dataSourceDir.getAbsolutePath() + "/interval3/v2/0", DESCRIPTOR_JSON);
    descriptor4_0 = new File(dataSourceDir.getAbsolutePath() + "/interval4/v1/0", DESCRIPTOR_JSON);
    descriptor4_1 = new File(dataSourceDir.getAbsolutePath() + "/interval4/v1/1", DESCRIPTOR_JSON);

    descriptor1.getParentFile().mkdirs();
    descriptor2.getParentFile().mkdirs();
    descriptor3.getParentFile().mkdirs();
    descriptor4_0.getParentFile().mkdirs();
    descriptor4_1.getParentFile().mkdirs();

    mapper.writeValue(descriptor1, SEGMENT_1);
    mapper.writeValue(descriptor2, SEGMENT_2);
    mapper.writeValue(descriptor3, SEGMENT_3);
    mapper.writeValue(descriptor4_0, SEGMENT_4_0);
    mapper.writeValue(descriptor4_1, SEGMENT_4_1);

    indexZip1 = new File(descriptor1.getParentFile(), INDEX_ZIP);
    indexZip2 = new File(descriptor2.getParentFile(), INDEX_ZIP);
    indexZip3 = new File(descriptor3.getParentFile(), INDEX_ZIP);
    indexZip4_0 = new File(descriptor4_0.getParentFile(), INDEX_ZIP);
    indexZip4_1 = new File(descriptor4_1.getParentFile(), INDEX_ZIP);

    indexZip1.createNewFile();
    indexZip2.createNewFile();
    indexZip3.createNewFile();
    indexZip4_0.createNewFile();
    indexZip4_1.createNewFile();
  }

  @Test
  public void testFindSegments() throws SegmentLoadingException, IOException
  {
    final LocalDataSegmentFinder localDataSegmentFinder = new LocalDataSegmentFinder(mapper);

    final Set<DataSegment> segments = localDataSegmentFinder.findSegments(dataSourceDir.getAbsolutePath(), false);

    Assert.assertEquals(5, segments.size());

    DataSegment updatedSegment1 = null;
    DataSegment updatedSegment2 = null;
    DataSegment updatedSegment3 = null;
    DataSegment updatedSegment4_0 = null;
    DataSegment updatedSegment4_1 = null;
    for (DataSegment dataSegment : segments) {
      if (dataSegment.getId().equals(SEGMENT_1.getId())) {
        updatedSegment1 = dataSegment;
      } else if (dataSegment.getId().equals(SEGMENT_2.getId())) {
        updatedSegment2 = dataSegment;
      } else if (dataSegment.getId().equals(SEGMENT_3.getId())) {
        updatedSegment3 = dataSegment;
      } else if (dataSegment.getId().equals(SEGMENT_4_0.getId())) {
        updatedSegment4_0 = dataSegment;
      } else if (dataSegment.getId().equals(SEGMENT_4_1.getId())) {
        updatedSegment4_1 = dataSegment;
      } else {
        Assert.fail("Unexpected segment");
      }
    }

    Assert.assertEquals(descriptor1.getAbsolutePath(), getDescriptorPath(updatedSegment1));
    Assert.assertEquals(descriptor2.getAbsolutePath(), getDescriptorPath(updatedSegment2));
    Assert.assertEquals(descriptor3.getAbsolutePath(), getDescriptorPath(updatedSegment3));
    Assert.assertEquals(descriptor4_0.getAbsolutePath(), getDescriptorPath(updatedSegment4_0));
    Assert.assertEquals(descriptor4_1.getAbsolutePath(), getDescriptorPath(updatedSegment4_1));

    final String serializedSegment1 = mapper.writeValueAsString(updatedSegment1);
    final String serializedSegment2 = mapper.writeValueAsString(updatedSegment2);
    final String serializedSegment3 = mapper.writeValueAsString(updatedSegment3);
    final String serializedSegment4_0 = mapper.writeValueAsString(updatedSegment4_0);
    final String serializedSegment4_1 = mapper.writeValueAsString(updatedSegment4_1);

    // since updateDescriptor was not enabled, descriptor.json still has stale information
    Assert.assertNotEquals(serializedSegment1, FileUtils.readFileToString(descriptor1));
    Assert.assertNotEquals(serializedSegment2, FileUtils.readFileToString(descriptor2));
    Assert.assertNotEquals(serializedSegment3, FileUtils.readFileToString(descriptor3));
    Assert.assertNotEquals(serializedSegment4_0, FileUtils.readFileToString(descriptor4_0));
    Assert.assertNotEquals(serializedSegment4_1, FileUtils.readFileToString(descriptor4_1));

    // enable updateDescriptor so that descriptors.json will be updated to relfect the new loadSpec
    final Set<DataSegment> segments2 = localDataSegmentFinder.findSegments(dataSourceDir.getAbsolutePath(), true);

    Assert.assertEquals(segments, segments2);
    Assert.assertEquals(serializedSegment1, FileUtils.readFileToString(descriptor1));
    Assert.assertEquals(serializedSegment2, FileUtils.readFileToString(descriptor2));
    Assert.assertEquals(serializedSegment3, FileUtils.readFileToString(descriptor3));
    Assert.assertEquals(serializedSegment4_0, FileUtils.readFileToString(descriptor4_0));
    Assert.assertEquals(serializedSegment4_1, FileUtils.readFileToString(descriptor4_1));
  }

  @Test(expected = SegmentLoadingException.class)
  public void testFindSegmentsFail() throws SegmentLoadingException
  {
    // remove one of index.zip while keeping its descriptor.json
    indexZip4_1.delete();

    final LocalDataSegmentFinder localDataSegmentFinder = new LocalDataSegmentFinder(mapper);
    localDataSegmentFinder.findSegments(dataSourceDir.getAbsolutePath(), false);
  }

  @Test
  public void testPreferNewestSegment() throws Exception
  {
    dataSourceDir = temporaryFolder.newFolder();
    descriptor1 = new File(dataSourceDir.getAbsolutePath() + "/interval10/v10/0/older", DESCRIPTOR_JSON);
    descriptor2 = new File(dataSourceDir.getAbsolutePath() + "/interval10/v10/0/newer", DESCRIPTOR_JSON);

    descriptor1.getParentFile().mkdirs();
    descriptor2.getParentFile().mkdirs();

    mapper.writeValue(descriptor1, SEGMENT_1);
    mapper.writeValue(descriptor2, SEGMENT_1);

    indexZip1 = new File(descriptor1.getParentFile(), INDEX_ZIP);
    indexZip2 = new File(descriptor2.getParentFile(), INDEX_ZIP);

    FileOutputStream fos1 = new FileOutputStream(indexZip1);
    fos1.getFD().sync();
    fos1.close();

    Thread.sleep(1000);

    FileOutputStream fos2 = new FileOutputStream(indexZip2);
    fos2.getFD().sync();
    fos2.close();

    final Set<DataSegment> segments = new LocalDataSegmentFinder(mapper).findSegments(
        dataSourceDir.getAbsolutePath(), false
    );

    Assert.assertEquals(1, segments.size());
    Assert.assertEquals(indexZip2.getAbsolutePath(), segments.iterator().next().getLoadSpec().get("path"));
  }

  private String getDescriptorPath(DataSegment segment)
  {
    final File indexzip = new File(String.valueOf(segment.getLoadSpec().get("path")));
    return indexzip.getParent() + "/" + DESCRIPTOR_JSON;
  }
}
