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

package io.druid.segment.loading;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.druid.java.util.common.IOE;
import io.druid.java.util.common.Intervals;
import io.druid.segment.TestHelper;
import io.druid.storage.hdfs.HdfsDataSegmentFinder;
import io.druid.timeline.DataSegment;
import io.druid.timeline.partition.NumberedShardSpec;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.Set;

/**
 */
public class HdfsDataSegmentFinderTest
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
          ImmutableMap.<String, Object>of(
              "type",
              "hdfs",
              "path",
              "hdfs://abc.com:1234/somewhere/index.zip"
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

  private static final DataSegment SEGMENT_5 = DataSegment
      .builder()
      .dataSource("wikipedia")
      .interval(Intervals.of("2013-09-03T00:00:00.000Z/2013-09-04T00:00:00.000Z"))
      .version("2015-10-21T22:07:57.074Z")
      .loadSpec(
          ImmutableMap.<String, Object>of(
              "type",
              "hdfs",
              "path",
              "hdfs://abc.com:1234/somewhere/1_index.zip"
          )
      )
      .dimensions(ImmutableList.of("language", "page"))
      .metrics(ImmutableList.of("count"))
      .build();

  private static MiniDFSCluster miniCluster;
  private static File hdfsTmpDir;
  private static URI uriBase;
  private static Configuration conf;
  private static FileSystem fs;

  private Path dataSourceDir;
  private Path descriptor1;
  private Path descriptor2;
  private Path descriptor3;
  private Path descriptor4_0;
  private Path descriptor4_1;
  private Path descriptor5;
  private Path indexZip1;
  private Path indexZip2;
  private Path indexZip3;
  private Path indexZip4_0;
  private Path indexZip4_1;
  private Path indexZip5;

  @BeforeClass
  public static void setupStatic() throws IOException
  {
    mapper.registerSubtypes(new NamedType(NumberedShardSpec.class, "numbered"));

    hdfsTmpDir = File.createTempFile("hdfsDataSource", "dir");
    if (!hdfsTmpDir.delete()) {
      throw new IOE("Unable to delete hdfsTmpDir [%s]", hdfsTmpDir.getAbsolutePath());
    }
    conf = new Configuration(true);
    conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, hdfsTmpDir.getAbsolutePath());
    miniCluster = new MiniDFSCluster.Builder(conf).build();
    uriBase = miniCluster.getURI();
    fs = miniCluster.getFileSystem();
  }

  @AfterClass
  public static void tearDownStatic() throws IOException
  {
    if (miniCluster != null) {
      miniCluster.shutdown(true);
    }
    FileUtils.deleteDirectory(hdfsTmpDir);
  }

  @Before
  public void setUp() throws IOException
  {
    dataSourceDir = new Path(new Path(uriBase), "/usr/dataSource");
    descriptor1 = new Path(dataSourceDir, "interval1/v1/0/" + DESCRIPTOR_JSON);
    descriptor2 = new Path(dataSourceDir, "interval2/v1/0/" + DESCRIPTOR_JSON);
    descriptor3 = new Path(dataSourceDir, "interval3/v2/0/" + DESCRIPTOR_JSON);
    descriptor4_0 = new Path(dataSourceDir, "interval4/v1/0/" + DESCRIPTOR_JSON);
    descriptor4_1 = new Path(dataSourceDir, "interval4/v1/1/" + DESCRIPTOR_JSON);
    descriptor5 = new Path(dataSourceDir, "interval5/v1/1/" + "1_" + DESCRIPTOR_JSON);
    indexZip1 = new Path(descriptor1.getParent(), INDEX_ZIP);
    indexZip2 = new Path(descriptor2.getParent(), INDEX_ZIP);
    indexZip3 = new Path(descriptor3.getParent(), INDEX_ZIP);
    indexZip4_0 = new Path(descriptor4_0.getParent(), INDEX_ZIP);
    indexZip4_1 = new Path(descriptor4_1.getParent(), INDEX_ZIP);
    indexZip5 = new Path(descriptor5.getParent(), "1_" + INDEX_ZIP);


    mapper.writeValue(fs.create(descriptor1), SEGMENT_1);
    mapper.writeValue(fs.create(descriptor2), SEGMENT_2);
    mapper.writeValue(fs.create(descriptor3), SEGMENT_3);
    mapper.writeValue(fs.create(descriptor4_0), SEGMENT_4_0);
    mapper.writeValue(fs.create(descriptor4_1), SEGMENT_4_1);
    mapper.writeValue(fs.create(descriptor5), SEGMENT_5);

    create(indexZip1);
    create(indexZip2);
    create(indexZip3);
    create(indexZip4_0);
    create(indexZip4_1);
    create(indexZip5);

  }

  private void create(Path indexZip1) throws IOException
  {
    try (FSDataOutputStream os = fs.create(indexZip1)) {
    }
  }

  @Test
  public void testFindSegments() throws Exception
  {
    final HdfsDataSegmentFinder hdfsDataSegmentFinder = new HdfsDataSegmentFinder(conf, mapper);

    final Set<DataSegment> segments = hdfsDataSegmentFinder.findSegments(dataSourceDir.toString(), false);

    Assert.assertEquals(6, segments.size());

    DataSegment updatedSegment1 = null;
    DataSegment updatedSegment2 = null;
    DataSegment updatedSegment3 = null;
    DataSegment updatedSegment4_0 = null;
    DataSegment updatedSegment4_1 = null;
    DataSegment updatedSegment5 = null;

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
      } else if (dataSegment.getIdentifier().equals(SEGMENT_5.getIdentifier())) {
        updatedSegment5 = dataSegment;
      } else {
        Assert.fail("Unexpected segment");
      }
    }

    Assert.assertEquals(descriptor1.toUri().getPath(), getDescriptorPath(updatedSegment1));
    Assert.assertEquals(descriptor2.toUri().getPath(), getDescriptorPath(updatedSegment2));
    Assert.assertEquals(descriptor3.toUri().getPath(), getDescriptorPath(updatedSegment3));
    Assert.assertEquals(descriptor4_0.toUri().getPath(), getDescriptorPath(updatedSegment4_0));
    Assert.assertEquals(descriptor4_1.toUri().getPath(), getDescriptorPath(updatedSegment4_1));
    Assert.assertEquals(descriptor5.toUri().getPath(), getDescriptorPathWithPartitionNum(updatedSegment5, 1));


    final String serializedSegment1 = mapper.writeValueAsString(updatedSegment1);
    final String serializedSegment2 = mapper.writeValueAsString(updatedSegment2);
    final String serializedSegment3 = mapper.writeValueAsString(updatedSegment3);
    final String serializedSegment4_0 = mapper.writeValueAsString(updatedSegment4_0);
    final String serializedSegment4_1 = mapper.writeValueAsString(updatedSegment4_1);
    final String serializedSegment5 = mapper.writeValueAsString(updatedSegment5);


    // since updateDescriptor was not enabled, descriptor.json still has stale information
    Assert.assertNotEquals(serializedSegment1, readContent(descriptor1));
    Assert.assertNotEquals(serializedSegment2, readContent(descriptor2));
    Assert.assertNotEquals(serializedSegment3, readContent(descriptor3));
    Assert.assertNotEquals(serializedSegment4_0, readContent(descriptor4_0));
    Assert.assertNotEquals(serializedSegment4_1, readContent(descriptor4_1));
    Assert.assertNotEquals(serializedSegment5, readContent(descriptor5));

    // enable updateDescriptor so that descriptors.json will be updated to relfect the new loadSpec
    final Set<DataSegment> segments2 = hdfsDataSegmentFinder.findSegments(dataSourceDir.toString(), true);

    Assert.assertEquals(segments, segments2);
    Assert.assertEquals(serializedSegment1, readContent(descriptor1));
    Assert.assertEquals(serializedSegment2, readContent(descriptor2));
    Assert.assertEquals(serializedSegment3, readContent(descriptor3));
    Assert.assertEquals(serializedSegment4_0, readContent(descriptor4_0));
    Assert.assertEquals(serializedSegment4_1, readContent(descriptor4_1));
    Assert.assertEquals(serializedSegment5, readContent(descriptor5));
  }

  @Test(expected = SegmentLoadingException.class)
  public void testFindSegmentsFail() throws Exception
  {
    // remove one of index.zip while keeping its descriptor.json
    fs.delete(indexZip4_1, false);

    final HdfsDataSegmentFinder hdfsDataSegmentFinder = new HdfsDataSegmentFinder(conf, mapper);
    hdfsDataSegmentFinder.findSegments(dataSourceDir.toString(), false);
  }

  private String getDescriptorPath(DataSegment segment)
  {
    final Path indexzip = new Path(String.valueOf(segment.getLoadSpec().get("path")));
    return indexzip.getParent().toString() + "/" + DESCRIPTOR_JSON;
  }

  private String getDescriptorPathWithPartitionNum(DataSegment segment, int partitionNum)
  {
    final Path indexzip = new Path(String.valueOf(segment.getLoadSpec().get("path")));
    return indexzip.getParent().toString() + "/" + partitionNum + "_" + DESCRIPTOR_JSON;
  }

  private String readContent(Path descriptor) throws IOException
  {
    final FSDataInputStream is = fs.open(descriptor);
    final String content = IOUtils.toString(is);
    is.close();
    return content;
  }
}
