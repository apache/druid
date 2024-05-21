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

package org.apache.druid.server;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.SegmentLazyLoadFailCallback;
import org.apache.druid.segment.loading.LoadSpec;
import org.apache.druid.segment.loading.SegmentLoadingException;
import org.apache.druid.segment.loading.SegmentizerFactory;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.NoneShardSpec;
import org.joda.time.Interval;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;


public class TestSegmentUtils
{
  @JsonTypeName("test")
  public static class TestLoadSpec implements LoadSpec
  {

    private final int size;
    private final String name;

    @JsonCreator
    public TestLoadSpec(
        @JsonProperty("size") int size,
        @JsonProperty("name") String name
    )
    {
      this.size = size;
      this.name = name;
    }

    @Override
    public LoadSpecResult loadSegment(File destDir) throws SegmentLoadingException
    {
      File segmentFile = new File(destDir, "segment");
      File factoryJson = new File(destDir, "factory.json");
      try {
        FileUtils.mkdirp(destDir);
        segmentFile.createNewFile();
        factoryJson.createNewFile();
      }
      catch (IOException e) {
        throw new SegmentLoadingException(
            e,
            "Failed to create files under dir[%s]",
            destDir.getAbsolutePath()
        );
      }

      try {
        byte[] bytes = new byte[size];
        ThreadLocalRandom.current().nextBytes(bytes);
        Files.write(bytes, segmentFile);
        Files.write("{\"type\":\"testSegmentFactory\"}".getBytes(StandardCharsets.UTF_8), factoryJson);
      }
      catch (IOException e) {
        throw new SegmentLoadingException(
            e,
            "Failed to write data in directory[%s]",
            destDir.getAbsolutePath()
        );
      }
      return new LoadSpecResult(size);
    }
  }

  @JsonTypeName("testSegmentFactory")
  public static class TestSegmentizerFactory implements SegmentizerFactory
  {
    @Override
    public Segment factorize(
        DataSegment segment,
        File parentDir,
        boolean lazy,
        SegmentLazyLoadFailCallback loadFailed
    )
    {
      return Mockito.mock(Segment.class);
    }
  }

  public static DataSegment makeSegment(String dataSource, String version, Interval interval)
  {
    return new DataSegment(
        dataSource,
        interval,
        version,
        ImmutableMap.of("type", "test"),
        Arrays.asList("dim1", "dim2", "dim3"),
        Arrays.asList("metric1", "metric2"),
        NoneShardSpec.instance(),
        IndexIO.CURRENT_VERSION_ID,
        1L
    );
  }

  public static DataSegment makeSegment(String dataSource, String version, long size)
  {
    return new DataSegment(
        dataSource,
        Intervals.ETERNITY,
        version,
        ImmutableMap.of("type", "test"),
        Arrays.asList("dim1", "dim2", "dim3"),
        Arrays.asList("metric1", "metric2"),
        NoneShardSpec.instance(),
        IndexIO.CURRENT_VERSION_ID,
        size
    );
  }
}
