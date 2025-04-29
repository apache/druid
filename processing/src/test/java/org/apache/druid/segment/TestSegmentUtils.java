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

package org.apache.druid.segment;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;
import org.apache.druid.collections.bitmap.BitmapFactory;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.query.OrderBy;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.data.Indexed;
import org.apache.druid.segment.loading.LoadSpec;
import org.apache.druid.segment.loading.SegmentLoadingException;
import org.apache.druid.segment.loading.SegmentizerFactory;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.partition.NoneShardSpec;
import org.apache.druid.timeline.partition.TombstoneShardSpec;
import org.joda.time.Interval;
import org.junit.Assert;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Test utility class for creating test segments and load specs.
 */
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
        Assert.assertTrue(segmentFile.createNewFile());
        Assert.assertTrue(factoryJson.createNewFile());
      }
      catch (IOException e) {
        throw new SegmentLoadingException(
            e,
            "Failed to create files in directory[%s]",
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
      return new SegmentForTesting(segment.getDataSource(), segment.getInterval(), segment.getVersion());
    }
  }

  public static class SegmentForTesting extends QueryableIndexSegment implements Segment
  {
    private final String datasource;
    private final String version;
    private final Interval interval;
    private final Object lock = new Object();
    private volatile boolean closed = false;
    private static final QueryableIndex INDEX = new QueryableIndex()
    {
      @Override
      public Interval getDataInterval()
      {
        throw new UnsupportedOperationException();
      }

      @Override
      public int getNumRows()
      {
        throw new UnsupportedOperationException();
      }

      @Override
      public Indexed<String> getAvailableDimensions()
      {
        throw new UnsupportedOperationException();
      }

      @Override
      public BitmapFactory getBitmapFactoryForDimensions()
      {
        throw new UnsupportedOperationException();
      }

      @Nullable
      @Override
      public Metadata getMetadata()
      {
        throw new UnsupportedOperationException();
      }

      @Override
      public Map<String, DimensionHandler> getDimensionHandlers()
      {
        throw new UnsupportedOperationException();
      }

      @Override
      public List<OrderBy> getOrdering()
      {
        return Cursors.ascendingTimeOrder();
      }

      @Override
      public void close()
      {

      }

      @Override
      public List<String> getColumnNames()
      {
        throw new UnsupportedOperationException();
      }

      @Nullable
      @Override
      public ColumnHolder getColumnHolder(String columnName)
      {
        throw new UnsupportedOperationException();
      }
    };

    public SegmentForTesting(String datasource, Interval interval, String version)
    {
      super(INDEX, SegmentId.of(datasource, interval, version, 0));
      this.datasource = datasource;
      this.interval = interval;
      this.version = version;
    }

    public String getVersion()
    {
      return version;
    }

    public Interval getInterval()
    {
      return interval;
    }

    @Override
    public SegmentId getId()
    {
      return SegmentId.of(datasource, interval, version, 0);
    }

    public boolean isClosed()
    {
      return closed;
    }

    @Override
    public Interval getDataInterval()
    {
      return interval;
    }

    @Override
    public QueryableIndex asQueryableIndex()
    {
      return INDEX;
    }

    @Override
    public CursorFactory asCursorFactory()
    {
      return new QueryableIndexCursorFactory(INDEX);
    }

    @Override
    public <T> T as(@Nonnull Class<T> clazz)
    {
      if (clazz.equals(QueryableIndex.class)) {
        return (T) asQueryableIndex();
      } else if (clazz.equals(CursorFactory.class)) {
        return (T) asCursorFactory();
      }
      return null;
    }

    @Override
    public void close()
    {
      synchronized (lock) {
        closed = true;
      }
    }
  }

  public static DataSegment makeTombstoneSegment(String dataSource, String version, Interval interval)
  {
    return new DataSegment(
        dataSource,
        interval,
        version,
        ImmutableMap.of("version", version,
                        "interval", interval,
                        "type",
                        DataSegment.TOMBSTONE_LOADSPEC_TYPE
        ),
        Arrays.asList("dim1", "dim2", "dim3"),
        Arrays.asList("metric1", "metric2"),
        TombstoneShardSpec.INSTANCE,
        IndexIO.CURRENT_VERSION_ID,
        1L
    );
  }

  public static DataSegment makeSegment(String dataSource, String version, Interval interval)
  {
    return new DataSegment(
        dataSource,
        interval,
        version,
        ImmutableMap.of("type", "test", "version", version, "interval", interval),
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
        ImmutableMap.of("type", "test", "version", version, "interval", Intervals.ETERNITY),
        Arrays.asList("dim1", "dim2", "dim3"),
        Arrays.asList("metric1", "metric2"),
        NoneShardSpec.instance(),
        IndexIO.CURRENT_VERSION_ID,
        size
    );
  }
}
