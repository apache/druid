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

package io.druid.indexer.hadoop;

import com.google.common.base.Charsets;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.Files;
import io.druid.indexer.JobHelper;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.timeline.DataSegment;
import io.druid.timeline.partition.NoneShardSpec;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.easymock.EasyMock;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 */
public class DatasourceInputFormatTest
{
  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  private List<WindowedDataSegment> segments;
  private List<LocatedFileStatus> locations;
  private JobConf config;
  private JobContext context;

  @Before
  public void setUp() throws Exception
  {
    segments = ImmutableList.of(
        WindowedDataSegment.of(
            new DataSegment(
                "test1",
                Interval.parse("2000/3000"),
                "ver",
                ImmutableMap.<String, Object>of(
                    "type", "local",
                    "path", "/tmp/index1.zip"
                ),
                ImmutableList.of("host"),
                ImmutableList.of("visited_sum", "unique_hosts"),
                NoneShardSpec.instance(),
                9,
                2
            )
        ),
        WindowedDataSegment.of(
            new DataSegment(
                "test2",
                Interval.parse("2050/3000"),
                "ver",
                ImmutableMap.<String, Object>of(
                    "type", "hdfs",
                    "path", "/tmp/index2.zip"
                ),
                ImmutableList.of("host"),
                ImmutableList.of("visited_sum", "unique_hosts"),
                NoneShardSpec.instance(),
                9,
                11
            )
        ),
        WindowedDataSegment.of(
            new DataSegment(
                "test3",
                Interval.parse("2030/3000"),
                "ver",
                ImmutableMap.<String, Object>of(
                    "type", "hdfs",
                    "path", "/tmp/index3.zip"
                ),
                ImmutableList.of("host"),
                ImmutableList.of("visited_sum", "unique_hosts"),
                NoneShardSpec.instance(),
                9,
                4
            )
        )
    );

    Path path1 = new Path(JobHelper.getURIFromSegment(segments.get(0).getSegment()));
    Path path2 = new Path(JobHelper.getURIFromSegment(segments.get(1).getSegment()));
    Path path3 = new Path(JobHelper.getURIFromSegment(segments.get(2).getSegment()));

    // dummy locations for test
    locations = ImmutableList.of(
        new LocatedFileStatus(
            1000, false, 0, 0, 0, 0, null, null, null, null, path1,
            new BlockLocation[]{
                new BlockLocation(null, new String[]{"s1", "s2"}, 0, 600),
                new BlockLocation(null, new String[]{"s2", "s3"}, 600, 400)
            }
        ),
        new LocatedFileStatus(
            4000, false, 0, 0, 0, 0, null, null, null, null, path2,
            new BlockLocation[]{
                new BlockLocation(null, new String[]{"s1", "s2"}, 0, 1000),
                new BlockLocation(null, new String[]{"s1", "s3"}, 1000, 1200),
                new BlockLocation(null, new String[]{"s2", "s3"}, 2200, 1100),
                new BlockLocation(null, new String[]{"s1", "s2"}, 3300, 700),
            }
        ),
        new LocatedFileStatus(
            500, false, 0, 0, 0, 0, null, null, null, null, path3,
            new BlockLocation[]{
                new BlockLocation(null, new String[]{"s2", "s3"}, 0, 500)
            }
        )
    );

    config = new JobConf();
    config.set(
        DatasourceInputFormat.CONF_INPUT_SEGMENTS,
        new DefaultObjectMapper().writeValueAsString(segments)
    );

    context = EasyMock.createMock(JobContext.class);
    EasyMock.expect(context.getConfiguration()).andReturn(config);
    EasyMock.replay(context);
  }

  private Supplier<InputFormat> testFormatter = new Supplier<InputFormat>() {
    @Override
    public InputFormat get()
    {
      final Map<String, LocatedFileStatus> locationMap = Maps.newHashMap();
      for (LocatedFileStatus status : locations) {
        locationMap.put(status.getPath().getName(), status);
      }

      return new TextInputFormat()
      {
        @Override
        protected boolean isSplitable(FileSystem fs, Path file)
        {
          return false;
        }

        @Override
        protected FileStatus[] listStatus(JobConf job) throws IOException
        {
          Path[] dirs = getInputPaths(job);
          if (dirs.length == 0) {
            throw new IOException("No input paths specified in job");
          }
          FileStatus[] status = new FileStatus[dirs.length];
          for (int i = 0; i < dirs.length; i++) {
            status[i] = locationMap.get(dirs[i].getName());
          }
          return status;
        }
      };
    }
  };

  @Test
  public void testGetSplitsNoCombining() throws Exception
  {
    DatasourceInputFormat inputFormat = new DatasourceInputFormat().setSupplier(testFormatter);
    List<InputSplit> splits = inputFormat.getSplits(context);

    Assert.assertEquals(segments.size(), splits.size());
    for (int i = 0; i < segments.size(); i++) {
      DatasourceInputSplit split = (DatasourceInputSplit) splits.get(i);
      Assert.assertEquals(segments.get(i), split.getSegments().get(0));
    }
    Assert.assertArrayEquals(new String[] {"s1", "s2"}, splits.get(0).getLocations());
    Assert.assertArrayEquals(new String[] {"s1", "s2"}, splits.get(1).getLocations());
    Assert.assertArrayEquals(new String[] {"s2", "s3"}, splits.get(2).getLocations());
  }

  @Test
  public void testGetSplitsAllCombined() throws Exception
  {
    config.set(DatasourceInputFormat.CONF_MAX_SPLIT_SIZE, "999999");
    List<InputSplit> splits = new DatasourceInputFormat().setSupplier(testFormatter).getSplits(context);

    Assert.assertEquals(1, splits.size());
    Assert.assertEquals(
        Sets.newHashSet(segments),
        Sets.newHashSet((((DatasourceInputSplit) splits.get(0)).getSegments()))
    );

    Assert.assertArrayEquals(new String[]{"s2", "s1", "s3"}, splits.get(0).getLocations());
  }

  @Test
  public void testGetSplitsCombineInTwo() throws Exception
  {
    config.set(DatasourceInputFormat.CONF_MAX_SPLIT_SIZE, "6");
    List<InputSplit> splits = new DatasourceInputFormat().setSupplier(testFormatter).getSplits(context);

    Assert.assertEquals(2, splits.size());

    Assert.assertEquals(
        Sets.newHashSet(segments.get(0), segments.get(2)),
        Sets.newHashSet((((DatasourceInputSplit) splits.get(0)).getSegments()))
    );
    Assert.assertArrayEquals(new String[]{"s2", "s1", "s3"}, splits.get(0).getLocations());

    Assert.assertEquals(
        Sets.newHashSet(segments.get(1)),
        Sets.newHashSet((((DatasourceInputSplit) splits.get(1)).getSegments()))
    );
    Assert.assertArrayEquals(new String[]{"s1", "s2"}, splits.get(1).getLocations());
  }

  @Test
  public void testGetSplitsCombineCalculated() throws Exception
  {
    config.set(DatasourceInputFormat.CONF_MAX_SPLIT_SIZE, "-1");
    config.setNumMapTasks(3);
    List<InputSplit> splits = new DatasourceInputFormat().setSupplier(testFormatter).getSplits(context);

    Assert.assertEquals(3, splits.size());

    Assert.assertEquals(
        Sets.newHashSet(segments.get(0)),
        Sets.newHashSet((((DatasourceInputSplit) splits.get(0)).getSegments()))
    );
    Assert.assertArrayEquals(new String[]{"s1", "s2"}, splits.get(0).getLocations());

    Assert.assertEquals(
        Sets.newHashSet(segments.get(2)),
        Sets.newHashSet((((DatasourceInputSplit) splits.get(1)).getSegments()))
    );
    Assert.assertArrayEquals(new String[]{"s2", "s3"}, splits.get(1).getLocations());

    Assert.assertEquals(
        Sets.newHashSet(segments.get(1)),
        Sets.newHashSet((((DatasourceInputSplit) splits.get(2)).getSegments()))
    );
    Assert.assertArrayEquals(new String[]{"s1", "s2"}, splits.get(2).getLocations());
  }

  @Test
  public void testGetSplitsUsingDefaultSupplier() throws Exception
  {
    // Use the builtin supplier, reading from the local filesystem, rather than testFormatter.
    final File tmpFile = temporaryFolder.newFile("something:with:colons");
    Files.write("dummy", tmpFile, Charsets.UTF_8);

    final ImmutableList<WindowedDataSegment> mySegments = ImmutableList.of(
        WindowedDataSegment.of(
            new DataSegment(
                "test1",
                Interval.parse("2000/3000"),
                "ver",
                ImmutableMap.<String, Object>of(
                    "type", "local",
                    "path", tmpFile.getPath()
                ),
                ImmutableList.of("host"),
                ImmutableList.of("visited_sum", "unique_hosts"),
                NoneShardSpec.instance(),
                9,
                2
            )
        )
    );

    final JobConf myConfig = new JobConf();
    myConfig.set(
        DatasourceInputFormat.CONF_INPUT_SEGMENTS,
        new DefaultObjectMapper().writeValueAsString(mySegments)
    );

    final JobContext myContext = EasyMock.createMock(JobContext.class);
    EasyMock.expect(myContext.getConfiguration()).andReturn(myConfig);
    EasyMock.replay(myContext);

    final List<InputSplit> splits = new DatasourceInputFormat().getSplits(myContext);
    Assert.assertEquals(1, splits.size());
    final DatasourceInputSplit theSplit = (DatasourceInputSplit) Iterables.getOnlyElement(splits);
    Assert.assertEquals(mySegments.get(0).getSegment().getSize(), theSplit.getLength());
    Assert.assertEquals(mySegments, theSplit.getSegments());
    Assert.assertArrayEquals(new String[]{"localhost"}, theSplit.getLocations());
  }

  @Test
  public void testGetRecordReader() throws Exception
  {
    Assert.assertTrue(new DatasourceInputFormat().createRecordReader(null, null) instanceof DatasourceRecordReader);
  }

  @Test
  public void testGetFrequentLocationsEmpty()
  {
    Assert.assertArrayEquals(
        new String[0],
        DatasourceInputFormat.getFrequentLocations(Stream.empty())
    );
  }

  @Test
  public void testGetFrequentLocationsLessThan3()
  {
    Assert.assertArrayEquals(
        new String[]{"s1", "s2"},
        DatasourceInputFormat.getFrequentLocations(Stream.of("s2", "s1"))
    );
  }

  @Test
  public void testGetFrequentLocationsMoreThan3()
  {
    Assert.assertArrayEquals(
        new String[]{"s3", "s1", "s2"},
        DatasourceInputFormat.getFrequentLocations(
            Stream.of("s3", "e", "s2", "s3", "s4", "s3", "s1", "s3", "s2", "s1")
        )
    );
  }

  @Test
  public void testGetLocationsInputFormatException() throws IOException
  {
    final InputFormat fio = EasyMock.mock(
        InputFormat.class
    );

    EasyMock.expect(fio.getSplits(config, 1)).andThrow(new IOException("testing"));
    EasyMock.replay(fio);

    Assert.assertEquals(
        0,
        DatasourceInputFormat.getLocations(segments.subList(0, 1), fio, config).count()
    );
  }

  @Test
  public void testGetLocationsSplitException() throws IOException
  {
    final InputFormat fio = EasyMock.mock(
        InputFormat.class
    );

    final org.apache.hadoop.mapred.InputSplit split = EasyMock.mock(
        org.apache.hadoop.mapred.InputSplit.class
    );

    EasyMock.expect(fio.getSplits(config, 1)).andReturn(
        new org.apache.hadoop.mapred.InputSplit[] {split}
    );
    EasyMock.expect(split.getLocations()).andThrow(new IOException("testing"));

    EasyMock.replay(fio, split);

    Assert.assertEquals(
        0,
        DatasourceInputFormat.getLocations(segments.subList(0, 1), fio, config).count()
    );
  }

  @Test
  public void testGetLocations() throws IOException
  {
    final InputFormat fio = EasyMock.mock(
        InputFormat.class
    );

    final org.apache.hadoop.mapred.InputSplit split = EasyMock.mock(
        org.apache.hadoop.mapred.InputSplit.class
    );

    EasyMock.expect(fio.getSplits(config, 1)).andReturn(
        new org.apache.hadoop.mapred.InputSplit[] {split}
    );
    EasyMock.expect(split.getLocations()).andReturn(new String[] {"s1", "s2"});

    EasyMock.expect(fio.getSplits(config, 1)).andReturn(
        new org.apache.hadoop.mapred.InputSplit[] {split}
    );
    EasyMock.expect(split.getLocations()).andReturn(new String[] {"s3"});

    EasyMock.expect(fio.getSplits(config, 1)).andReturn(
        new org.apache.hadoop.mapred.InputSplit[] {split}
    );
    EasyMock.expect(split.getLocations()).andReturn(new String[] {"s4", "s2"});

    EasyMock.replay(fio, split);

    Assert.assertArrayEquals(
        new String[] {"s1", "s2", "s3", "s4", "s2"},
        DatasourceInputFormat.getLocations(segments, fio, config).toArray(String[]::new)
    );
  }
}
