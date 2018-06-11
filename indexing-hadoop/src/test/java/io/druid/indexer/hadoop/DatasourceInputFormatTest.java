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

import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.Files;
import io.druid.indexer.JobHelper;
import io.druid.java.util.common.Intervals;
import io.druid.java.util.common.JodaUtils;
import io.druid.timeline.DataSegment;
import io.druid.timeline.partition.NoneShardSpec;
import org.apache.hadoop.conf.Configuration;
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
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 */
public class DatasourceInputFormatTest
{
  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  private List<WindowedDataSegment> segments1;
  private List<WindowedDataSegment> segments2;
  private List<LocatedFileStatus> locations;
  private JobConf config;
  private JobContext context;

  @Before
  public void setUp() throws Exception
  {
    segments1 = ImmutableList.of(
        WindowedDataSegment.of(
            new DataSegment(
                "test1",
                Intervals.of("2000/3000"),
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
                "test1",
                Intervals.of("2050/3000"),
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
                "test1",
                Intervals.of("2030/3000"),
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

    segments2 = ImmutableList.of(
        WindowedDataSegment.of(
            new DataSegment(
                "test2",
                Intervals.of("2000/3000"),
                "ver",
                ImmutableMap.<String, Object>of(
                    "type", "local",
                    "path", "/tmp/index4.zip"
                ),
                ImmutableList.of("host"),
                ImmutableList.of("visited_sum", "unique_hosts"),
                NoneShardSpec.instance(),
                9,
                2
            )
        )
    );

    Path path1 = new Path(JobHelper.getURIFromSegment(segments1.get(0).getSegment()));
    Path path2 = new Path(JobHelper.getURIFromSegment(segments1.get(1).getSegment()));
    Path path3 = new Path(JobHelper.getURIFromSegment(segments1.get(2).getSegment()));
    Path path4 = new Path(JobHelper.getURIFromSegment(segments2.get(0).getSegment()));

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
                new BlockLocation(null, new String[]{"s1", "s2"}, 3300, 700)
            }
        ),
        new LocatedFileStatus(
            500, false, 0, 0, 0, 0, null, null, null, null, path3,
            new BlockLocation[]{
                new BlockLocation(null, new String[]{"s2", "s3"}, 0, 500)
            }
        ),
        new LocatedFileStatus(
            500, false, 0, 0, 0, 0, null, null, null, null, path4,
            new BlockLocation[]{
                new BlockLocation(null, new String[]{"s2", "s3"}, 0, 500)
            }
        )
    );

    config = populateConfiguration(new JobConf(), segments1, 0);
    context = EasyMock.createMock(JobContext.class);
    EasyMock.expect(context.getConfiguration()).andReturn(config);
    EasyMock.replay(context);
  }

  private static <T extends Configuration> T populateConfiguration(
      final T conf,
      final List<WindowedDataSegment> segments,
      final long maxSplitSize
  )
      throws IOException
  {
    DatasourceInputFormat.addDataSource(
        conf,
        new DatasourceIngestionSpec(
            Iterators.getOnlyElement(segments.stream().map(s -> s.getSegment().getDataSource()).distinct().iterator()),
            null,
            Collections.singletonList(
                JodaUtils.umbrellaInterval(
                    segments.stream()
                            .map(WindowedDataSegment::getInterval)
                            .collect(Collectors.toList())
                )
            ),
            null,
            null,
            null,
            null,
            false,
            null
        ),
        segments,
        maxSplitSize
    );

    return conf;
  }

  private Supplier<InputFormat> testFormatter = new Supplier<InputFormat>()
  {
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

    Assert.assertEquals(segments1.size(), splits.size());
    for (int i = 0; i < segments1.size(); i++) {
      DatasourceInputSplit split = (DatasourceInputSplit) splits.get(i);
      Assert.assertEquals(segments1.get(i), split.getSegments().get(0));
    }
    Assert.assertArrayEquals(new String[]{"s1", "s2"}, splits.get(0).getLocations());
    Assert.assertArrayEquals(new String[]{"s1", "s2"}, splits.get(1).getLocations());
    Assert.assertArrayEquals(new String[]{"s2", "s3"}, splits.get(2).getLocations());
  }

  @Test
  public void testGetSplitsTwoDataSources() throws Exception
  {
    config.clear();
    populateConfiguration(populateConfiguration(config, segments1, 999999), segments2, 999999);
    List<InputSplit> splits = new DatasourceInputFormat().setSupplier(testFormatter).getSplits(context);

    Assert.assertEquals(2, splits.size());
    Assert.assertEquals(
        Sets.newHashSet(segments1),
        Sets.newHashSet((((DatasourceInputSplit) splits.get(0)).getSegments()))
    );
    Assert.assertEquals(
        Sets.newHashSet(segments2),
        Sets.newHashSet((((DatasourceInputSplit) splits.get(1)).getSegments()))
    );

    Assert.assertArrayEquals(new String[]{"s2", "s1", "s3"}, splits.get(0).getLocations());
    Assert.assertArrayEquals(new String[]{"s2", "s3"}, splits.get(1).getLocations());
  }

  @Test
  public void testGetSplitsAllCombined() throws Exception
  {
    config.clear();
    populateConfiguration(config, segments1, 999999);
    List<InputSplit> splits = new DatasourceInputFormat().setSupplier(testFormatter).getSplits(context);

    Assert.assertEquals(1, splits.size());
    Assert.assertEquals(
        Sets.newHashSet(segments1),
        Sets.newHashSet((((DatasourceInputSplit) splits.get(0)).getSegments()))
    );

    Assert.assertArrayEquals(new String[]{"s2", "s1", "s3"}, splits.get(0).getLocations());
  }

  @Test
  public void testGetSplitsCombineInTwo() throws Exception
  {
    config.clear();
    populateConfiguration(config, segments1, 6);
    List<InputSplit> splits = new DatasourceInputFormat().setSupplier(testFormatter).getSplits(context);

    Assert.assertEquals(2, splits.size());

    Assert.assertEquals(
        Sets.newHashSet(segments1.get(0), segments1.get(2)),
        Sets.newHashSet((((DatasourceInputSplit) splits.get(0)).getSegments()))
    );
    Assert.assertArrayEquals(new String[]{"s2", "s1", "s3"}, splits.get(0).getLocations());

    Assert.assertEquals(
        Sets.newHashSet(segments1.get(1)),
        Sets.newHashSet((((DatasourceInputSplit) splits.get(1)).getSegments()))
    );
    Assert.assertArrayEquals(new String[]{"s1", "s2"}, splits.get(1).getLocations());
  }

  @Test
  public void testGetSplitsCombineCalculated() throws Exception
  {
    config.clear();
    populateConfiguration(config, segments1, -1);
    config.setNumMapTasks(3);
    List<InputSplit> splits = new DatasourceInputFormat().setSupplier(testFormatter).getSplits(context);

    Assert.assertEquals(3, splits.size());

    Assert.assertEquals(
        Sets.newHashSet(segments1.get(0)),
        Sets.newHashSet((((DatasourceInputSplit) splits.get(0)).getSegments()))
    );
    Assert.assertArrayEquals(new String[]{"s1", "s2"}, splits.get(0).getLocations());

    Assert.assertEquals(
        Sets.newHashSet(segments1.get(2)),
        Sets.newHashSet((((DatasourceInputSplit) splits.get(1)).getSegments()))
    );
    Assert.assertArrayEquals(new String[]{"s2", "s3"}, splits.get(1).getLocations());

    Assert.assertEquals(
        Sets.newHashSet(segments1.get(1)),
        Sets.newHashSet((((DatasourceInputSplit) splits.get(2)).getSegments()))
    );
    Assert.assertArrayEquals(new String[]{"s1", "s2"}, splits.get(2).getLocations());
  }

  @Test
  public void testGetSplitsUsingDefaultSupplier() throws Exception
  {
    // Use the builtin supplier, reading from the local filesystem, rather than testFormatter.
    final File tmpFile = temporaryFolder.newFile("something:with:colons");
    Files.write("dummy", tmpFile, StandardCharsets.UTF_8);

    final ImmutableList<WindowedDataSegment> mySegments = ImmutableList.of(
        WindowedDataSegment.of(
            new DataSegment(
                "test1",
                Intervals.of("2000/3000"),
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

    final JobConf myConfig = populateConfiguration(new JobConf(), mySegments, 0L);
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
  public void testGetRecordReader()
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
        DatasourceInputFormat.getLocations(segments1.subList(0, 1), fio, config).count()
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
        new org.apache.hadoop.mapred.InputSplit[]{split}
    );
    EasyMock.expect(split.getLocations()).andThrow(new IOException("testing"));

    EasyMock.replay(fio, split);

    Assert.assertEquals(
        0,
        DatasourceInputFormat.getLocations(segments1.subList(0, 1), fio, config).count()
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
        new org.apache.hadoop.mapred.InputSplit[]{split}
    );
    EasyMock.expect(split.getLocations()).andReturn(new String[]{"s1", "s2"});

    EasyMock.expect(fio.getSplits(config, 1)).andReturn(
        new org.apache.hadoop.mapred.InputSplit[]{split}
    );
    EasyMock.expect(split.getLocations()).andReturn(new String[]{"s3"});

    EasyMock.expect(fio.getSplits(config, 1)).andReturn(
        new org.apache.hadoop.mapred.InputSplit[]{split}
    );
    EasyMock.expect(split.getLocations()).andReturn(new String[]{"s4", "s2"});

    EasyMock.replay(fio, split);

    Assert.assertArrayEquals(
        new String[]{"s1", "s2", "s3", "s4", "s2"},
        DatasourceInputFormat.getLocations(segments1, fio, config).toArray(String[]::new)
    );
  }
}
