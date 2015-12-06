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

package io.druid.indexing.common.task;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.druid.indexing.common.TestUtils;
import io.druid.segment.IndexSpec;
import io.druid.segment.data.ConciseBitmapSerdeFactory;
import io.druid.timeline.DataSegment;
import io.druid.timeline.partition.NoneShardSpec;
import io.druid.timeline.partition.ShardSpec;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;

public class HadoopConverterTaskSerDeTest
{

  private static final String TASK_ID = "task id";
  private static final String DATA_SOURCE = "datasource";
  private static final Interval INTERVAL = Interval.parse("2010/2011");
  private static final String SEGMENT_VERSION = "some version";
  private static final Map<String, Object> LOAD_SPEC = ImmutableMap.<String, Object>of("someKey", "someVal");
  private static final List<String> DIMENSIONS = ImmutableList.of("dim1", "dim2");
  private static final List<String> METRICS = ImmutableList.of("metric1", "metric2");
  private static final ShardSpec SHARD_SPEC = new NoneShardSpec();
  private static final int BINARY_VERSION = 34718;
  private static final long SEGMENT_SIZE = 7483901348790L;
  private static final IndexSpec INDEX_SPEC = new IndexSpec(new ConciseBitmapSerdeFactory(), "lz4", "lzf");
  private static final DataSegment DATA_SEGMENT = new DataSegment(
      DATA_SOURCE,
      INTERVAL,
      SEGMENT_VERSION,
      LOAD_SPEC,
      DIMENSIONS,
      METRICS,
      SHARD_SPEC,
      BINARY_VERSION,
      SEGMENT_SIZE
  );
  private static final List<String> HADOOP_DEPENDENCY = ImmutableList.of("dependency1");
  private static final URI DISTRIBUTED_CACHE = URI.create("http://your.momma"); // Should have plenty of space
  private static final String PRIORITY = "0";
  private static final String OUTPUT_PATH = "/dev/null";
  private static final String CLASSPATH_PREFIX = "something:where:I:need:stuff";

  private final ObjectMapper jsonMapper;

  public HadoopConverterTaskSerDeTest()
  {
    TestUtils testUtils = new TestUtils();
    jsonMapper = testUtils.getTestObjectMapper();
  }

  @Test
  public void testSimpleConverterTaskSerDe() throws IOException
  {
    HadoopConverterTask orig = new HadoopConverterTask(
        TASK_ID,
        DATA_SOURCE,
        INTERVAL,
        INDEX_SPEC,
        true,
        true,
        HADOOP_DEPENDENCY,
        DISTRIBUTED_CACHE,
        PRIORITY,
        OUTPUT_PATH,
        CLASSPATH_PREFIX,
        null
    );
    final String strOrig = jsonMapper.writeValueAsString(orig);
    HadoopConverterTask other = jsonMapper.readValue(strOrig, HadoopConverterTask.class);
    Assert.assertEquals(strOrig, jsonMapper.writeValueAsString(other));
    Assert.assertFalse(orig == other);
    Assert.assertEquals(orig, other);
    assertExpectedTask(other);
  }

  @Test
  public void testSimpleSubTaskSerDe() throws IOException
  {
    HadoopConverterTask parent = new HadoopConverterTask(
        TASK_ID,
        DATA_SOURCE,
        INTERVAL,
        INDEX_SPEC,
        true,
        true,
        HADOOP_DEPENDENCY,
        DISTRIBUTED_CACHE,
        PRIORITY,
        OUTPUT_PATH,
        CLASSPATH_PREFIX,
        null
    );
    HadoopConverterTask.ConverterSubTask subTask = new HadoopConverterTask.ConverterSubTask(
        ImmutableList.of(
            DATA_SEGMENT
        ),
        parent,
        null
    );
    final String origString = jsonMapper.writeValueAsString(subTask);
    final HadoopConverterTask.ConverterSubTask otherSub = jsonMapper.readValue(
        origString,
        HadoopConverterTask.ConverterSubTask.class
    );
    Assert.assertEquals(subTask, otherSub);
    Assert.assertEquals(origString, jsonMapper.writeValueAsString(otherSub));
    Assert.assertEquals(ImmutableList.of(DATA_SEGMENT), otherSub.getSegments());
    Assert.assertFalse(parent == otherSub.getParent());
    Assert.assertEquals(parent, otherSub.getParent());

    assertExpectedTask(otherSub.getParent());
  }

  private static void assertExpectedTask(HadoopConverterTask other)
  {
    Assert.assertEquals(TASK_ID, other.getId());
    Assert.assertEquals(DATA_SOURCE, other.getDataSource());
    Assert.assertEquals(INTERVAL, other.getInterval());
    Assert.assertEquals(INDEX_SPEC, other.getIndexSpec());
    Assert.assertTrue(other.isForce());
    Assert.assertTrue(other.isValidate());
    Assert.assertEquals(HADOOP_DEPENDENCY, other.getHadoopDependencyCoordinates());
    Assert.assertEquals(DISTRIBUTED_CACHE, other.getDistributedSuccessCache());
    Assert.assertEquals(PRIORITY, other.getJobPriority());
    Assert.assertEquals(OUTPUT_PATH, other.getSegmentOutputPath());
    Assert.assertEquals(CLASSPATH_PREFIX, other.getClasspathPrefix());
  }

  @Test
  public void testSubTask()
  {
    HadoopConverterTask parent = new HadoopConverterTask(
        TASK_ID,
        DATA_SOURCE,
        INTERVAL,
        INDEX_SPEC,
        true,
        true,
        HADOOP_DEPENDENCY,
        DISTRIBUTED_CACHE,
        PRIORITY,
        OUTPUT_PATH,
        CLASSPATH_PREFIX,
        null
    );
    HadoopConverterTask.ConverterSubTask subTask = new HadoopConverterTask.ConverterSubTask(
        ImmutableList.of(
            DATA_SEGMENT
        ),
        parent,
        null
    );
    Assert.assertEquals(parent.getType(), "hadoop_convert_segment");
    Assert.assertEquals(parent.getType() + "_sub", subTask.getType());
  }

  @Test
  public void testNullValidate()
  {
    HadoopConverterTask orig = new HadoopConverterTask(
        TASK_ID,
        DATA_SOURCE,
        INTERVAL,
        INDEX_SPEC,
        true,
        null,
        HADOOP_DEPENDENCY,
        DISTRIBUTED_CACHE,
        PRIORITY,
        OUTPUT_PATH,
        CLASSPATH_PREFIX,
        null
    );
    Assert.assertTrue(orig.isValidate());
  }

  @Test
  public void testMinimal()
  {
    HadoopConverterTask parent = new HadoopConverterTask(
        null,
        DATA_SOURCE,
        INTERVAL,
        null,
        true,
        null,
        null,
        DISTRIBUTED_CACHE,
        null,
        OUTPUT_PATH,
        null,
        null
    );
    Assert.assertEquals(DATA_SOURCE, parent.getDataSource());
    Assert.assertEquals(INTERVAL, parent.getInterval());
    Assert.assertEquals(DISTRIBUTED_CACHE, parent.getDistributedSuccessCache());
    Assert.assertEquals(OUTPUT_PATH, parent.getSegmentOutputPath());
    Assert.assertNotNull(parent.getId());
    Assert.assertFalse(parent.getId().isEmpty());
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testGetDataSegment()
  {
    HadoopConverterTask orig = new HadoopConverterTask(
        TASK_ID,
        DATA_SOURCE,
        INTERVAL,
        INDEX_SPEC,
        true,
        null,
        HADOOP_DEPENDENCY,
        DISTRIBUTED_CACHE,
        PRIORITY,
        OUTPUT_PATH,
        CLASSPATH_PREFIX,
        null
    );
    orig.getSegment();
  }

  @Test(expected = NullPointerException.class)
  public void testNull1()
  {
    HadoopConverterTask parent = new HadoopConverterTask(
        null,
        null,
        INTERVAL,
        null,
        true,
        null,
        null,
        DISTRIBUTED_CACHE,
        null,
        OUTPUT_PATH,
        null,
        null
    );
  }

  @Test(expected = NullPointerException.class)
  public void testNull2()
  {
    HadoopConverterTask parent = new HadoopConverterTask(
        null,
        DATA_SOURCE,
        null,
        null,
        true,
        null,
        null,
        DISTRIBUTED_CACHE,
        null,
        OUTPUT_PATH,
        null,
        null
    );
  }

  @Test(expected = NullPointerException.class)
  public void testNull3()
  {
    HadoopConverterTask parent = new HadoopConverterTask(
        null,
        DATA_SOURCE,
        INTERVAL,
        null,
        true,
        null,
        null,
        null,
        null,
        OUTPUT_PATH,
        null,
        null
    );
  }

  @Test(expected = NullPointerException.class)
  public void testNull4()
  {
    HadoopConverterTask parent = new HadoopConverterTask(
        null,
        DATA_SOURCE,
        INTERVAL,
        null,
        true,
        null,
        null,
        DISTRIBUTED_CACHE,
        null,
        null,
        null,
        null
    );
  }
}
