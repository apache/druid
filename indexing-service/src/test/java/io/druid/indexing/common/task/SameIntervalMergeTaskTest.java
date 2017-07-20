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
import com.google.common.collect.Lists;
import io.druid.indexing.common.TaskLock;
import io.druid.indexing.common.TaskToolbox;
import io.druid.indexing.common.TestUtils;
import io.druid.indexing.common.actions.LockListAction;
import io.druid.indexing.common.actions.LockTryAcquireAction;
import io.druid.indexing.common.actions.SegmentInsertAction;
import io.druid.indexing.common.actions.SegmentListUsedAction;
import io.druid.indexing.common.actions.TaskAction;
import io.druid.indexing.common.actions.TaskActionClient;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.CountAggregatorFactory;
import io.druid.segment.IndexIO;
import io.druid.segment.IndexMergerV9;
import io.druid.segment.IndexSpec;
import io.druid.segment.Segment;
import io.druid.segment.loading.DataSegmentPusher;
import io.druid.segment.loading.SegmentLoader;
import io.druid.segment.loading.SegmentLoadingException;
import io.druid.server.metrics.NoopServiceEmitter;
import io.druid.timeline.DataSegment;
import io.druid.timeline.partition.LinearShardSpec;
import io.druid.timeline.partition.NoneShardSpec;
import org.easymock.EasyMock;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

public class SameIntervalMergeTaskTest
{
  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();
  public TaskLock taskLock;
  private final CountDownLatch isRedayCountDown = new CountDownLatch(1);
  private final CountDownLatch publishCountDown = new CountDownLatch(1);
  private final IndexSpec indexSpec;
  private final ObjectMapper jsonMapper;
  private IndexIO indexIO;

  public SameIntervalMergeTaskTest()
  {
    indexSpec = new IndexSpec();
    TestUtils testUtils = new TestUtils();
    jsonMapper = testUtils.getTestObjectMapper();
    indexIO = testUtils.getTestIndexIO();
  }

  @Test
  public void testRun() throws Exception
  {
    final List<AggregatorFactory> aggregators = ImmutableList.<AggregatorFactory>of(new CountAggregatorFactory("cnt"));
    final SameIntervalMergeTask task = new SameIntervalMergeTask(
        null,
        "foo",
        new Interval("2010-01-01/P1D"),
        aggregators,
        true,
        indexSpec,
        true,
        null
    );

    String newVersion = "newVersion";
    final List<DataSegment> segments = runTask(task, newVersion);

    // the lock is acquired
    Assert.assertEquals(0, isRedayCountDown.getCount());
    // the merged segment is published
    Assert.assertEquals(0, publishCountDown.getCount());
    // the merged segment is the only element
    Assert.assertEquals(1, segments.size());

    DataSegment mergeSegment = segments.get(0);
    Assert.assertEquals("foo", mergeSegment.getDataSource());
    Assert.assertEquals(newVersion, mergeSegment.getVersion());
    // the merged segment's interval is within the requested interval
    Assert.assertTrue(new Interval("2010-01-01/P1D").contains(mergeSegment.getInterval()));
    // the merged segment should be NoneShardSpec
    Assert.assertTrue(mergeSegment.getShardSpec() instanceof NoneShardSpec);
  }

  private List<DataSegment> runTask(final SameIntervalMergeTask mergeTask, final String version) throws Exception
  {
    boolean isReady = mergeTask.isReady(new TaskActionClient()
    {
      @Override
      public <RetType> RetType submit(TaskAction<RetType> taskAction) throws IOException
      {
        if (taskAction instanceof LockTryAcquireAction) {
          // the lock of this interval is required
          Assert.assertEquals(mergeTask.getInterval(), ((LockTryAcquireAction) taskAction).getInterval());
          isRedayCountDown.countDown();
          taskLock = new TaskLock(
              mergeTask.getGroupId(),
              mergeTask.getDataSource(),
              mergeTask.getInterval(),
              version
          );
          return (RetType) taskLock;
        }
        return null;
      }
    });
    // ensure LockTryAcquireAction is submitted
    Assert.assertTrue(isReady);
    final List<DataSegment> segments = Lists.newArrayList();

    mergeTask.run(
        new TaskToolbox(
            null, new TaskActionClient()
        {
          @Override
          public <RetType> RetType submit(TaskAction<RetType> taskAction) throws IOException
          {
            if (taskAction instanceof LockListAction) {
              Assert.assertNotNull("taskLock should be acquired before list", taskLock);
              return (RetType) Arrays.asList(taskLock);
            }
            if (taskAction instanceof SegmentListUsedAction) {
              List<DataSegment> segments = ImmutableList.of(
                  DataSegment.builder()
                             .dataSource(mergeTask.getDataSource())
                             .interval(new Interval("2010-01-01/PT1H"))
                             .version("oldVersion")
                             .shardSpec(new LinearShardSpec(0))
                             .build(),
                  DataSegment.builder()
                             .dataSource(mergeTask.getDataSource())
                             .interval(new Interval("2010-01-01/PT1H"))
                             .version("oldVersion")
                             .shardSpec(new LinearShardSpec(0))
                             .build(),
                  DataSegment.builder()
                             .dataSource(mergeTask.getDataSource())
                             .interval(new Interval("2010-01-01/PT2H"))
                             .version("oldVersion")
                             .shardSpec(new LinearShardSpec(0))
                             .build()
              );
              return (RetType) segments;
            }
            if (taskAction instanceof SegmentInsertAction) {
              publishCountDown.countDown();
              return null;
            }

            return null;
          }
        }, new NoopServiceEmitter(), new DataSegmentPusher()
        {
          @Deprecated
          @Override
          public String getPathForHadoop(String dataSource)
          {
            return getPathForHadoop();
          }

          @Override
          public String getPathForHadoop()
          {
            return null;
          }

          @Override
          public DataSegment push(File file, DataSegment segment) throws IOException
          {
            // the merged segment is pushed to storage
            segments.add(segment);
            return segment;
          }
          @Override
          public Map<String, Object> makeLoadSpec(URI finalIndexZipFilePath)
          {
            return null;
          }

        }, null, null, null, null, null, null, null, null, null, new SegmentLoader()
        {
          @Override
          public boolean isSegmentLoaded(DataSegment segment) throws SegmentLoadingException
          {
            return false;
          }

          @Override
          public Segment getSegment(DataSegment segment) throws SegmentLoadingException
          {
            return null;
          }

          @Override
          public File getSegmentFiles(DataSegment segment) throws SegmentLoadingException
          {
            // dummy file to represent the downloaded segment's dir
            return new File("" + segment.getShardSpec().getPartitionNum());
          }

          @Override
          public void cleanup(DataSegment segment) throws SegmentLoadingException
          {
          }
        }, jsonMapper, temporaryFolder.newFolder(),
            indexIO, null, null, EasyMock.createMock(IndexMergerV9.class)
        )
    );

    return segments;
  }
}
