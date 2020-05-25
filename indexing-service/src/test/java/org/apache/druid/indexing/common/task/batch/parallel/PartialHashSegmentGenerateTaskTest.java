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

package org.apache.druid.indexing.common.task.batch.parallel;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import org.apache.druid.data.input.impl.JsonInputFormat;
import org.apache.druid.data.input.impl.LocalInputSource;
import org.apache.druid.indexer.partitions.HashedPartitionsSpec;
import org.apache.druid.indexing.common.task.batch.partition.HashPartitionAnalysis;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.indexing.granularity.UniformGranularitySpec;
import org.hamcrest.Matchers;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.List;

public class PartialHashSegmentGenerateTaskTest
{
  private static final ObjectMapper OBJECT_MAPPER = ParallelIndexTestingFactory.createObjectMapper();
  private static final ParallelIndexIngestionSpec INGESTION_SPEC = ParallelIndexTestingFactory.createIngestionSpec(
      new LocalInputSource(new File("baseDir"), "filer"),
      new JsonInputFormat(null, null, null),
      new ParallelIndexTestingFactory.TuningConfigBuilder().build(),
      ParallelIndexTestingFactory.createDataSchema(ParallelIndexTestingFactory.INPUT_INTERVALS)
  );

  private PartialHashSegmentGenerateTask target;

  @Before
  public void setup()
  {
    target = new PartialHashSegmentGenerateTask(
        ParallelIndexTestingFactory.AUTOMATIC_ID,
        ParallelIndexTestingFactory.GROUP_ID,
        ParallelIndexTestingFactory.TASK_RESOURCE,
        ParallelIndexTestingFactory.SUPERVISOR_TASK_ID,
        ParallelIndexTestingFactory.NUM_ATTEMPTS,
        INGESTION_SPEC,
        ParallelIndexTestingFactory.CONTEXT,
        ParallelIndexTestingFactory.INDEXING_SERVICE_CLIENT,
        ParallelIndexTestingFactory.TASK_CLIENT_FACTORY,
        ParallelIndexTestingFactory.APPENDERATORS_MANAGER
    );
  }

  @Test
  public void serializesDeserializes()
  {
    TestHelper.testSerializesDeserializes(OBJECT_MAPPER, target);
  }

  @Test
  public void hasCorrectPrefixForAutomaticId()
  {
    String id = target.getId();
    Assert.assertThat(id, Matchers.startsWith(PartialHashSegmentGenerateTask.TYPE));
  }

  @Test
  public void testCreateHashPartitionAnalysisFromPartitionsSpecWithNumShardsReturningAnalysisOfValidNumBuckets()
  {
    final List<Interval> intervals = ImmutableList.of(
        Intervals.of("2020-01-01/2020-01-02"),
        Intervals.of("2020-01-02/2020-01-03"),
        Intervals.of("2020-01-03/2020-01-04")
    );
    final int expectedNumBuckets = 5;
    final HashPartitionAnalysis partitionAnalysis = PartialHashSegmentGenerateTask
        .createHashPartitionAnalysisFromPartitionsSpec(
            new UniformGranularitySpec(
                Granularities.DAY,
                Granularities.NONE,
                intervals
            ),
            new HashedPartitionsSpec(null, expectedNumBuckets, null)
        );
    Assert.assertEquals(intervals.size(), partitionAnalysis.getNumTimePartitions());
    for (Interval interval : intervals) {
      Assert.assertEquals(expectedNumBuckets, partitionAnalysis.getBucketAnalysis(interval).intValue());
    }
  }
}
