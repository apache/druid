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

import org.apache.druid.indexer.partitions.HashedPartitionsSpec;
import org.apache.druid.segment.TestHelper;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;

public class PartialHashSegmentMergeTaskTest extends AbstractParallelIndexSupervisorTaskTest
{
  private static final HashPartitionLocation HASH_PARTITION_LOCATION = new HashPartitionLocation(
      ParallelIndexTestingFactory.HOST,
      ParallelIndexTestingFactory.PORT,
      ParallelIndexTestingFactory.USE_HTTPS,
      ParallelIndexTestingFactory.SUBTASK_ID,
      ParallelIndexTestingFactory.INTERVAL,
      ParallelIndexTestingFactory.PARTITION_ID
  );
  private static final PartialHashSegmentMergeIOConfig IO_CONFIG =
      new PartialHashSegmentMergeIOConfig(Collections.singletonList(HASH_PARTITION_LOCATION));
  private static final HashedPartitionsSpec PARTITIONS_SPEC = new HashedPartitionsSpec(
      null,
      1,
      Collections.emptyList()
  );
  private static final PartialHashSegmentMergeIngestionSpec INGESTION_SPEC =
      new PartialHashSegmentMergeIngestionSpec(
          ParallelIndexTestingFactory.createDataSchema(ParallelIndexTestingFactory.INPUT_INTERVALS),
          IO_CONFIG,
          new ParallelIndexTestingFactory.TuningConfigBuilder()
              .partitionsSpec(PARTITIONS_SPEC)
              .build()
      );

  private PartialHashSegmentMergeTask target;

  @Before
  public void setup()
  {
    target = new PartialHashSegmentMergeTask(
        ParallelIndexTestingFactory.AUTOMATIC_ID,
        ParallelIndexTestingFactory.GROUP_ID,
        ParallelIndexTestingFactory.TASK_RESOURCE,
        ParallelIndexTestingFactory.SUPERVISOR_TASK_ID,
        ParallelIndexTestingFactory.NUM_ATTEMPTS,
        INGESTION_SPEC,
        ParallelIndexTestingFactory.CONTEXT,
        ParallelIndexTestingFactory.INDEXING_SERVICE_CLIENT,
        ParallelIndexTestingFactory.TASK_CLIENT_FACTORY,
        ParallelIndexTestingFactory.SHUFFLE_CLIENT
    );
  }

  @Test
  public void serializesDeserializes()
  {
    TestHelper.testSerializesDeserializes(getObjectMapper(), target);
  }

  @Test
  public void hasCorrectPrefixForAutomaticId()
  {
    String id = target.getId();
    Assert.assertThat(id, Matchers.startsWith(PartialHashSegmentMergeTask.TYPE));
  }
}
