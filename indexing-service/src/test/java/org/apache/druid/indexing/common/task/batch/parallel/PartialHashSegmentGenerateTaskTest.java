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
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.realtime.firehose.InlineFirehoseFactory;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class PartialHashSegmentGenerateTaskTest
{
  private static final ObjectMapper OBJECT_MAPPER = Factory.createObjectMapper();
  private static final ParallelIndexIngestionSpec INGESTION_SPEC = Factory.createIngestionSpec(
      new InlineFirehoseFactory("data"),
      new Factory.TuningConfigBuilder().build(),
      Factory.createDataSchema(Factory.INPUT_INTERVALS)
  );

  private PartialSegmentGenerateTask target;

  @Before
  public void setup()
  {
    target = new PartialHashSegmentGenerateTask(
        Factory.AUTOMATIC_ID,
        Factory.GROUP_ID,
        Factory.TASK_RESOURCE,
        Factory.SUPERVISOR_TASK_ID,
        Factory.NUM_ATTEMPTS,
        INGESTION_SPEC,
        Factory.CONTEXT,
        Factory.INDEXING_SERVICE_CLIENT,
        Factory.TASK_CLIENT_FACTORY,
        Factory.APPENDERATORS_MANAGER
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
}
