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
import org.apache.druid.data.input.impl.JsonInputFormat;
import org.apache.druid.data.input.impl.LocalInputSource;
import org.apache.druid.segment.TestHelper;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

public class PartialHashSegmentGenerateTaskTest
{
  private static final ObjectMapper OBJECT_MAPPER = ParallelIndexTestingFactory.createObjectMapper();
  private static final ParallelIndexIngestionSpec INGESTION_SPEC = ParallelIndexTestingFactory.createIngestionSpec(
      new LocalInputSource(new File("baseDir"), "filer"),
      new JsonInputFormat(null, null),
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
}
