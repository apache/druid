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

package org.apache.druid.indexer;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class TaskStatusTest
{
  static final String STACK_TRACE =
      "org.apache.druid.java.util.common.ISE: Lock for interval [2024-04-23T00:00:00.000Z/2024-04-24T00:00:00.000Z] was revoked.\n"
      + "\tat org.apache.druid.indexing.common.task.AbstractBatchIndexTask.tryTimeChunkLock(AbstractBatchIndexTask.java:465) ~[druid-indexing-service-2024.03.0-iap.jar:2024.03.0-iap]\n"
      + "\tat org.apache.druid.indexing.common.task.batch.parallel.PartialHashSegmentGenerateTask.isReady(PartialHashSegmentGenerateTask.java:152) ~[druid-indexing-service-2024.03.0-iap.jar:2024.03.0-iap]\n"
      + "\tat org.apache.druid.indexing.overlord.TaskQueue.manageInternalCritical(TaskQueue.java:420) [druid-indexing-service-2024.03.0-iap.jar:2024.03.0-iap]\n"
      + "\tat org.apache.druid.indexing.overlord.TaskQueue.manageInternal(TaskQueue.java:373) [druid-indexing-service-2024.03.0-iap.jar:2024.03.0-iap]\n"
      + "\tat org.apache.druid.indexing.overlord.TaskQueue.manage(TaskQueue.java:356) [druid-indexing-service-2024.03.0-iap.jar:2024.03.0-iap]\n"
      + "\tat org.apache.druid.indexing.overlord.TaskQueue.access$000(TaskQueue.java:91) [druid-indexing-service-2024.03.0-iap.jar:2024.03.0-iap]\n"
      + "\tat org.apache.druid.indexing.overlord.TaskQueue$1.run(TaskQueue.java:212) [druid-indexing-service-2024.03.0-iap.jar:2024.03.0-iap]\n"
      + "\tat java.base/java.util.concurrent.Executors$RunnableAdapter.call(Executors.java:515) [?:?]\n"
      + "\tat java.base/java.util.concurrent.FutureTask.run(FutureTask.java:264) [?:?]\n"
      + "\tat java.base/java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1128) [?:?]\n"
      + "\tat java.base/java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:628) [?:?]\n"
      + "\tat java.base/java.lang.Thread.run(Thread.java:829) [?:?]\n";

  static final String EXPECTED_ERROR_MESSAGE =
      "org.apache.druid.java.util.common.ISE: Lock for interval [2024-04-23T00:00:00.000Z/2024-04-24T00:00:00.000Z] was revoked.\n"
      + "\tat org.apache.druid.indexing.common.task.AbstractBatchIndexTask.tryTimeChunkLock(AbstractBatchIndexTask.java:465) ~[druid-indexing-service-2024.03.0-iap.jar:2024.03.0-iap]\n"
      + "\tat org.apache.druid.indexing.common.task.batch.parallel.PartialHashSegmentGenerateTask.isReady(PartialHashSegmentGenerateTask.java:152) ~[druid-indexing-service-2024.03.0-iap.jar:2024.03.0-iap]\n"
      + "\tat org.apache.druid.i...584 characters omitted...e$1.run(TaskQueue.java:212) [druid-indexing-service-2024.03.0-iap.jar:2024.03.0-iap]\n"
      + "\tat java.base/java.util.concurrent.Executors$RunnableAdapter.call(Executors.java:515) [?:?]\n"
      + "\tat java.base/java.util.concurrent.FutureTask.run(FutureTask.java:264) [?:?]\n"
      + "\tat java.base/java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1128) [?:?]\n"
      + "\tat java.base/java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:628) [?:?]\n"
      + "\tat java.base/java.lang.Thread.run(Thread.java:829) [?:?]\n";

  @Test
  public void testSerde() throws IOException
  {
    final ObjectMapper mapper = new ObjectMapper();

    final TaskStatus status = new TaskStatus(
        "testId",
        TaskState.RUNNING,
        1000L,
        "an error message",
        TaskLocation.create("testHost", 1010, -1)
    );

    final String json = mapper.writeValueAsString(status);
    Assert.assertEquals(status, mapper.readValue(json, TaskStatus.class));

    final String jsonNoLocation = "{\n"
                                  + "\"id\": \"testId\",\n"
                                  + "\"status\": \"SUCCESS\",\n"
                                  + "\"duration\": 3000,\n"
                                  + "\"errorMsg\": \"hello\"\n"
                                  + "}";

    final TaskStatus statusNoLocation = new TaskStatus(
        "testId",
        TaskState.SUCCESS,
        3000L,
        "hello",
        null
    );
    Assert.assertEquals(statusNoLocation, mapper.readValue(jsonNoLocation, TaskStatus.class));

    TaskStatus success = TaskStatus.success("forkTaskID", TaskLocation.create("localhost", 0, 1));
    Assert.assertEquals(success.getLocation().getHost(), "localhost");
    Assert.assertEquals(success.getLocation().getPort(), 0);
    Assert.assertEquals(success.getLocation().getTlsPort(), 1);
  }

  @Test
  public void testTruncation()
  {
    final TaskStatus status = TaskStatus.failure("testId", STACK_TRACE);
    Assert.assertEquals(status.getErrorMsg(), EXPECTED_ERROR_MESSAGE);
  }
}
