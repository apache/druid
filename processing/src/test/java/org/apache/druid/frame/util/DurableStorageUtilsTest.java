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

package org.apache.druid.frame.util;

import com.google.common.collect.ImmutableSet;
import org.junit.Assert;
import org.junit.Test;

public class DurableStorageUtilsTest
{

  private static final String CONTROLLER_ID = "controller_id_1";
  private static final String TASK_ID = "task_id_1";

  private static final int WORKER_NUMBER = 2;

  private static final int STAGE_NUMBER = 1;

  private static final int PARTITION_NUMBER = 3;


  @Test
  public void getNextDirNameWithPrefixFromPath()
  {
    Assert.assertEquals("", DurableStorageUtils.getNextDirNameWithPrefixFromPath("/123/123"));
    Assert.assertEquals("123", DurableStorageUtils.getNextDirNameWithPrefixFromPath("123"));
    Assert.assertEquals(
        "controller_query_123",
        DurableStorageUtils.getNextDirNameWithPrefixFromPath("controller_query_123/123")
    );
    Assert.assertEquals("", DurableStorageUtils.getNextDirNameWithPrefixFromPath(""));
    Assert.assertNull(DurableStorageUtils.getNextDirNameWithPrefixFromPath(null));
  }

  @Test
  public void isQueryResultFileActive()
  {

    Assert.assertTrue(DurableStorageUtils.isQueryResultFileActive(
        DurableStorageUtils.QUERY_RESULTS_DIR + "/123/result",
        ImmutableSet.of("123")
    ));
    Assert.assertFalse(DurableStorageUtils.isQueryResultFileActive(
        DurableStorageUtils.QUERY_RESULTS_DIR + "/123/result",
        ImmutableSet.of("")
    ));
    Assert.assertFalse(DurableStorageUtils.isQueryResultFileActive(
        DurableStorageUtils.QUERY_RESULTS_DIR + "/",
        ImmutableSet.of("123")
    ));
    Assert.assertFalse(DurableStorageUtils.isQueryResultFileActive(
        null,
        ImmutableSet.of("123")
    ));
    Assert.assertFalse(DurableStorageUtils.isQueryResultFileActive(
        DurableStorageUtils.QUERY_RESULTS_DIR,
        ImmutableSet.of("123")
    ));
  }

  @Test
  public void sanityTest()
  {

    String baseString = "controller_" + CONTROLLER_ID + "/stage_" + STAGE_NUMBER + "/worker_" + WORKER_NUMBER + "/";

    Assert.assertEquals(
        baseString + "__success",
        DurableStorageUtils.getWorkerOutputSuccessFilePath(CONTROLLER_ID, STAGE_NUMBER, WORKER_NUMBER)
    );
    Assert.assertEquals(
        DurableStorageUtils.QUERY_RESULTS_DIR + "/" + baseString + "__success",
        DurableStorageUtils.getQueryResultsSuccessFilePath(CONTROLLER_ID, STAGE_NUMBER, WORKER_NUMBER)
    );


    Assert.assertEquals(
        baseString + "taskId_" + TASK_ID,
        DurableStorageUtils.getTaskIdOutputsFolderName(
            CONTROLLER_ID,
            STAGE_NUMBER,
            WORKER_NUMBER,
            TASK_ID
        )
    );
    Assert.assertEquals(
        DurableStorageUtils.QUERY_RESULTS_DIR + "/" + baseString + "taskId_" + TASK_ID,
        DurableStorageUtils.getQueryResultsForTaskIdFolderName(
            CONTROLLER_ID,
            STAGE_NUMBER,
            WORKER_NUMBER,
            TASK_ID
        )
    );


    Assert.assertEquals(
        baseString + "taskId_" + TASK_ID + "/part_3",
        DurableStorageUtils.getPartitionOutputsFileNameWithPathForPartition(
            CONTROLLER_ID,
            STAGE_NUMBER,
            WORKER_NUMBER,
            TASK_ID,
            PARTITION_NUMBER
        )
    );
    Assert.assertEquals(
        DurableStorageUtils.QUERY_RESULTS_DIR + "/" + baseString + "taskId_" + TASK_ID + "/part_3",
        DurableStorageUtils.getQueryResultsFileNameWithPathForPartition(
            CONTROLLER_ID,
            STAGE_NUMBER,
            WORKER_NUMBER,
            TASK_ID,
            PARTITION_NUMBER
        )
    );

  }

}
