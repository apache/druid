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

package org.apache.druid.testsEx.msq;

import org.apache.curator.shaded.com.google.common.collect.ImmutableMap;
import org.apache.druid.testsEx.categories.MultiStageQuery;
import org.apache.druid.testsEx.config.DruidTestRunner;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.File;
import java.util.Arrays;

@RunWith(DruidTestRunner.class)
@Category(MultiStageQuery.class)
public class ITSQLBasedBatchIngestion extends AbstractITSQLBasedBatchIngestion
{
  private static final String BATCH_INDEX_TASKS_DIR = "/multi-stage-query/batch-index/";

  @Test
  public void testSQLBasedBatchIngestion() throws Exception
  {
    // Get list of all directories in batch-index folder. Each folder is considered a test case.
    File[] directories = (new File(getClass().getResource(BATCH_INDEX_TASKS_DIR).toURI())).listFiles(File::isDirectory);
    int fail_count = 0;
    LOG.info("Test will be run for sql in the following directories - \n %s", Arrays.toString(directories));
    for (File dir : directories) {
      try {
        runMSQTaskandTestQueries(BATCH_INDEX_TASKS_DIR + dir.getName(), "msqBatchIndex_" + dir.getName(),
                                 ImmutableMap.of("finalizeAggregations", false,
                                                 "maxNumTasks", 10,
                                                 "groupByEnableMultiValueUnnesting", false
                                 ));
      }
      catch (Exception e) {
        LOG.error(e, "Error while testing %s", dir.getName());
        fail_count++;
      }
    }
    if (fail_count > 0) {
      LOG.error("%s tests were run out of which %s FAILED", directories.length, fail_count);
      throw new RuntimeException();
    }
  }
}
