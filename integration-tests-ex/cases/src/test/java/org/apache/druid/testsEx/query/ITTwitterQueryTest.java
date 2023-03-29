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

package org.apache.druid.testsEx.query;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.inject.Inject;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.testing.clients.CoordinatorResourceTestClient;
import org.apache.druid.testing.utils.TestQueryHelper;
import org.apache.druid.testsEx.categories.Query;
import org.apache.druid.testsEx.config.DruidTestRunner;
import org.apache.druid.testsEx.indexer.AbstractITBatchIndexTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.function.Function;

@RunWith(DruidTestRunner.class)
@Category(Query.class)
public class ITTwitterQueryTest extends AbstractITBatchIndexTest
{
  private static final String TWITTER_INDEX_TASK = "/queries/twitterstream_index_task.json";
  private static final String TWITTER_DATA_SOURCE = "twitterstream";
  private static final String TWITTER_QUERIES_RESOURCE = "/queries/twitterstream_queries.json";
  @Inject
  CoordinatorResourceTestClient coordinatorClient;
  @Inject
  private TestQueryHelper queryHelper;

  @Before
  public void before() throws IOException
  {
    final Function<String, String> transform = spec -> {
      try {
        return StringUtils.replace(
            spec,
            "%%LOCAL_FILE_NAME%%",
            jsonMapper.writeValueAsString(TWITTER_DATA_SOURCE + ".csv")
        );
      }
      catch (JsonProcessingException e) {
        throw new RuntimeException(e);
      }
    };

    // Ingesting twitterstream data
    doIndexTest(
        TWITTER_DATA_SOURCE,
        TWITTER_INDEX_TASK,
        transform,
        null,
        true,
        false,
        true,
        new Pair<>(false, false)
    );
  }

  @Test
  public void testTwitterQueriesFromFile() throws Exception
  {
    queryHelper.testQueriesFromFile(TWITTER_QUERIES_RESOURCE);
  }

}
