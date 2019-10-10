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

package org.apache.druid.query.search;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryConfig;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.QueryContexts.Vectorize;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.TestQuery;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.segment.QueryableIndexStorageAdapter;
import org.apache.druid.segment.TestHelper;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class QueryConfigTest
{
  @Test
  public void testSerde() throws IOException
  {
    final ObjectMapper mapper = TestHelper.makeJsonMapper();
    final String json = "{"
                        + "\"vectorize\" : \"force\","
                        + "\"vectorSize\" : 1"
                        + "}";
    final QueryConfig config = mapper.readValue(json, QueryConfig.class);
    Assert.assertEquals(Vectorize.FORCE, config.getVectorize());
    Assert.assertEquals(1, config.getVectorSize());
  }

  @Test
  public void testDefault()
  {
    final QueryConfig config = new QueryConfig();
    Assert.assertEquals(QueryContexts.DEFAULT_VECTORIZE, config.getVectorize());
    Assert.assertEquals(QueryableIndexStorageAdapter.DEFAULT_VECTOR_SIZE, config.getVectorSize());
  }

  @Test
  public void testOverrides()
  {
    final Query<?> query = new TestQuery(
        new TableDataSource("datasource"),
        new MultipleIntervalSegmentSpec(ImmutableList.of()),
        false,
        ImmutableMap.of(
            QueryContexts.VECTORIZE_KEY,
            "true",
            QueryContexts.VECTOR_SIZE_KEY,
            QueryableIndexStorageAdapter.DEFAULT_VECTOR_SIZE * 2
        )
    );
    final QueryConfig config = new QueryConfig().withOverrides(query);
    Assert.assertEquals(Vectorize.TRUE, config.getVectorize());
    Assert.assertEquals(QueryableIndexStorageAdapter.DEFAULT_VECTOR_SIZE * 2, config.getVectorSize());
  }
}
