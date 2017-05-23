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

package io.druid.query;

import com.google.common.collect.ImmutableList;
import io.druid.java.util.common.guava.Sequence;
import io.druid.java.util.common.guava.Sequences;
import io.druid.query.spec.MultipleIntervalSegmentSpec;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * Tests that if a QueryRunner overrides a legacy {@link QueryRunner#run(Query, Map)} method, it still works. This
 * test should be removed when {@link QueryRunner#run(Query, Map)} is removed.
 */
public class LegacyApiQueryRunnerTest
{
  private static class LegacyApiQueryRunner<T> implements QueryRunner<T>
  {
    /**
     * Overrides legacy API.
     */
    @Override
    public Sequence<T> run(Query<T> query, Map<String, Object> responseContext)
    {
      return Sequences.empty();
    }
  }

  @Test
  public void testQueryRunnerLegacyApi()
  {
    final Query query = new TestQuery(
        new TableDataSource("test"),
        new MultipleIntervalSegmentSpec(ImmutableList.of(new Interval("0/100"))),
        false,
        new HashMap()
    );

    Map<String, Object> context = new HashMap<>();
    Assert.assertEquals(Sequences.empty(), new LegacyApiQueryRunner<>().run(QueryPlus.wrap(query), context));
  }
}
