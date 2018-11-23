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

package org.apache.druid.query.groupby.having;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.data.input.MapBasedRow;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.segment.column.ValueType;
import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.internal.matchers.ThrowableMessageMatcher;
import org.junit.rules.ExpectedException;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class DimFilterHavingSpecTest
{
  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Test
  public void testSimple()
  {
    final DimFilterHavingSpec havingSpec = new DimFilterHavingSpec(new SelectorDimFilter("foo", "bar", null), null);
    havingSpec.setRowSignature(null);

    Assert.assertTrue(havingSpec.eval(new MapBasedRow(0, ImmutableMap.of("foo", "bar"))));
    Assert.assertFalse(havingSpec.eval(new MapBasedRow(0, ImmutableMap.of("foo", "baz"))));
  }

  @Test
  public void testRowSignature()
  {
    final DimFilterHavingSpec havingSpec = new DimFilterHavingSpec(new SelectorDimFilter("foo", "1", null), null);
    havingSpec.setRowSignature(ImmutableMap.of("foo", ValueType.LONG));

    Assert.assertTrue(havingSpec.eval(new MapBasedRow(0, ImmutableMap.of("foo", 1L))));
    Assert.assertFalse(havingSpec.eval(new MapBasedRow(0, ImmutableMap.of("foo", 2L))));
  }

  @Test(timeout = 60_000L)
  @Ignore // Doesn't always pass. The check in "eval" is best effort and not guaranteed to detect concurrent usage.
  public void testConcurrentUsage() throws Exception
  {
    final ExecutorService exec = Executors.newFixedThreadPool(2);
    final DimFilterHavingSpec havingSpec = new DimFilterHavingSpec(new SelectorDimFilter("foo", "1", null), null);
    final List<Future<?>> futures = new ArrayList<>();

    for (int i = 0; i < 2; i++) {
      final MapBasedRow row = new MapBasedRow(0, ImmutableMap.of("foo", String.valueOf(i)));
      futures.add(
          exec.submit(
              new Runnable()
              {
                @Override
                public void run()
                {
                  havingSpec.setRowSignature(null);
                  while (!Thread.interrupted()) {
                    havingSpec.eval(row);
                  }
                }
              }
          )
      );
    }

    expectedException.expect(ExecutionException.class);
    expectedException.expectCause(CoreMatchers.<IllegalStateException>instanceOf(IllegalStateException.class));
    expectedException.expectCause(
        ThrowableMessageMatcher.hasMessage(CoreMatchers.containsString("concurrent 'eval' calls not permitted"))
    );

    try {
      for (Future<?> future : futures) {
        future.get();
      }
    }
    finally {
      exec.shutdownNow();
    }

    // Not reached
    Assert.assertTrue(false);
  }

  @Test
  public void testSerde() throws Exception
  {
    final DimFilterHavingSpec havingSpec = new DimFilterHavingSpec(new SelectorDimFilter("foo", "1", null), false);
    final ObjectMapper objectMapper = new DefaultObjectMapper();
    Assert.assertEquals(
        havingSpec,
        objectMapper.readValue(objectMapper.writeValueAsBytes(havingSpec), HavingSpec.class)
    );
  }
}
