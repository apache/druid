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

package org.apache.druid.cli;

import com.google.common.collect.ImmutableList;
import com.google.inject.Injector;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.query.DirectQueryProcessingPool;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryRunnerFactory;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import java.util.Collections;

public class DumpSegmentTest
{
  @Test
  public void testExecuteQuery()
  {
    Injector injector = Mockito.mock(Injector.class);
    QueryRunnerFactoryConglomerate conglomerate = Mockito.mock(QueryRunnerFactoryConglomerate.class);
    QueryRunnerFactory factory = Mockito.mock(QueryRunnerFactory.class, Mockito.RETURNS_DEEP_STUBS);
    QueryRunner runner = Mockito.mock(QueryRunner.class);
    QueryRunner mergeRunner = Mockito.mock(QueryRunner.class);
    Query query = Mockito.mock(Query.class);
    Sequence expected = Sequences.simple(Collections.singletonList(123));
    Mockito.when(injector.getInstance(QueryRunnerFactoryConglomerate.class)).thenReturn(conglomerate);
    Mockito.when(conglomerate.findFactory(ArgumentMatchers.any())).thenReturn(factory);
    Mockito.when(factory.createRunner(ArgumentMatchers.any())).thenReturn(runner);
    Mockito.when(factory.getToolchest().mergeResults(factory.mergeRunners(DirectQueryProcessingPool.INSTANCE, ImmutableList.of(runner)))).thenReturn(mergeRunner);
    Mockito.when(mergeRunner.run(ArgumentMatchers.any(), ArgumentMatchers.any())).thenReturn(expected);
    Sequence actual = DumpSegment.executeQuery(injector, null, query);
    Assert.assertSame(expected, actual);
  }
}
