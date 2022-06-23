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

package org.apache.druid.query.scan;

import org.apache.druid.java.util.common.Pair;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.queryng.fragment.FragmentBuilderFactory;
import org.apache.druid.queryng.fragment.TestFragmentBuilderFactory;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Version of {@link ScanQueryRunnerTest} that enables the "Query NG" operator-based
 * scan query implementation.
 */
@RunWith(Parameterized.class)
public class ScanQueryRunnerWithOperatorsTest extends ScanQueryRunnerTest
{
  public ScanQueryRunnerWithOperatorsTest(QueryRunner<ScanResultValue> runner, boolean legacy)
  {
    super(runner, legacy);
  }

  private FragmentBuilderFactory fragmentBuilderFactory = new TestFragmentBuilderFactory(true);

  @Override
  protected Pair<QueryPlus<ScanResultValue>, ResponseContext> queryPlusPlus(ScanQuery query)
  {
    ResponseContext responseContext = ResponseContext.createEmpty();
    QueryPlus<ScanResultValue> queryPlus = QueryPlus
        .wrap(query)
        .withFragmentBuilder(fragmentBuilderFactory.create(query, responseContext));
    return Pair.of(queryPlus, responseContext);
  }
}
