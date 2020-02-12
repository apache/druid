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

package org.apache.druid.sql.calcite.aggregation;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.TypeLiteral;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Set;

public class SqlAggregationModuleTest
{
  private SqlAggregationModule target;
  private Injector injector;

  @Before
  public void setUp()
  {
    target = new SqlAggregationModule();
    injector = Guice.createInjector(target);
  }

  @Test
  public void testEmptySqlAggregatorsAreBound()
  {
    Set<SqlAggregator> sqlAggregators = injector.getInstance(Key.get(new TypeLiteral<Set<SqlAggregator>>(){}));
    Assert.assertNotNull(sqlAggregators);
    Assert.assertTrue(sqlAggregators.isEmpty());
  }
}
