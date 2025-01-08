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

package org.apache.druid.query.policy;

import com.google.common.collect.ImmutableList;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.filter.EqualityFilter;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.segment.CursorBuildSpec;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.filter.AndFilter;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class RowFilterPolicyTest
{
  @Before
  public void setup()
  {
    NullHandling.initializeForTests();
  }

  @Test
  public void testVisit()
  {
    DimFilter policyFilter = new EqualityFilter("col", ColumnType.STRING, "val", null);
    final RowFilterPolicy policy = RowFilterPolicy.from(policyFilter);

    Assert.assertEquals(policyFilter, policy.visit(CursorBuildSpec.FULL_SCAN).getFilter());
  }

  @Test
  public void testVisit_combineFilters()
  {
    Filter filter = new EqualityFilter("col0", ColumnType.STRING, "val0", null);
    CursorBuildSpec spec = CursorBuildSpec.builder().setFilter(filter).build();

    DimFilter policyFilter = new EqualityFilter("col", ColumnType.STRING, "val", null);
    final RowFilterPolicy policy = RowFilterPolicy.from(policyFilter);

    Filter expected = new AndFilter(ImmutableList.of(policyFilter.toFilter(), filter));
    Assert.assertEquals(expected, policy.visit(spec).getFilter());
  }
}
