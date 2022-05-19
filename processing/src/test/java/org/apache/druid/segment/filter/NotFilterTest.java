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

package org.apache.druid.segment.filter;

import com.google.common.collect.ImmutableMap;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.filter.ExpressionDimFilter;
import org.apache.druid.query.filter.Filter;
import org.junit.Assert;
import org.junit.Test;

public class NotFilterTest
{
  @Test
  public void testEquals()
  {
    EqualsVerifier.forClass(NotFilter.class).usingGetClass().withNonnullFields("baseFilter").verify();
  }

  @Test
  public void testHashCodeCompareWithBaseFilter()
  {
    final Filter baseFilter = FilterTestUtils.selector("col1", "1");
    final Filter notFilter = FilterTestUtils.not(baseFilter);
    Assert.assertNotEquals(notFilter.hashCode(), baseFilter.hashCode());
  }

  @Test
  public void testRequiredColumnRewrite()
  {
    Filter filter = new NotFilter(new SelectorFilter("dim0", "B"));
    Filter filter2 = new NotFilter(new SelectorFilter("dim1", "B"));

    Assert.assertTrue(filter.supportsRequiredColumnRewrite());
    Assert.assertTrue(filter2.supportsRequiredColumnRewrite());

    Filter rewrittenFilter = filter.rewriteRequiredColumns(ImmutableMap.of("dim0", "dim1"));
    Assert.assertEquals(filter2, rewrittenFilter);

    Filter filter3 = new NotFilter(new ExpressionDimFilter("dim0 == 'B'", ExprMacroTable.nil()).toFilter());
    Assert.assertFalse(filter3.supportsRequiredColumnRewrite());
  }

}
