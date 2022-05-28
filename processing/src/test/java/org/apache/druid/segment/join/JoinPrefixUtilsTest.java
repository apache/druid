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

package org.apache.druid.segment.join;

import org.apache.druid.segment.column.ColumnHolder;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class JoinPrefixUtilsTest
{
  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Test
  public void test_validatePrefix_null()
  {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("Join clause cannot have null or empty prefix");

    JoinPrefixUtils.validatePrefix(null);
  }

  @Test
  public void test_validatePrefix_empty()
  {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("Join clause cannot have null or empty prefix");

    JoinPrefixUtils.validatePrefix("");
  }

  @Test
  public void test_validatePrefix_underscore()
  {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("Join clause cannot have prefix[_]");

    JoinPrefixUtils.validatePrefix("_");
  }

  @Test
  public void test_validatePrefix_timeColumn()
  {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("Join clause cannot have prefix[__time]");

    JoinPrefixUtils.validatePrefix(ColumnHolder.TIME_COLUMN_NAME);
  }

  @Test
  public void test_isPrefixedBy()
  {
    Assert.assertTrue(JoinPrefixUtils.isPrefixedBy("foo", ""));
    Assert.assertTrue(JoinPrefixUtils.isPrefixedBy("foo", "f"));
    Assert.assertTrue(JoinPrefixUtils.isPrefixedBy("foo", "fo"));
    Assert.assertFalse(JoinPrefixUtils.isPrefixedBy("foo", "foo"));
  }
}
