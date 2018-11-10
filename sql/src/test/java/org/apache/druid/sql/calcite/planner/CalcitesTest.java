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

package org.apache.druid.sql.calcite.planner;

import com.google.common.collect.ImmutableSortedSet;
import org.apache.druid.sql.calcite.util.CalciteTestBase;
import org.junit.Assert;
import org.junit.Test;

public class CalcitesTest extends CalciteTestBase
{
  @Test
  public void testEscapeStringLiteral()
  {
    Assert.assertEquals("''", Calcites.escapeStringLiteral(""));
    Assert.assertEquals("'foo'", Calcites.escapeStringLiteral("foo"));
    Assert.assertEquals("'foo bar'", Calcites.escapeStringLiteral("foo bar"));
    Assert.assertEquals("U&'foö bar'", Calcites.escapeStringLiteral("foö bar"));
    Assert.assertEquals("U&'foo \\0026\\0026 bar'", Calcites.escapeStringLiteral("foo && bar"));
    Assert.assertEquals("U&'foo \\005C bar'", Calcites.escapeStringLiteral("foo \\ bar"));
    Assert.assertEquals("U&'foo\\0027s bar'", Calcites.escapeStringLiteral("foo's bar"));
    Assert.assertEquals("U&'друид'", Calcites.escapeStringLiteral("друид"));
  }

  @Test
  public void testFindUnusedPrefix()
  {
    Assert.assertEquals("x", Calcites.findUnusedPrefix("x", ImmutableSortedSet.of("foo", "bar")));
    Assert.assertEquals("x", Calcites.findUnusedPrefix("x", ImmutableSortedSet.of("foo", "bar", "x")));
    Assert.assertEquals("_x", Calcites.findUnusedPrefix("x", ImmutableSortedSet.of("foo", "bar", "x0")));
    Assert.assertEquals("_x", Calcites.findUnusedPrefix("x", ImmutableSortedSet.of("foo", "bar", "x4")));
    Assert.assertEquals("__x", Calcites.findUnusedPrefix("x", ImmutableSortedSet.of("foo", "xa", "_x2xx", "x0")));
    Assert.assertEquals("x", Calcites.findUnusedPrefix("x", ImmutableSortedSet.of("foo", "xa", "_x2xx", " x")));
    Assert.assertEquals("x", Calcites.findUnusedPrefix("x", ImmutableSortedSet.of("foo", "_xbxx")));
    Assert.assertEquals("x", Calcites.findUnusedPrefix("x", ImmutableSortedSet.of("foo", "xa", "_x")));
    Assert.assertEquals("__x", Calcites.findUnusedPrefix("x", ImmutableSortedSet.of("foo", "x1a", "_x90")));
  }
}
