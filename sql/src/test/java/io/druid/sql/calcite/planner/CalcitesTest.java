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

package io.druid.sql.calcite.planner;

import org.junit.Assert;
import org.junit.Test;

public class CalcitesTest
{
  @Test
  public void testEscapeStringLiteral()
  {
    Assert.assertEquals("''", Calcites.escapeStringLiteral(null));
    Assert.assertEquals("''", Calcites.escapeStringLiteral(""));
    Assert.assertEquals("'foo'", Calcites.escapeStringLiteral("foo"));
    Assert.assertEquals("'foo bar'", Calcites.escapeStringLiteral("foo bar"));
    Assert.assertEquals("U&'foö bar'", Calcites.escapeStringLiteral("foö bar"));
    Assert.assertEquals("U&'foo \\0026\\0026 bar'", Calcites.escapeStringLiteral("foo && bar"));
    Assert.assertEquals("U&'foo \\005C bar'", Calcites.escapeStringLiteral("foo \\ bar"));
    Assert.assertEquals("U&'foo\\0027s bar'", Calcites.escapeStringLiteral("foo's bar"));
    Assert.assertEquals("U&'друид'", Calcites.escapeStringLiteral("друид"));
  }
}
