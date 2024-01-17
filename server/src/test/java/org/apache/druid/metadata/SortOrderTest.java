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

package org.apache.druid.metadata;

import org.apache.druid.error.DruidExceptionMatcher;
import org.junit.Assert;
import org.junit.Test;

public class SortOrderTest
{

  @Test
  public void testAsc()
  {
    Assert.assertEquals(SortOrder.ASC, SortOrder.fromValue("asc"));
    Assert.assertEquals("ASC", SortOrder.fromValue("asc").toString());
    Assert.assertEquals(SortOrder.ASC, SortOrder.fromValue("ASC"));
    Assert.assertEquals("ASC", SortOrder.fromValue("ASC").toString());
    Assert.assertEquals(SortOrder.ASC, SortOrder.fromValue("AsC"));
    Assert.assertEquals("ASC", SortOrder.fromValue("AsC").toString());
  }

  @Test
  public void testDesc()
  {
    Assert.assertEquals(SortOrder.DESC, SortOrder.fromValue("desc"));
    Assert.assertEquals("DESC", SortOrder.fromValue("desc").toString());
    Assert.assertEquals(SortOrder.DESC, SortOrder.fromValue("DESC"));
    Assert.assertEquals("DESC", SortOrder.fromValue("DESC").toString());
    Assert.assertEquals(SortOrder.DESC, SortOrder.fromValue("DesC"));
    Assert.assertEquals("DESC", SortOrder.fromValue("DesC").toString());
  }

  @Test
  public void testInvalid()
  {
    DruidExceptionMatcher.invalidInput().expectMessageIs(
        "Unexpected value[bad] for SortOrder. Possible values are: [ASC, DESC]"
    ).assertThrowsAndMatches(
        () -> SortOrder.fromValue("bad")
    );
  }
}
