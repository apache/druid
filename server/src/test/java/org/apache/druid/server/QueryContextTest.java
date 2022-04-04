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

package org.apache.druid.server;

import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Test;

public class QueryContextTest
{
  @Test
  public void testEmptyParam()
  {
    final QueryContext context = new QueryContext();
    Assert.assertEquals(ImmutableMap.of(), context.getMergedParams());
  }

  @Test
  public void testGetString()
  {
    final QueryContext context = new QueryContext(
        ImmutableMap.of("key", "val")
    );

    Assert.assertEquals("val", context.get("key"));
    Assert.assertEquals("val", context.getAsString("key"));
  }

  @Test
  public void testGetBoolean()
  {
    final QueryContext context = new QueryContext(
        ImmutableMap.of(
            "key1", "true",
            "key2", true
        )
    );

    Assert.assertTrue(context.getAsBoolean("key1", false));
    Assert.assertTrue(context.getAsBoolean("key2", false));
  }

  @Test
  public void testGetInt()
  {
    final QueryContext context = new QueryContext(
        ImmutableMap.of(
            "key1", "100",
            "key2", 100
        )
    );

    Assert.assertEquals(100, context.getAsInt("key1", 0));
    Assert.assertEquals(100, context.getAsInt("key2", 0));
  }

  @Test
  public void testAddSystemParamOverrideUserParam()
  {
    final QueryContext context = new QueryContext(
        ImmutableMap.of(
            "user1", "userVal1",
            "conflict", "userVal2"
        )
    );
    context.addSystemParam("sys1", "sysVal1");
    context.addSystemParam("conflict", "sysVal2");

    Assert.assertEquals(
        ImmutableMap.of(
            "user1", "userVal1",
            "conflict", "userVal2"
        ),
        context.getUserParams()
    );

    Assert.assertEquals(
        ImmutableMap.of(
            "user1", "userVal1",
            "sys1", "sysVal1",
            "conflict", "sysVal2"
        ),
        context.getMergedParams()
    );
  }

  @Test
  public void testUserParamOverrideDefaultParam()
  {
    final QueryContext context = new QueryContext(
        ImmutableMap.of(
            "user1", "userVal1",
            "conflict", "userVal2"
        )
    );
    context.addDefaultParams(
        ImmutableMap.of(
            "default1", "defaultVal1"
        )
    );
    context.addDefaultParam("conflict", "defaultVal2");

    Assert.assertEquals(
        ImmutableMap.of(
            "user1", "userVal1",
            "conflict", "userVal2"
        ),
        context.getUserParams()
    );

    Assert.assertEquals(
        ImmutableMap.of(
            "user1", "userVal1",
            "default1", "defaultVal1",
            "conflict", "userVal2"
        ),
        context.getMergedParams()
    );
  }

  @Test
  public void testRemoveUserParam()
  {
    final QueryContext context = new QueryContext(
        ImmutableMap.of(
            "user1", "userVal1",
            "conflict", "userVal2"
        )
    );
    context.addDefaultParams(
        ImmutableMap.of(
            "default1", "defaultVal1",
            "conflict", "defaultVal2"
        )
    );

    Assert.assertEquals(
        ImmutableMap.of(
            "user1", "userVal1",
            "default1", "defaultVal1",
            "conflict", "userVal2"
        ),
        context.getMergedParams()
    );
    Assert.assertEquals("userVal2", context.removeUserParam("conflict"));
    Assert.assertEquals(
        ImmutableMap.of(
            "user1", "userVal1",
            "default1", "defaultVal1",
            "conflict", "defaultVal2"
        ),
        context.getMergedParams()
    );
  }
}
