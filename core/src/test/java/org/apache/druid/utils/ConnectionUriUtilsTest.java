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

package org.apache.druid.utils;

import com.google.common.collect.ImmutableSet;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

@RunWith(Enclosed.class)
public class ConnectionUriUtilsTest
{
  public static class ThrowIfURLHasNotAllowedPropertiesTest
  {
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void testEmptyActualProperties()
    {
      ConnectionUriUtils.throwIfPropertiesAreNotAllowed(
          ImmutableSet.of(),
          ImmutableSet.of("valid_key1", "valid_key2"),
          ImmutableSet.of("system_key1", "system_key2")
      );
    }

    @Test
    public void testThrowForNonAllowedProperties()
    {
      expectedException.expect(IllegalArgumentException.class);
      expectedException.expectMessage("The property [invalid_key] is not in the allowed list [valid_key1, valid_key2]");

      ConnectionUriUtils.throwIfPropertiesAreNotAllowed(
          ImmutableSet.of("valid_key1", "invalid_key"),
          ImmutableSet.of("system_key1", "system_key2"),
          ImmutableSet.of("valid_key1", "valid_key2")
      );
    }

    @Test
    public void testAllowedProperties()
    {
      ConnectionUriUtils.throwIfPropertiesAreNotAllowed(
          ImmutableSet.of("valid_key2"),
          ImmutableSet.of("system_key1", "system_key2"),
          ImmutableSet.of("valid_key1", "valid_key2")
      );
    }

    @Test
    public void testAllowSystemProperties()
    {
      ConnectionUriUtils.throwIfPropertiesAreNotAllowed(
          ImmutableSet.of("system_key1", "valid_key2"),
          ImmutableSet.of("system_key1", "system_key2"),
          ImmutableSet.of("valid_key1", "valid_key2")
      );
    }

    @Test
    public void testMatchSystemProperties()
    {
      ConnectionUriUtils.throwIfPropertiesAreNotAllowed(
          ImmutableSet.of("system_key1.1", "system_key1.5", "system_key11.11", "valid_key2"),
          ImmutableSet.of("system_key1", "system_key2"),
          ImmutableSet.of("valid_key1", "valid_key2")
      );
    }
  }
}
