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

package org.apache.druid.segment;

import org.apache.druid.common.config.NullHandling;

public class TestNullableFloatColumnSelector extends TestFloatColumnSelector
{

  private final Float[] floats;

  static {
    NullHandling.initializeForTests();
  }

  private int index = 0;

  public TestNullableFloatColumnSelector(Float[] floats)
  {
    this.floats = floats;
  }

  @Override
  public float getFloat()
  {
    if (floats[index] != null) {
      return floats[index];
    } else if (NullHandling.replaceWithDefault()) {
      return NullHandling.ZERO_FLOAT;
    } else {
      throw new IllegalStateException("Should never be invoked when current value is null && SQL-compatible null handling is enabled!");
    }
  }

  @Override
  public boolean isNull()
  {
    return !NullHandling.replaceWithDefault() && floats[index] == null;
  }

  public void increment()
  {
    ++index;
  }
}
