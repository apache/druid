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

import com.google.common.base.Preconditions;
import it.unimi.dsi.fastutil.ints.AbstractIntList;
import it.unimi.dsi.fastutil.ints.IntList;

public class IntListUtils
{
  private IntListUtils()
  {
  }

  public static IntList fromTo(int from, int to)
  {
    Preconditions.checkArgument(from <= to);
    return new RangeIntList(from, to);
  }

  private static final class RangeIntList extends AbstractIntList
  {
    private final int start;
    private final int size;

    RangeIntList(int start, int end)
    {
      this.start = start;
      this.size = end - start;
    }

    @Override
    public int getInt(int index)
    {
      Preconditions.checkElementIndex(index, size);
      return start + index;
    }

    @Override
    public int size()
    {
      return size;
    }
  }
}
