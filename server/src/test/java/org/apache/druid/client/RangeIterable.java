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

package org.apache.druid.client;

import java.util.Iterator;

/**
 */
public class RangeIterable implements Iterable<Integer>
{
  private final int end;
  private final int start;
  private final int increment;

  public RangeIterable(
      int end
  )
  {
    this(0, end);
  }

  public RangeIterable(
      int start,
      int end
  )
  {
    this(start, end, 1);
  }

  public RangeIterable(
      int start,
      int end,
      final int i
  )
  {
    this.start = start;
    this.end = end;
    this.increment = i;
  }

  @Override
  public Iterator<Integer> iterator()
  {
    return new Iterator<Integer>()
    {
      private int curr = start;

      @Override
      public boolean hasNext()
      {
        return curr < end;
      }

      @Override
      public Integer next()
      {
        try {
          return curr;
        }
        finally {
          curr += increment;
        }
      }

      @Override
      public void remove()
      {
        throw new UnsupportedOperationException();
      }
    };
  }
}
