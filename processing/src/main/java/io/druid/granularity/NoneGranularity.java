/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.granularity;

public final class NoneGranularity extends BaseQueryGranularity
{
  @Override
  public long next(long offset)
  {
    return offset + 1;
  }

  @Override
  public long truncate(long offset)
  {
    return offset;
  }

  @Override
  public byte[] cacheKey()
  {
    return new byte[]{0x0};
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    return -1;
  }

  @Override
  public String toString()
  {
    return "NoneGranularity";
  }
}
