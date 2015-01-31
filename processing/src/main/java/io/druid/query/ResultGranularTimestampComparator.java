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

package io.druid.query;

import com.google.common.primitives.Longs;
import io.druid.granularity.QueryGranularity;

import java.util.Comparator;

/**
 */
public class ResultGranularTimestampComparator<T> implements Comparator<Result<T>>
{
  private final QueryGranularity gran;

  public ResultGranularTimestampComparator(QueryGranularity granularity)
  {
    this.gran = granularity;
  }

  @Override
  public int compare(Result<T> r1, Result<T> r2)
  {
    return Longs.compare(
        gran.truncate(r1.getTimestamp().getMillis()),
        gran.truncate(r2.getTimestamp().getMillis())
    );
  }
}
