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

package io.druid.query;

import com.google.common.collect.Ordering;
import com.google.common.primitives.Longs;
import io.druid.java.util.common.granularity.Granularity;

import java.util.Comparator;

/**
 */
public class ResultGranularTimestampComparator<T> implements Comparator<Result<T>>
{
  private final Granularity gran;

  public ResultGranularTimestampComparator(Granularity granularity)
  {
    this.gran = granularity;
  }

  @Override
  public int compare(Result<T> r1, Result<T> r2)
  {
    return Longs.compare(
        gran.bucketStart(r1.getTimestamp()).getMillis(),
        gran.bucketStart(r2.getTimestamp()).getMillis()
    );
  }

  public static <T> Ordering<Result<T>> create(Granularity granularity, boolean descending)
  {
    Comparator<Result<T>> comparator = new ResultGranularTimestampComparator<>(granularity);
    return descending ? Ordering.from(comparator).reverse() : Ordering.from(comparator);
  }
}
