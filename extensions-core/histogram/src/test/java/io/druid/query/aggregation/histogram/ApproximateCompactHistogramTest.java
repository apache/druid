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

package io.druid.query.aggregation.histogram;

import org.junit.Ignore;

/**
 */
public class ApproximateCompactHistogramTest extends ApproximateHistogramTest
{
  @Override
  protected ApproximateHistogramHolder buildHistogram(int size)
  {
    return new ApproximateCompactHistogram(size);
  }

  @Override
  protected ApproximateHistogramHolder buildHistogram(int size, float[] values, float lowerLimit, float upperLimit)
  {
    ApproximateHistogramHolder h = new ApproximateCompactHistogram(size, lowerLimit, upperLimit);
    for (float v : values) {
      h.offer(v);
    }
    return h;
  }

  @Override
  protected ApproximateHistogramHolder buildHistogram(int binCount, float[] positions, long[] bins, float min, float max)
  {
    return new ApproximateCompactHistogram(binCount, positions, bins, min, max);
  }

  protected ApproximateHistogramHolder buildHistogram(byte[] buffer)
  {
    return new ApproximateCompactHistogram().fromBytes(buffer);
  }

  @Ignore
  @Override
  public void testSerializeDense()
  {
  }

  @Ignore
  @Override
  public void testSerializeSparse()
  {
  }

  @Ignore
  @Override
  public void testSerializeCompact()
  {
  }
}
