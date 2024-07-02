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

package org.apache.druid.query.aggregation.histogram;

import org.apache.druid.error.DruidException;
import org.apache.druid.query.aggregation.BasicComplexMetricSerde;
import org.apache.druid.segment.data.ObjectStrategy;
import java.nio.ByteBuffer;

public class HistogramSerde2 extends BasicComplexMetricSerde<Histogram>
{
  public HistogramSerde2()
  {
    super(Histogram.TYPE, new HistogramObjectStrategy());
  }

  static class HistogramObjectStrategy implements ObjectStrategy<Histogram>
  {

    @Override
    public int compare(Histogram o1, Histogram o2)
    {
      return 0;
    }

    @Override
    public Class<? extends Histogram> getClazz()
    {
      return Histogram.class;
    }

    @Override
    public Histogram fromByteBuffer(ByteBuffer buffer, int numBytes)
    {
      throw DruidException.defensive("not supported");
    }

    @Override
    public byte[] toBytes(Histogram val)
    {
      throw DruidException.defensive("not supported");
    }
  }
}
