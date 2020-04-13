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

package org.apache.druid.query.aggregation.mean;

import org.apache.druid.java.util.common.Numbers;
import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.segment.ColumnValueSelector;

import java.util.List;

/**
 */
public class DoubleMeanAggregator implements Aggregator
{
  private final ColumnValueSelector selector;

  private final DoubleMeanHolder value = new DoubleMeanHolder(0, 0);

  public DoubleMeanAggregator(ColumnValueSelector selector)
  {
    this.selector = selector;
  }

  @Override
  public void aggregate()
  {
    Object update = selector.getObject();

    if (update instanceof DoubleMeanHolder) {
      value.update((DoubleMeanHolder) update);
    } else if (update instanceof List) {
      for (Object o : (List) update) {
        value.update(Numbers.tryParseDouble(o, 0));
      }
    } else {
      value.update(Numbers.tryParseDouble(update, 0));
    }
  }

  @Override
  public Object get()
  {
    return value;
  }

  @Override
  public float getFloat()
  {
    throw new UnsupportedOperationException("not supported");
  }

  @Override
  public long getLong()
  {
    throw new UnsupportedOperationException("not supported");
  }

  @Override
  public double getDouble()
  {
    throw new UnsupportedOperationException("not supported");
  }

  @Override
  public void close()
  {
    // no resources to cleanup
  }
}
