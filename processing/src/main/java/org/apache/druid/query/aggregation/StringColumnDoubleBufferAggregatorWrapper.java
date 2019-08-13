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

package org.apache.druid.query.aggregation;

import org.apache.druid.java.util.common.Numbers;
import org.apache.druid.segment.BaseDoubleColumnValueSelector;
import org.apache.druid.segment.BaseObjectColumnValueSelector;
import org.apache.druid.segment.selector.settable.SettableValueDoubleColumnValueSelector;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.function.Function;

/**
 * This class can be used to wrap Double BufferAggregator that consume double type columns to handle String type.
 */
public class StringColumnDoubleBufferAggregatorWrapper extends DelegatingBufferAggregator
{
  private final BaseObjectColumnValueSelector selector;
  private final double nullValue;
  private final SettableValueDoubleColumnValueSelector doubleSelector;

  public StringColumnDoubleBufferAggregatorWrapper(
      BaseObjectColumnValueSelector selector,
      Function<BaseDoubleColumnValueSelector, BufferAggregator> delegateBuilder,
      double nullValue
  )
  {
    this.doubleSelector = new SettableValueDoubleColumnValueSelector();
    this.selector = selector;
    this.nullValue = nullValue;
    this.delegate = delegateBuilder.apply(doubleSelector);
  }

  @Override
  public void aggregate(ByteBuffer buf, int position)
  {
    Object update = selector.getObject();

    if (update == null) {
      doubleSelector.setValue(nullValue);
      delegate.aggregate(buf, position);
    } else if (update instanceof List) {
      for (Object o : (List) update) {
        doubleSelector.setValue(Numbers.tryParseDouble(o, nullValue));
        delegate.aggregate(buf, position);
      }
    } else {
      doubleSelector.setValue(Numbers.tryParseDouble(update, nullValue));
      delegate.aggregate(buf, position);
    }
  }
}
