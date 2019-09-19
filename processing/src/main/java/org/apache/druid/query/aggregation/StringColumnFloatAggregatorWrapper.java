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
import org.apache.druid.segment.BaseFloatColumnValueSelector;
import org.apache.druid.segment.BaseObjectColumnValueSelector;
import org.apache.druid.segment.selector.settable.SettableValueFloatColumnValueSelector;

import java.util.List;
import java.util.function.Function;

/**
 * This class can be used to wrap Float Aggregator that consume float type columns to handle String type.
 */
public class StringColumnFloatAggregatorWrapper extends DelegatingAggregator
{
  private final BaseObjectColumnValueSelector selector;
  private final float nullValue;
  private final SettableValueFloatColumnValueSelector floatSelector;

  public StringColumnFloatAggregatorWrapper(
      BaseObjectColumnValueSelector selector,
      Function<BaseFloatColumnValueSelector, Aggregator> delegateBuilder,
      float nullValue
  )
  {
    this.floatSelector = new SettableValueFloatColumnValueSelector();
    this.selector = selector;
    this.nullValue = nullValue;
    this.delegate = delegateBuilder.apply(floatSelector);
  }

  @Override
  public void aggregate()
  {
    Object update = selector.getObject();

    if (update == null) {
      floatSelector.setValue(nullValue);
      delegate.aggregate();
    } else if (update instanceof List) {
      for (Object o : (List) update) {
        floatSelector.setValue(Numbers.tryParseFloat(o, nullValue));
        delegate.aggregate();
      }
    } else {
      floatSelector.setValue(Numbers.tryParseFloat(update, nullValue));
      delegate.aggregate();
    }
  }
}
