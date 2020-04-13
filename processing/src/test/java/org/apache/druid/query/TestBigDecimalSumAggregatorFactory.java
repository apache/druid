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

package org.apache.druid.query;

import org.apache.druid.query.aggregation.DoubleSumAggregatorFactory;

import javax.annotation.Nullable;
import java.math.BigDecimal;
import java.util.Comparator;

public class TestBigDecimalSumAggregatorFactory extends DoubleSumAggregatorFactory
{
  public TestBigDecimalSumAggregatorFactory(String name, String fieldName)
  {
    super(name, fieldName);
  }

  @Override
  @Nullable
  public Object finalizeComputation(@Nullable Object object)
  {
    if (object instanceof Long) {
      return BigDecimal.valueOf((Long) object);
    } else if (object instanceof Double) {
      return BigDecimal.valueOf((Double) object);
    } else {
      return object;
    }
  }

  @Override
  public Object deserialize(Object object)
  {
    if (object instanceof String) {
      return BigDecimal.valueOf(Double.parseDouble((String) object));
    } else if (object instanceof Double) {
      return BigDecimal.valueOf((Double) object);
    } else if (object instanceof Long) {
      return BigDecimal.valueOf((Long) object);
    }
    return object;
  }

  @Override
  public Comparator getComparator()
  {
    return new Comparator<BigDecimal>()
    {
      @Override
      public int compare(BigDecimal o, BigDecimal o1)
      {
        return o.compareTo(o1);
      }
    };
  }
}
