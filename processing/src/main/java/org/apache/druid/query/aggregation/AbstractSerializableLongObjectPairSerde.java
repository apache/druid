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

import org.apache.druid.collections.SerializablePair;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.segment.serde.ComplexMetricExtractor;
import org.apache.druid.segment.serde.ComplexMetricSerde;

import javax.annotation.Nullable;

// To maintain parity with the longString pair serde,
// we have made EXPECTED_VERSION as 3 because it uses the same delta & block encoding technique.
// 0,1,2 versions for numeric serdes do not exist.
public abstract class AbstractSerializableLongObjectPairSerde<T extends SerializablePair<Long, ?>> extends
    ComplexMetricSerde
{
  private final Class<T> pairClass;

  AbstractSerializableLongObjectPairSerde(Class<T> pairClass)
  {
    this.pairClass = pairClass;
  }

  @Override
  public ComplexMetricExtractor<?> getExtractor()
  {
    return new ComplexMetricExtractor<Object>()
    {
      @Override
      public Class<T> extractedClass()
      {
        return pairClass;
      }

      @Nullable
      @Override
      public Object extractValue(InputRow inputRow, String metricName)
      {
        return inputRow.getRaw(metricName);
      }
    };
  }
}
