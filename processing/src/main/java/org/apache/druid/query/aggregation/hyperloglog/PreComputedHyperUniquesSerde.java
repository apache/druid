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

package org.apache.druid.query.aggregation.hyperloglog;

import org.apache.druid.data.input.InputRow;
import org.apache.druid.hll.HyperLogLogCollector;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.segment.serde.ComplexMetricExtractor;

import java.nio.ByteBuffer;

public class PreComputedHyperUniquesSerde extends HyperUniquesSerde
{
  public PreComputedHyperUniquesSerde()
  {
    super();
  }

  @Override
  public ComplexMetricExtractor getExtractor()
  {
    return new ComplexMetricExtractor()
    {
      @Override
      public Class<HyperLogLogCollector> extractedClass()
      {
        return HyperLogLogCollector.class;
      }

      @Override
      public HyperLogLogCollector extractValue(InputRow inputRow, String metricName)
      {
        Object rawValue = inputRow.getRaw(metricName);

        if (rawValue == null) {
          return HyperLogLogCollector.makeLatestCollector();
        } else if (rawValue instanceof HyperLogLogCollector) {
          return (HyperLogLogCollector) rawValue;
        } else if (rawValue instanceof byte[]) {
          return HyperLogLogCollector.makeLatestCollector().fold(ByteBuffer.wrap((byte[]) rawValue));
        } else if (rawValue instanceof String) {
          return HyperLogLogCollector.makeLatestCollector()
                                     .fold(ByteBuffer.wrap(StringUtils.decodeBase64String((String) rawValue)));
        }

        throw new ISE("Object is not of a type[%s] that can be deserialized to HyperLogLog.", rawValue.getClass());
      }
    };
  }
}
