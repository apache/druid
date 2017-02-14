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

package io.druid.query.aggregation.hyperloglog;

import io.druid.data.input.InputRow;
import io.druid.hll.HyperLogLogCollector;
import io.druid.hll.HyperLogLogHash;
import io.druid.java.util.common.ISE;
import io.druid.segment.serde.ComplexMetricExtractor;
import org.apache.commons.codec.binary.Base64;

import java.nio.ByteBuffer;

public class PreComputedHyperUniquesSerde extends HyperUniquesSerde
{
  public PreComputedHyperUniquesSerde(HyperLogLogHash hyperLogLogHash)
  {
    super(hyperLogLogHash);
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
                                     .fold(ByteBuffer.wrap(Base64.decodeBase64((String) rawValue)));
        }

        throw new ISE("Object is not of a type[%s] that can be deserialized to HyperLogLog.", rawValue.getClass());
      }
    };
  }
}
