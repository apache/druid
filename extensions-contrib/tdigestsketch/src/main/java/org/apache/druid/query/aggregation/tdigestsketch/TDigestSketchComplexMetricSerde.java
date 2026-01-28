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

package org.apache.druid.query.aggregation.tdigestsketch;

import com.tdunning.math.stats.MergingDigest;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.segment.data.ObjectStrategy;
import org.apache.druid.segment.serde.ComplexMetricExtractor;
import org.apache.druid.segment.serde.ComplexMetricSerde;

public class TDigestSketchComplexMetricSerde extends ComplexMetricSerde
{
  private static final TDigestSketchObjectStrategy STRATEGY = new TDigestSketchObjectStrategy();

  @Override
  public String getTypeName()
  {
    return TDigestSketchAggregatorFactory.TYPE_NAME;
  }

  @Override
  public ComplexMetricExtractor getExtractor()
  {
    return new ComplexMetricExtractor()
    {
      @Override
      public Class<?> extractedClass()
      {
        return MergingDigest.class;
      }

      @Override
      public Object extractValue(final InputRow inputRow, final String metricName)
      {
        final Object object = inputRow.getRaw(metricName);
        if (object == null || object instanceof Number || object instanceof MergingDigest) {
          return object;
        }
        if (object instanceof String) {
          String objectString = (String) object;
          if (Character.isDigit((objectString).charAt(0))) {
            // Base64 representation of MergingDigest starts with A. So if it's a
            // string that starts with a digit, we assume it is a number.
            try {
              Double doubleValue = Double.parseDouble(objectString);
              return doubleValue;
            }
            catch (NumberFormatException e) {
              throw new IAE("Expected a string with a number, received value " + objectString);
            }
          }
        }
        return TDigestSketchUtils.deserialize(object);
      }
    };
  }

  @Override
  public ObjectStrategy<MergingDigest> getObjectStrategy()
  {
    return STRATEGY;
  }
}
