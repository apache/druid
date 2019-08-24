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
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.VirtualColumn;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;
import org.apache.druid.sql.calcite.aggregation.Aggregation;
import org.apache.druid.sql.calcite.expression.DruidExpression;

import java.nio.ByteBuffer;

public class TDigestSketchUtils
{
  // Class is not meant to be instantiated
  private TDigestSketchUtils()
  {
  }

  public static MergingDigest deserialize(Object serializedSketch)
  {
    if (serializedSketch instanceof String) {
      String str = (String) serializedSketch;
      return MergingDigest.fromBytes(ByteBuffer.wrap(StringUtils.decodeBase64(StringUtils.toUtf8(str))));
    } else if (serializedSketch instanceof byte[]) {
      return MergingDigest.fromBytes(ByteBuffer.wrap((byte[]) serializedSketch));
    } else if (serializedSketch instanceof MergingDigest) {
      return (MergingDigest) serializedSketch;
    }
    throw new IAE(
        "Object cannot be deserialized to a TDigest Sketch: "
        + serializedSketch.getClass()
    );
  }

  static byte[] toBytes(MergingDigest tDigest)
  {
    byte[] arr = new byte[tDigest.byteSize()];
    ByteBuffer result = ByteBuffer.wrap(arr);
    tDigest.asBytes(result);
    return result.array();
  }

  /**
   * This method computes an estimate of the max intermediate size of a {@link MergingDigest}.
   * Since there is no utility available in the T-Digest library to compute this size,
   * the below code is inspired by looking at
   * {@link MergingDigest#MergingDigest(double, int, int)}
   * This method is current as of 3.2 version and many need to change in future.
   */
  static int getMaxIntermdiateTDigestSize(int compression)
  {
    int intermediateSize = 0;
    int centroidArraySize = (int) (2 * Math.ceil(compression)) + 10;
    intermediateSize += 2 * centroidArraySize * Double.BYTES;
    int tempBufferSize = (int) (5 * Math.ceil(compression));
    intermediateSize += 2 * tempBufferSize * Double.BYTES;
    intermediateSize += tempBufferSize * Integer.BYTES;
    // Adding an extra buffer of 1K for overhead
    return intermediateSize + 1000;
  }

  static void throwExceptionForWrongType(ColumnValueSelector selector)
  {
    final String msg = selector.getObject() == null
                       ? StringUtils.format("Expected a number, but received null")
                       : StringUtils.format(
                           "Expected a number, but received [%s] of type [%s]",
                           selector.getObject(),
                           selector.getObject().getClass()
                       );
    throw new IAE(msg);
  }

  public static boolean matchingAggregatorFactoryExists(
      final DruidExpression input,
      final Integer compression,
      final Aggregation existing,
      final TDigestSketchAggregatorFactory factory
  )
  {
    // Check input for equivalence.
    final boolean inputMatches;
    final VirtualColumn virtualInput = existing.getVirtualColumns()
                                               .stream()
                                               .filter(
                                                   virtualColumn ->
                                                       virtualColumn.getOutputName()
                                                                    .equals(factory.getFieldName())
                                               )
                                               .findFirst()
                                               .orElse(null);

    if (virtualInput == null) {
      inputMatches = input.isDirectColumnAccess()
                     && input.getDirectColumn().equals(factory.getFieldName());
    } else {
      inputMatches = ((ExpressionVirtualColumn) virtualInput).getExpression()
                                                             .equals(input.getExpression());
    }
    return inputMatches && compression == factory.getCompression();
  }
}
