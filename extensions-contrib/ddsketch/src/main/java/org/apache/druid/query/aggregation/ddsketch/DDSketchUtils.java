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

package org.apache.druid.query.aggregation.ddsketch;

import com.datadoghq.sketch.ddsketch.DDSketch;
import com.datadoghq.sketch.ddsketch.DDSketchProtoBinding;
import com.datadoghq.sketch.ddsketch.store.CollapsingLowestDenseStore;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.sql.calcite.expression.DruidExpression;
import org.apache.druid.sql.calcite.rel.VirtualColumnRegistry;


public class DDSketchUtils
{
  // Class is not meant to be instantiated
  private DDSketchUtils()
  {
  }

  public static DDSketch deserialize(Object serializedSketch)
  {
    try {
      if (serializedSketch instanceof String) {
        String str = (String) serializedSketch;
        byte[] bytes = StringUtils.decodeBase64(StringUtils.toUtf8(str));
        com.datadoghq.sketch.ddsketch.proto.DDSketch proto = com.datadoghq.sketch.ddsketch.proto.DDSketch.parseFrom(bytes);
        DDSketch recovered = DDSketchProtoBinding.fromProto(() -> new CollapsingLowestDenseStore(1000), proto);
        return recovered;
      } else if (serializedSketch instanceof byte[]) {
        com.datadoghq.sketch.ddsketch.proto.DDSketch proto = com.datadoghq.sketch.ddsketch.proto.DDSketch.parseFrom((byte[]) serializedSketch);
        DDSketch recovered = DDSketchProtoBinding.fromProto(() -> new CollapsingLowestDenseStore(1000), proto);
        return recovered;
      } 
    }
    catch (InvalidProtocolBufferException e) {
      throw new IAE(
          "Object cannot be deserialized to a DDSketch Sketch: "
          + serializedSketch.getClass()
      );
    }
    if (serializedSketch instanceof DDSketch) {
      return (DDSketch) serializedSketch;
    }
    throw new IAE(
        "Object cannot be deserialized to a DDSketch Sketch: "
        + serializedSketch.getClass()
    );
  }

  static byte[] toBytes(DDSketch sketch)
  {
    return DDSketchProtoBinding.toProto(sketch).toByteArray();
  }

  public static boolean matchingAggregatorFactoryExists(
      final VirtualColumnRegistry virtualColumnRegistry,
      final DruidExpression input,
      final double relativeError,
      final int numBins,
      final DDSketchAggregatorFactory factory
  )
  {
    // Check input for equivalence.
    final boolean inputMatches;
    final DruidExpression virtualInput =
        virtualColumnRegistry.findVirtualColumnExpressions(factory.requiredFields())
                             .stream()
                             .findFirst()
                             .orElse(null);

    if (virtualInput == null) {
      inputMatches = input.isDirectColumnAccess() && input.getDirectColumn().equals(factory.getFieldName());
    } else {
      inputMatches = virtualInput.equals(input);
    }
    return inputMatches && relativeError == factory.getRelativeError() && numBins == factory.getNumBins();
  }
}
