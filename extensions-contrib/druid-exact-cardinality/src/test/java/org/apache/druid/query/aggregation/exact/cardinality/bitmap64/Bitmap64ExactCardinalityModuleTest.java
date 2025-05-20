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

package org.apache.druid.query.aggregation.exact.cardinality.bitmap64;

import org.apache.druid.segment.serde.ComplexMetricSerde;
import org.apache.druid.segment.serde.ComplexMetrics;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class Bitmap64ExactCardinalityModuleTest
{
  @Test
  public void testRegisterSerde()
  {
    Bitmap64ExactCardinalityModule.registerSerde();

    ComplexMetricSerde typeNameSerde = ComplexMetrics.getSerdeForType(Bitmap64ExactCardinalityModule.TYPE_NAME);
    Assertions.assertNotNull(typeNameSerde);
    Assertions.assertInstanceOf(Bitmap64ExactCardinalityMergeComplexMetricSerde.class, typeNameSerde);

    ComplexMetricSerde buildTypeNameSerde = ComplexMetrics.getSerdeForType(Bitmap64ExactCardinalityModule.BUILD_TYPE_NAME);
    Assertions.assertNotNull(buildTypeNameSerde);
    Assertions.assertInstanceOf(Bitmap64ExactCardinalityBuildComplexMetricSerde.class, buildTypeNameSerde);

    ComplexMetricSerde mergeTypeNameSerde = ComplexMetrics.getSerdeForType(Bitmap64ExactCardinalityModule.MERGE_TYPE_NAME);
    Assertions.assertNotNull(mergeTypeNameSerde);
    Assertions.assertInstanceOf(Bitmap64ExactCardinalityMergeComplexMetricSerde.class, mergeTypeNameSerde);
  }
}
