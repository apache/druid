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

package org.apache.druid.segment.metadata;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.firstlast.first.LongFirstAggregatorFactory;
import org.apache.druid.segment.SchemaPayload;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class FingerprintGeneratorTest
{
  static {
    NullHandling.initializeForTests();
  }

  private final ObjectMapper mapper = TestHelper.makeJsonMapper();
  private final FingerprintGenerator fingerprintGenerator = new FingerprintGenerator(mapper);

  @Test
  public void testGenerateFingerprint_precalculatedHash()
  {
    RowSignature rowSignature = RowSignature.builder().add("c1", ColumnType.FLOAT).build();
    Map<String, AggregatorFactory> aggregatorFactoryMap = new HashMap<>();
    aggregatorFactoryMap.put("longFirst", new LongFirstAggregatorFactory("longFirst", "long-col", null));

    SchemaPayload schemaPayload = new SchemaPayload(rowSignature, aggregatorFactoryMap);

    String expected = "FB7E8AD8F2B96E58ACB99F43E380106D134774B1F5C56641268539FBADB897B3";
    Assert.assertEquals(expected, fingerprintGenerator.generateFingerprint(schemaPayload, "ds", 0));
  }

  @Test
  public void testGenerateFingerprint_columnPermutation()
  {
    RowSignature rowSignature =
        RowSignature.builder()
                    .add("c1", ColumnType.FLOAT)
                    .add("c2", ColumnType.LONG)
                    .add("c3", ColumnType.DOUBLE)
                    .build();

    Map<String, AggregatorFactory> aggregatorFactoryMap = new HashMap<>();
    aggregatorFactoryMap.put("longFirst", new LongFirstAggregatorFactory("longFirst", "long-col", null));

    SchemaPayload schemaPayload = new SchemaPayload(rowSignature, aggregatorFactoryMap);

    RowSignature rowSignaturePermutation =
        RowSignature.builder()
                    .add("c2", ColumnType.LONG)
                    .add("c3", ColumnType.DOUBLE)
                    .add("c1", ColumnType.FLOAT)
                    .build();

    SchemaPayload schemaPayloadNew = new SchemaPayload(rowSignaturePermutation, aggregatorFactoryMap);
    Assert.assertNotEquals(
        fingerprintGenerator.generateFingerprint(schemaPayload, "ds", 0),
        fingerprintGenerator.generateFingerprint(schemaPayloadNew, "ds", 0)
    );
  }

  @Test
  public void testGenerateFingerprint_differentDatasources()
  {
    RowSignature rowSignature =
        RowSignature.builder()
                    .add("c1", ColumnType.FLOAT)
                    .add("c2", ColumnType.LONG)
                    .add("c3", ColumnType.DOUBLE)
                    .build();

    Map<String, AggregatorFactory> aggregatorFactoryMap = new HashMap<>();
    aggregatorFactoryMap.put("longFirst", new LongFirstAggregatorFactory("longFirst", "long-col", null));

    SchemaPayload schemaPayload = new SchemaPayload(rowSignature, aggregatorFactoryMap);

    Assert.assertNotEquals(
        fingerprintGenerator.generateFingerprint(schemaPayload, "ds1", 0),
        fingerprintGenerator.generateFingerprint(schemaPayload, "ds2", 0)
    );
  }

  @Test
  public void testGenerateFingerprint_differentVersion()
  {
    RowSignature rowSignature =
        RowSignature.builder()
                    .add("c1", ColumnType.FLOAT)
                    .add("c2", ColumnType.LONG)
                    .add("c3", ColumnType.DOUBLE)
                    .build();

    Map<String, AggregatorFactory> aggregatorFactoryMap = new HashMap<>();
    aggregatorFactoryMap.put("longFirst", new LongFirstAggregatorFactory("longFirst", "long-col", null));

    SchemaPayload schemaPayload = new SchemaPayload(rowSignature, aggregatorFactoryMap);

    Assert.assertNotEquals(
        fingerprintGenerator.generateFingerprint(schemaPayload, "ds", 0),
        fingerprintGenerator.generateFingerprint(schemaPayload, "ds", 1)
    );
  }
}
