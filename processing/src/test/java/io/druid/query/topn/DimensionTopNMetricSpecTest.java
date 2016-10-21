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

package io.druid.query.topn;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.query.ordering.StringComparators;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class DimensionTopNMetricSpecTest
{
  @Test
  public void testSerdeAlphaNumericDimensionTopNMetricSpec() throws IOException{
    DimensionTopNMetricSpec expectedMetricSpec = new DimensionTopNMetricSpec(null, StringComparators.ALPHANUMERIC);
    DimensionTopNMetricSpec expectedMetricSpec1 = new DimensionTopNMetricSpec("test", StringComparators.ALPHANUMERIC);
    String jsonSpec = "{\n"
                      + "    \"type\": \"dimension\","
                      + "    \"ordering\": \"alphanumeric\"\n"
                      + "}";
    String jsonSpec1 = "{\n"
                       + "    \"type\": \"dimension\","
                       + "    \"ordering\": \"alphanumeric\",\n"
                       + "    \"previousStop\": \"test\"\n"
                       + "}";
    ObjectMapper jsonMapper = new DefaultObjectMapper();
    TopNMetricSpec actualMetricSpec = jsonMapper.readValue(jsonMapper.writeValueAsString(jsonMapper.readValue(jsonSpec, TopNMetricSpec.class)), DimensionTopNMetricSpec.class);
    TopNMetricSpec actualMetricSpec1 = jsonMapper.readValue(jsonMapper.writeValueAsString(jsonMapper.readValue(jsonSpec1, TopNMetricSpec.class)), DimensionTopNMetricSpec.class);
    Assert.assertEquals(expectedMetricSpec, actualMetricSpec);
    Assert.assertEquals(expectedMetricSpec1, actualMetricSpec1);
  }

  @Test
  public void testSerdeLexicographicDimensionTopNMetricSpec() throws IOException{
    DimensionTopNMetricSpec expectedMetricSpec = new DimensionTopNMetricSpec(null, StringComparators.LEXICOGRAPHIC);
    DimensionTopNMetricSpec expectedMetricSpec1 = new DimensionTopNMetricSpec("test", StringComparators.LEXICOGRAPHIC);
    String jsonSpec = "{\n"
                      + "    \"type\": \"dimension\","
                      + "    \"ordering\": \"lexicographic\"\n"
                      + "}";
    String jsonSpec1 = "{\n"
                       + "    \"type\": \"dimension\","
                       + "    \"ordering\": \"lexicographic\",\n"
                       + "    \"previousStop\": \"test\"\n"
                       + "}";
    ObjectMapper jsonMapper = new DefaultObjectMapper();
    TopNMetricSpec actualMetricSpec = jsonMapper.readValue(jsonMapper.writeValueAsString(jsonMapper.readValue(jsonSpec, TopNMetricSpec.class)), DimensionTopNMetricSpec.class);
    TopNMetricSpec actualMetricSpec1 = jsonMapper.readValue(jsonMapper.writeValueAsString(jsonMapper.readValue(jsonSpec1, TopNMetricSpec.class)), DimensionTopNMetricSpec.class);
    Assert.assertEquals(expectedMetricSpec, actualMetricSpec);
    Assert.assertEquals(expectedMetricSpec1, actualMetricSpec1);
  }

  @Test
  public void testSerdeStrlenDimensionTopNMetricSpec() throws IOException{
    DimensionTopNMetricSpec expectedMetricSpec = new DimensionTopNMetricSpec(null, StringComparators.STRLEN);
    DimensionTopNMetricSpec expectedMetricSpec1 = new DimensionTopNMetricSpec("test", StringComparators.STRLEN);
    String jsonSpec = "{\n"
                      + "    \"type\": \"dimension\","
                      + "    \"ordering\": \"strlen\"\n"
                      + "}";
    String jsonSpec1 = "{\n"
                       + "    \"type\": \"dimension\","
                       + "    \"ordering\": \"strlen\",\n"
                       + "    \"previousStop\": \"test\"\n"
                       + "}";
    ObjectMapper jsonMapper = new DefaultObjectMapper();
    TopNMetricSpec actualMetricSpec = jsonMapper.readValue(jsonMapper.writeValueAsString(jsonMapper.readValue(jsonSpec, TopNMetricSpec.class)), DimensionTopNMetricSpec.class);
    TopNMetricSpec actualMetricSpec1 = jsonMapper.readValue(jsonMapper.writeValueAsString(jsonMapper.readValue(jsonSpec1, TopNMetricSpec.class)), DimensionTopNMetricSpec.class);
    Assert.assertEquals(expectedMetricSpec, actualMetricSpec);
    Assert.assertEquals(expectedMetricSpec1, actualMetricSpec1);
  }

  @Test
  public void testSerdeNumericDimensionTopNMetricSpec() throws IOException{
    DimensionTopNMetricSpec expectedMetricSpec = new DimensionTopNMetricSpec(null, StringComparators.NUMERIC);
    DimensionTopNMetricSpec expectedMetricSpec1 = new DimensionTopNMetricSpec("test", StringComparators.NUMERIC);
    String jsonSpec = "{\n"
                      + "    \"type\": \"dimension\","
                      + "    \"ordering\": \"numeric\"\n"
                      + "}";
    String jsonSpec1 = "{\n"
                       + "    \"type\": \"dimension\","
                       + "    \"ordering\": \"numeric\",\n"
                       + "    \"previousStop\": \"test\"\n"
                       + "}";
    ObjectMapper jsonMapper = new DefaultObjectMapper();
    TopNMetricSpec actualMetricSpec = jsonMapper.readValue(jsonMapper.writeValueAsString(jsonMapper.readValue(jsonSpec, TopNMetricSpec.class)), DimensionTopNMetricSpec.class);
    TopNMetricSpec actualMetricSpec1 = jsonMapper.readValue(jsonMapper.writeValueAsString(jsonMapper.readValue(jsonSpec1, TopNMetricSpec.class)), DimensionTopNMetricSpec.class);
    Assert.assertEquals(expectedMetricSpec, actualMetricSpec);
    Assert.assertEquals(expectedMetricSpec1, actualMetricSpec1);
  }
}
