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
package io.druid.data.input.parquet.models;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import io.druid.indexer.HadoopDruidIndexerConfig;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestSuiteEntity
{
  static final TypeReference<List<TestSuiteEntity>> TYPE_REFERENCE_TEST_SUITE =
      new TypeReference<List<TestSuiteEntity>>()
      {
      };

  @JsonProperty("suite_name")
  private String name;

  @JsonProperty("description")
  private String description;

  @JsonProperty("druid_spec")
  private HadoopDruidIndexerConfig druidSpec;

  public static Map<String, TestSuiteEntity> fromFile(File file) throws IOException
  {
    List<TestSuiteEntity> testSuiteEntities =
        HadoopDruidIndexerConfig.JSON_MAPPER.readValue(file, TYPE_REFERENCE_TEST_SUITE);

    Map<String, TestSuiteEntity> testSuiteEntityMap = new HashMap<>();
    for (TestSuiteEntity testSuiteEntity : testSuiteEntities) {
      testSuiteEntityMap.put(testSuiteEntity.name, testSuiteEntity);
    }

    return testSuiteEntityMap;
  }

  public String getName()
  {
    return name;
  }

  public void setName(String name)
  {
    this.name = name;
  }

  public String getDescription()
  {
    return description;
  }

  public void setDescription(String description)
  {
    this.description = description;
  }

  public HadoopDruidIndexerConfig getDruidSpec()
  {
    return druidSpec;
  }

  public void setDruidSpec(HadoopDruidIndexerConfig druidSpec)
  {
    this.druidSpec = druidSpec;
  }
}
