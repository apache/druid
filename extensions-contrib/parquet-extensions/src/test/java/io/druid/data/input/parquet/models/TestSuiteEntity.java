package io.druid.data.input.parquet.models;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import io.druid.indexer.HadoopDruidIndexerConfig;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by vrparikh on 5/17/18.
 */
public class TestSuiteEntity {
  static final TypeReference<List<TestSuiteEntity>> TYPE_REFERENCE_TEST_SUITE =
          new TypeReference<List<TestSuiteEntity>>() {};

  @JsonProperty("suite_name")
  private String name;

  @JsonProperty("description")
  private String description;

  @JsonProperty("druid_spec")
  private HadoopDruidIndexerConfig druidSpec;

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getDescription() {
    return description;
  }

  public void setDescription(String description) {
    this.description = description;
  }

  public HadoopDruidIndexerConfig getDruidSpec() {
    return druidSpec;
  }

  public void setDruidSpec(HadoopDruidIndexerConfig druidSpec) {
    this.druidSpec = druidSpec;
  }

  public static Map<String, TestSuiteEntity> fromFile(File file) throws IOException {
    List<TestSuiteEntity> testSuiteEntities =
            HadoopDruidIndexerConfig.JSON_MAPPER.readValue(file, TYPE_REFERENCE_TEST_SUITE);

     Map<String, TestSuiteEntity> testSuiteEntityMap = new HashMap<>();
     for (TestSuiteEntity testSuiteEntity : testSuiteEntities) {
       testSuiteEntityMap.put(testSuiteEntity.name, testSuiteEntity);
     }

     return testSuiteEntityMap;
  }
}
