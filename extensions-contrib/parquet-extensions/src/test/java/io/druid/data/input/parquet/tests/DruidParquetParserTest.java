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
package io.druid.data.input.parquet.tests;

import io.druid.data.input.InputRow;
import io.druid.data.input.parquet.models.TestSuiteEntity;
import io.druid.indexer.HadoopDruidIndexerConfig;
import io.druid.indexer.path.StaticPathSpec;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertTrue;

public class DruidParquetParserTest extends DruidParquetInputTest
{

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testParquetParser() throws IOException, InterruptedException
  {
    HadoopDruidIndexerConfig config = HadoopDruidIndexerConfig.fromFile(new File(
        "example/parser/spec_with_defunct_attributes_for_parser_testing.json"));
    Job job = Job.getInstance(new Configuration());
    config.intoConfiguration(job);
    thrown.expect(IllegalArgumentException.class);
    getFirstRecord(job, ((StaticPathSpec) config.getPathSpec()).getPaths());
  }

  @Test
  public void testAllDatatypeCombinationsParser() throws IOException, InterruptedException
  {
    HadoopDruidIndexerConfig config = HadoopDruidIndexerConfig.fromFile(new File(
        "example/parser/events_with_all_datatype_test_combinations.json"));
    Job job = Job.getInstance(new Configuration());
    config.intoConfiguration(job);
    GenericRecord data = getFirstRecord(job, ((StaticPathSpec) config.getPathSpec()).getPaths());
    InputRow row = ((List<InputRow>) config.getParser().parseBatch(data)).get(0);
    for (String dimension : row.getDimensions()) {
      System.out.println(dimension + " -> " + row.getDimension(dimension));
    }
    System.out.println("cost_b_2nd_tag_sum -> " + row.getMetric("cost_b_2nd_tag_sum"));
    assertTrue(row.getDimension("eventId").equals(Collections.singletonList("1")));
    assertTrue(row.getDimension("valid_map_access").equals(Collections.singletonList("US")));
    assertTrue(row.getDimension("max_access_with_non_existing_key").isEmpty());
    assertTrue(row.getDimension("max_access_with_un_available_map").isEmpty());
    assertTrue(row.getDimension("array_access_with_index").equals(Collections.singletonList("20")));
    assertTrue(row.getDimension("array_access_with_default_index").equals(Arrays.asList("10", "20")));
    assertTrue(row.getDimension("array_access_with_overbound_index").isEmpty());
    assertTrue(row.getDimension("union_attribute").isEmpty());
    assertTrue(row.getDimension("tag_ids").equals(Arrays.asList("100", "101")));
    assertTrue(row.getDimension("reference_ids").equals(Arrays.asList("10", "20")));
    assertTrue(row.getDimension("cost_a").equals(Arrays.asList("10.35", "9.4")));
    assertTrue(row.getDimension("cost_b_1st_tag").equals(Collections.singletonList("2.7")));
    assertTrue(row.getDimension("non_available_list").isEmpty());
    assertTrue(row.getDimension("list_with_non_available_index").isEmpty());
  }

  @Test
  public void testSpecHavingToTraversalOverMaxDepth() throws IOException, InterruptedException
  {
    HadoopDruidIndexerConfig config = HadoopDruidIndexerConfig.fromFile(new File(
        "example/parser/spec_having_traversal_greater_than_max_depth.json"));
    Job job = Job.getInstance(new Configuration());
    config.intoConfiguration(job);
    GenericRecord data = getFirstRecord(job, ((StaticPathSpec) config.getPathSpec()).getPaths());
    InputRow row = ((List<InputRow>) config.getParser().parseBatch(data)).get(0);
    for (String dimension : row.getDimensions()) {
      System.out.println(dimension + " -> " + row.getDimension(dimension));
    }
    assertTrue(row.getDimension("e1_id").equals(Collections.singletonList("ELEMENT1")));
    assertTrue(row.getDimension("e3_id").equals(Collections.singletonList("ELEMENT3")));
    assertTrue(row.getDimension("cost_factor").equals(Collections.singletonList("23.5")));
    //This attribute crossed the default max depth level of 3, hence receiving empty set
    assertTrue(row.getDimension("e5_id").isEmpty());
  }

  @Test
  public void testCustomMaxDepthTraversal() throws IOException, InterruptedException
  {
    HadoopDruidIndexerConfig config = HadoopDruidIndexerConfig.fromFile(new File(
        "example/parser/spec_with_custom_max_depth_traversal.json"));
    Job job = Job.getInstance(new Configuration());
    config.intoConfiguration(job);
    GenericRecord data = getFirstRecord(job, ((StaticPathSpec) config.getPathSpec()).getPaths());
    InputRow row = ((List<InputRow>) config.getParser().parseBatch(data)).get(0);
    for (String dimension : row.getDimensions()) {
      System.out.println(dimension + " -> " + row.getDimension(dimension));
    }
    assertTrue(row.getDimension("e5_id").equals(Collections.singletonList("ELEM5")));
    assertTrue(row.getDimension("empty_val_as_wrong_ref").isEmpty());
  }

  @Test
  public void testAllDatatypeCombinationsParserNew() throws IOException, InterruptedException
  {
    HadoopDruidIndexerConfig config = HadoopDruidIndexerConfig.fromFile(new File(
        "example/parser/events_with_all_datatype_test_combinations_new.json"));
    Job job = Job.getInstance(new Configuration());
    config.intoConfiguration(job);
    GenericRecord data = getFirstRecord(job, ((StaticPathSpec) config.getPathSpec()).getPaths());
    InputRow row = ((List<InputRow>) config.getParser().parseBatch(data)).get(0);
    for (String dimension : row.getDimensions()) {
      System.out.println(dimension + " -> " + row.getDimension(dimension));
    }
    System.out.println("cost_b_2nd_tag_sum -> " + row.getMetric("cost_b_2nd_tag_sum"));
    assertTrue(row.getDimension("eventId").equals(Collections.singletonList("1")));
    assertTrue(row.getDimension("valid_map_access").equals(Collections.singletonList("US")));
    assertTrue(row.getDimension("max_access_with_non_existing_key").isEmpty());
    assertTrue(row.getDimension("max_access_with_un_available_map").isEmpty());
    assertTrue(row.getDimension("array_access_with_index").equals(Collections.singletonList("20")));
    assertTrue(row.getDimension("array_access_with_default_index").equals(Arrays.asList("10", "20")));
    assertTrue(row.getDimension("array_access_with_overbound_index").isEmpty());
    assertTrue(row.getDimension("union_attribute").isEmpty());
    assertTrue(row.getDimension("tag_ids").equals(Arrays.asList("100", "101")));
    assertTrue(row.getDimension("reference_ids").equals(Arrays.asList("10", "20")));
    assertTrue(row.getDimension("cost_a").equals(Arrays.asList("10.35", "9.4")));
    assertTrue(row.getDimension("cost_b_1st_tag").equals(Collections.singletonList("2.7")));
  }

  @Test
  public void testSuiteCoveringValidations() throws IOException
  {
    testMapValidations();
    testNestedMapValidations();
  }

  private void testMapValidations() throws IOException
  {
    Schema schema = SchemaBuilder.record("test_record")
                                 .fields()
                                 .name("root_map_key").type().map().values().intType().noDefault()
                                 .name("utf8type_string").type().stringType().noDefault()
                                 .endRecord();

    GenericRecordBuilder genericRecordBuilder = new GenericRecordBuilder(schema);
    Map<Utf8, Integer> map = new HashMap<>();
    map.put(new Utf8("map_key"), 2);
    genericRecordBuilder.set("root_map_key", map);
    genericRecordBuilder.set("utf8type_string", new Utf8("utf8_dimension"));
    GenericData.Record record = genericRecordBuilder.build();

    Map<String, TestSuiteEntity> testSuites = TestSuiteEntity.fromFile(new File(
        "example/parser/test_suite_for_map_validations.json"));

    TestSuiteEntity testSuiteEntity = testSuites.get("test_map_validations");
    HadoopDruidIndexerConfig config = testSuiteEntity.getDruidSpec();

    Job job = Job.getInstance(new Configuration());
    config.intoConfiguration(job);

    InputRow row = ((List<InputRow>) config.getParser().parseBatch(record)).get(0);

    for (String dimension : row.getDimensions()) {
      System.out.println(dimension + " -> " + row.getDimension(dimension));
    }
    assertTrue(row.getDimension("dimension_1").equals(Collections.singletonList("2")));

    testSuiteEntity = testSuites.get("test_map_with_empty_field");
    config = testSuiteEntity.getDruidSpec();

    job = Job.getInstance(new Configuration());
    config.intoConfiguration(job);

    row = ((List<InputRow>) config.getParser().parseBatch(record)).get(0);

    for (String dimension : row.getDimensions()) {
      System.out.println(dimension + " -> " + row.getDimension(dimension));
    }
    assertTrue(row.getDimension("dimension_1").isEmpty());
  }


  private void testNestedMapValidations() throws IOException
  {
    Schema schema = SchemaBuilder.record("test_record")
                                 .fields()
                                 .name("root_map_key").type().map().values().intType().noDefault()
                                 .endRecord();
    GenericRecordBuilder genericRecordBuilder = new GenericRecordBuilder(schema);
    Map<Utf8, Integer> child2 = new HashMap<>();
    child2.put(new Utf8("child2_key"), 2);
    Map<Utf8, Map> child1 = new HashMap<>();
    child1.put(new Utf8("child1_key"), child2);

    Map<Utf8, Map> map = new HashMap<>();
    map.put(new Utf8("map_key"), child1);
    genericRecordBuilder.set("root_map_key", map);
    GenericData.Record record = genericRecordBuilder.build();

    Map<String, TestSuiteEntity> testSuites = TestSuiteEntity.fromFile(new File(
        "example/parser/test_suite_for_map_validations.json"));

    TestSuiteEntity testSuiteEntity = testSuites.get("test_nested_map");
    HadoopDruidIndexerConfig config = testSuiteEntity.getDruidSpec();

    Job job = Job.getInstance(new Configuration());
    config.intoConfiguration(job);

    InputRow row = ((List<InputRow>) config.getParser().parseBatch(record)).get(0);

    for (String dimension : row.getDimensions()) {
      System.out.println(dimension + " -> " + row.getDimension(dimension));
    }
    assertTrue(row.getDimension("dimension_1").equals(Collections.singletonList("2")));
  }

  @Test
  public void testDimensionsBothWithinParserSpecAndDirectColumn() throws IOException
  {
    Schema schema = SchemaBuilder.record("test_record")
                                 .fields()
                                 .name("root_map_key").type().map().values().intType().noDefault()
                                 .name("utf8type_string").type().stringType().noDefault()
                                 .name("long_column").type().longType().longDefault(30)
                                 .name("double_column").type().doubleType().doubleDefault(89.35)
                                 .name("string_column").type().stringType().stringDefault("default_string")
                                 .endRecord();

    GenericRecordBuilder genericRecordBuilder = new GenericRecordBuilder(schema);
    Map<Utf8, Integer> map = new HashMap<>();
    map.put(new Utf8("map_key"), 2);
    genericRecordBuilder.set("root_map_key", map);
    genericRecordBuilder.set("utf8type_string", new Utf8("utf8_dimension"));
    GenericData.Record record = genericRecordBuilder.build();

    Map<String, TestSuiteEntity> testSuites = TestSuiteEntity.fromFile(new File(
        "example/parser/test_suite_for_map_validations.json"));

    TestSuiteEntity testSuiteEntity = testSuites.get("test_dimensions_in_both_parser_spec_and_as_parquet_column");
    HadoopDruidIndexerConfig config = testSuiteEntity.getDruidSpec();

    Job job = Job.getInstance(new Configuration());
    config.intoConfiguration(job);

    InputRow row = ((List<InputRow>) config.getParser().parseBatch(record)).get(0);

    for (String dimension : row.getDimensions()) {
      System.out.println(dimension + " -> " + row.getDimension(dimension));
    }
    assertTrue(row.getDimension("dimension_1").equals(Collections.singletonList("2")));
    assertTrue(row.getDimension("utf8type_string").equals(Collections.singletonList("utf8_dimension")));
    assertTrue(row.getDimension("long_column").equals(Collections.singletonList("30")));
    assertTrue(row.getDimension("double_column").equals(Collections.singletonList("89.35")));
    assertTrue(row.getDimension("string_column").equals(Collections.singletonList("default_string")));
  }

  @Test
  public void structHavingMapWithDoubleKey() throws IOException, InterruptedException
  {
    HadoopDruidIndexerConfig config = HadoopDruidIndexerConfig.fromFile(new File(
        "example/parser/test_parquet_double_map_key_parse_exception.json"));
    Job job = Job.getInstance(new Configuration());
    config.intoConfiguration(job);
    // Expect IllegalArgumentException("Map key type must be binary (UTF8): " + keyType);
    // As avro schema always accepts UTF8 MAP key
    thrown.expect(IllegalArgumentException.class);
    getFirstRecord(job, ((StaticPathSpec) config.getPathSpec()).getPaths());
  }
}
