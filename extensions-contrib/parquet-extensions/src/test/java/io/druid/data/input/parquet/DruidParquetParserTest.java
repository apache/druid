package io.druid.data.input.parquet;

import io.druid.data.input.InputRow;
import io.druid.indexer.HadoopDruidIndexerConfig;
import io.druid.indexer.path.StaticPathSpec;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

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
    assertTrue(row.getDimension("array_access_with_default_index").equals(Collections.singletonList("10")));
    assertTrue(row.getDimension("array_access_with_overbound_index").isEmpty());
    assertTrue(row.getDimension("union_attribute").isEmpty());
    assertTrue(row.getDimension("tag_ids").equals(Arrays.asList("100", "101")));
    assertTrue(row.getDimension("reference_ids").equals(Arrays.asList("10", "20")));
    assertTrue(row.getDimension("cost_a").equals(Arrays.asList("10.35", "9.4")));
    assertTrue(row.getDimension("cost_b_1st_tag").equals(Collections.singletonList("2.7")));
  }

  @Test
  public void testCircularSpec() throws IOException, InterruptedException
  {
    HadoopDruidIndexerConfig config = HadoopDruidIndexerConfig.fromFile(new File(
        "example/parser/circular_dependency_spec.json"));
    Job job = Job.getInstance(new Configuration());
    config.intoConfiguration(job);
    GenericRecord data = getFirstRecord(job, ((StaticPathSpec) config.getPathSpec()).getPaths());
    InputRow row = ((List<InputRow>) config.getParser().parseBatch(data)).get(0);
    for (String dimension : row.getDimensions()) {
      System.out.println(dimension + " -> " + row.getDimension(dimension));
    }
  }

  @Test
  public void testSpecHavingToTraversalOverMaxDepth() throws IOException, InterruptedException
  {

  }

  @Test
  public void testWithNonAvailableFields()
  {

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
