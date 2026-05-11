/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.druid.transform;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.druid.ConfluentExtensionsModule;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.InputRowParser;
import org.apache.druid.data.input.impl.MapInputRowParser;
import org.apache.druid.data.input.impl.TimeAndDimsParseSpec;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.transform.TransformSpec;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

public class ExtractTransformTest
{

  private static final MapInputRowParser PARSER = new MapInputRowParser(
      new TimeAndDimsParseSpec(
          new TimestampSpec("t", "auto", DateTimes.of("2020-01-01")),
          new DimensionsSpec(DimensionsSpec.getDefaultSchemas(ImmutableList.of("topic", "tenant")))
      )
  );

  private static final Map<String, Object> ROW1 = ImmutableMap.<String, Object>builder()
      .put("topic", "lkc-abc123_mytopic")
      .build();

  private static final Map<String, Object> ROW2 = ImmutableMap.<String, Object>builder()
      .put("tenant", "lkc-xyz789")
      .put("tenant_topic", "topic0")
      .put("topic", "lkc-abc123_mytopic")
      .build();

  private static final Map<String, Object> ROW3 = ImmutableMap.<String, Object>builder()
      .put("topic", "invalid-topic")
      .build();

  private static final Map<String, Object> ROW4 = ImmutableMap.<String, Object>builder()
      .build();


  @Test
  public void testExtraction()
  {
    final TransformSpec transformSpec = new TransformSpec(
        null,
        ImmutableList.of(
            new ExtractTenantTransform("tenant", "topic"),
            new ExtractTenantTopicTransform("tenant_topic", "topic")
        )
    );

    final InputRowParser<Map<String, Object>> parser = transformSpec.decorate(PARSER);
    final InputRow row = parser.parseBatch(ROW1).get(0);

    Assert.assertNotNull(row);
    Assert.assertEquals(ImmutableList.of("topic", "tenant"), row.getDimensions());
    Assert.assertEquals(ImmutableList.of("lkc-abc123"), row.getDimension("tenant"));
    Assert.assertEquals(ImmutableList.of("mytopic"), row.getDimension("tenant_topic"));
  }

  @Test
  public void testInternal()
  {
    Assert.assertEquals(null, TenantUtils.extractTenantTopic("__consumer_offsets"));
    Assert.assertEquals(null, TenantUtils.extractTenant("__consumer_offsets"));
    Assert.assertEquals(null, TenantUtils.extractTenantTopic("other.topic"));
    Assert.assertEquals(null, TenantUtils.extractTenant("other.topic"));
  }

  @Test
  public void testPreserveExistingFields()
  {
    final TransformSpec transformSpec = new TransformSpec(
        null,
        ImmutableList.of(
            new ExtractTenantTransform("tenant", "topic"),
            new ExtractTenantTopicTransform("tenant_topic", "topic")
        )
    );

    final InputRowParser<Map<String, Object>> parser = transformSpec.decorate(PARSER);
    final InputRow row = parser.parseBatch(ROW2).get(0);

    Assert.assertNotNull(row);
    Assert.assertEquals(ImmutableList.of("topic", "tenant"), row.getDimensions());
    Assert.assertEquals(ImmutableList.of("lkc-xyz789"), row.getDimension("tenant"));
    Assert.assertEquals(ImmutableList.of("topic0"), row.getDimension("tenant_topic"));
  }

  @Test
  public void testInvalidTopics()
  {
    final TransformSpec transformSpec = new TransformSpec(
        null,
        ImmutableList.of(
            new ExtractTenantTransform("tenant", "topic"),
            new ExtractTenantTopicTransform("tenant_topic", "topic")
        )
    );

    final InputRowParser<Map<String, Object>> parser = transformSpec.decorate(PARSER);
    final InputRow row = parser.parseBatch(ROW3).get(0);

    Assert.assertNotNull(row);
    Assert.assertEquals(ImmutableList.of("topic", "tenant"), row.getDimensions());
    Assert.assertNull(row.getRaw("tenant"));
    Assert.assertNull(row.getRaw("tenant_topic"));
  }

  @Test
  public void testNullTopic()
  {
    final TransformSpec transformSpec = new TransformSpec(
        null,
        ImmutableList.of(
            new ExtractTenantTransform("tenant", "topic"),
            new ExtractTenantTopicTransform("tenant_topic", "topic")
        )
    );

    final InputRowParser<Map<String, Object>> parser = transformSpec.decorate(PARSER);
    final InputRow row = parser.parseBatch(ROW4).get(0);

    Assert.assertNotNull(row);
    Assert.assertEquals(ImmutableList.of("topic", "tenant"), row.getDimensions());
    Assert.assertNull(row.getRaw("tenant"));
    Assert.assertNull(row.getRaw("tenant_topic"));
  }

  @Test
  public void testSerde() throws Exception
  {
    final TransformSpec transformSpec = new TransformSpec(
        null,
        ImmutableList.of(
            new ExtractTenantTopicTransform("tenant_topic", "topic"),
            new ExtractTenantTransform("tenant", "topic")
        )
    );

    final ObjectMapper jsonMapper = TestHelper.makeJsonMapper();
    jsonMapper.registerModules(new ConfluentExtensionsModule().getJacksonModules());

    Assert.assertEquals(
        transformSpec,
        jsonMapper.readValue(jsonMapper.writeValueAsString(transformSpec), TransformSpec.class)
    );
  }
}
