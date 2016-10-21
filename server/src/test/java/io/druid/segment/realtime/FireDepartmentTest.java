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

package io.druid.segment.realtime;

import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.druid.client.cache.CacheConfig;
import io.druid.client.cache.MapCache;
import io.druid.data.input.impl.DimensionsSpec;
import io.druid.data.input.impl.JSONParseSpec;
import io.druid.data.input.impl.StringInputRowParser;
import io.druid.data.input.impl.TimestampSpec;
import io.druid.granularity.QueryGranularities;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.java.util.common.Granularity;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.CountAggregatorFactory;
import io.druid.segment.TestHelper;
import io.druid.segment.indexing.DataSchema;
import io.druid.segment.indexing.RealtimeIOConfig;
import io.druid.segment.indexing.RealtimeTuningConfig;
import io.druid.segment.indexing.granularity.UniformGranularitySpec;
import io.druid.segment.realtime.plumber.RealtimePlumberSchool;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.util.Arrays;
import java.util.Map;

/**
 */
public class FireDepartmentTest
{

  public static final CacheConfig NO_CACHE_CONFIG = new CacheConfig()
  {
    @Override
    public boolean isPopulateCache()
    {
      return false;
    }

    @Override
    public boolean isUseCache()
    {
      return false;
    }
  };

  @Test
  public void testSerde() throws Exception
  {
    ObjectMapper jsonMapper = new DefaultObjectMapper();
    jsonMapper.setInjectableValues(new InjectableValues.Std().addValue(ObjectMapper.class, jsonMapper));

    FireDepartment schema = new FireDepartment(
        new DataSchema(
            "foo",
            jsonMapper.convertValue(
                new StringInputRowParser(
                    new JSONParseSpec(
                        new TimestampSpec(
                            "timestamp",
                            "auto",
                            null
                        ),
                        new DimensionsSpec(
                            DimensionsSpec.getDefaultSchemas(Arrays.asList("dim1", "dim2")),
                            null,
                            null
                        ),
                        null,
                        null
                    ),
                    null
                ),
                Map.class
            ),
            new AggregatorFactory[]{
                new CountAggregatorFactory("count")
            },
            new UniformGranularitySpec(Granularity.HOUR, QueryGranularities.MINUTE, null),
            jsonMapper
        ),
        new RealtimeIOConfig(
            null,
            new RealtimePlumberSchool(
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                TestHelper.getTestIndexMerger(),
                TestHelper.getTestIndexMergerV9(),
                TestHelper.getTestIndexIO(),
                MapCache.create(0),
                NO_CACHE_CONFIG,
                TestHelper.getObjectMapper()

            ),
            null
        ),
        RealtimeTuningConfig.makeDefaultTuningConfig(new File("/tmp/nonexistent"))
    );

    String json = jsonMapper.writeValueAsString(schema);

    FireDepartment newSchema = jsonMapper.readValue(json, FireDepartment.class);

    Assert.assertEquals(schema.getDataSchema().getDataSource(), newSchema.getDataSchema().getDataSource());
    Assert.assertEquals("/tmp/nonexistent", schema.getTuningConfig().getBasePersistDirectory().toString());
  }
}
