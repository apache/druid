/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.segment.realtime;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.metamx.common.Granularity;
import io.druid.data.input.impl.DimensionsSpec;
import io.druid.data.input.impl.JSONParseSpec;
import io.druid.data.input.impl.StringInputRowParser;
import io.druid.data.input.impl.TimestampSpec;
import io.druid.granularity.QueryGranularity;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.CountAggregatorFactory;
import io.druid.segment.indexing.DataSchema;
import io.druid.segment.indexing.RealtimeIOConfig;
import io.druid.segment.indexing.RealtimeTuningConfig;
import io.druid.segment.indexing.granularity.UniformGranularitySpec;
import io.druid.segment.realtime.plumber.RealtimePlumberSchool;
import junit.framework.Assert;
import org.junit.Test;

import java.util.Arrays;

/**
 */
public class FireDepartmentTest
{
  @Test
  public void testSerde() throws Exception
  {
    ObjectMapper jsonMapper = new DefaultObjectMapper();

    FireDepartment schema = new FireDepartment(
        new DataSchema(
            "foo",
            new StringInputRowParser(
                new JSONParseSpec(
                    new TimestampSpec(
                        "timestamp",
                        "auto"
                    ),
                    new DimensionsSpec(
                        Arrays.asList("dim1", "dim2"),
                        null,
                        null
                    )
                )
            ),
            new AggregatorFactory[]{
                new CountAggregatorFactory("count")
            },
            new UniformGranularitySpec(Granularity.HOUR, QueryGranularity.MINUTE, null)
        ),
        new RealtimeIOConfig(
            null,
            new RealtimePlumberSchool(
                null, null, null, null, null, null, null
            )
        ),
        new RealtimeTuningConfig(
            null, null, null, null, null, null, null, null, null, false, false, null
        )
    );

    String json = jsonMapper.writeValueAsString(schema);

    FireDepartment newSchema = jsonMapper.readValue(json, FireDepartment.class);

    Assert.assertEquals(schema.getDataSchema().getDataSource(), newSchema.getDataSchema().getDataSource());
  }
}
