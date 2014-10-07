/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
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
                ),
                null, null, null, null
            ),
            new AggregatorFactory[]{
                new CountAggregatorFactory("count")
            },
            new UniformGranularitySpec(Granularity.HOUR, QueryGranularity.MINUTE, null, Granularity.HOUR)
        ),
        new RealtimeIOConfig(
            null,
            new RealtimePlumberSchool(
                null, null, null, null, null, null, null, null, null, null, null, null, null, 0
            )
        ),
        new RealtimeTuningConfig(
            null, null, null, null, null, null, null, null
        ),
        null, null, null, null
    );

    String json = jsonMapper.writeValueAsString(schema);

    FireDepartment newSchema = jsonMapper.readValue(json, FireDepartment.class);

    Assert.assertEquals(schema.getDataSchema().getDataSource(), newSchema.getDataSchema().getDataSource());
  }
}
