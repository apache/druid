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

package org.apache.druid.query.filter;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Sets;
import com.google.inject.Injector;
import com.google.inject.Key;
import org.apache.druid.guice.GuiceInjectors;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.query.extraction.RegexDimExtractionFn;
import org.apache.druid.segment.column.ColumnHolder;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;

public class IntervalDimFilterTest
{
  private static ObjectMapper mapper;

  @Before
  public void setUp()
  {
    Injector defaultInjector = GuiceInjectors.makeStartupInjector();
    mapper = defaultInjector.getInstance(Key.get(ObjectMapper.class, Json.class));
  }

  @Test
  public void testSerde() throws IOException
  {
    DimFilter intervalFilter = new IntervalDimFilter(
        ColumnHolder.TIME_COLUMN_NAME,
        Arrays.asList(
            Intervals.of("1970-01-01T00:00:00.001Z/1970-01-01T00:00:00.004Z"),
            Intervals.of("1975-01-01T00:00:00.001Z/1980-01-01T00:00:00.004Z")
        ),
        null
    );
    String filterStr = mapper.writeValueAsString(intervalFilter);
    IntervalDimFilter actualFilter = mapper.readerFor(DimFilter.class).readValue(filterStr);
    Assert.assertEquals(intervalFilter, actualFilter);

    intervalFilter = new IntervalDimFilter(
        ColumnHolder.TIME_COLUMN_NAME,
        Arrays.asList(
            Intervals.of("1970-01-01T00:00:00.001Z/1970-01-01T00:00:00.004Z"),
            Intervals.of("1975-01-01T00:00:00.001Z/1980-01-01T00:00:00.004Z")
        ),
        new RegexDimExtractionFn(".*", false, null)
    );

    filterStr = mapper.writeValueAsString(intervalFilter);
    actualFilter = mapper.readerFor(DimFilter.class).readValue(filterStr);
    Assert.assertEquals(intervalFilter, actualFilter);
  }

  @Test
  public void testGetCacheKey()
  {
    DimFilter intervalFilter1 = new IntervalDimFilter(
        ColumnHolder.TIME_COLUMN_NAME,
        Arrays.asList(
            Intervals.of("1970-01-01T00:00:00.001Z/1970-01-01T00:00:00.004Z"),
            Intervals.of("1975-01-01T00:00:00.001Z/1980-01-01T00:00:00.004Z")
        ),
        null
    );

    DimFilter intervalFilter2 = new IntervalDimFilter(
        ColumnHolder.TIME_COLUMN_NAME,
        Arrays.asList(
            Intervals.of("1970-01-01T00:00:00.001Z/1970-01-01T00:00:00.004Z"),
            Intervals.of("1976-01-01T00:00:00.001Z/1980-01-01T00:00:00.004Z")
        ),
        null
    );
    Assert.assertNotEquals(intervalFilter1.getCacheKey(), intervalFilter2.getCacheKey());

    RegexDimExtractionFn regexFn = new RegexDimExtractionFn(".*", false, null);
    DimFilter intervalFilter3 = new IntervalDimFilter(
        ColumnHolder.TIME_COLUMN_NAME,
        Arrays.asList(
            Intervals.of("1970-01-01T00:00:00.001Z/1970-01-01T00:00:00.004Z"),
            Intervals.of("1975-01-01T00:00:00.001Z/1980-01-01T00:00:00.004Z")
        ),
        regexFn
    );
    DimFilter intervalFilter4 = new IntervalDimFilter(
        ColumnHolder.TIME_COLUMN_NAME,
        Arrays.asList(
            Intervals.of("1970-01-01T00:00:00.001Z/1970-01-01T00:00:00.004Z"),
            Intervals.of("1976-01-01T00:00:00.001Z/1980-01-01T00:00:00.004Z")
        ),
        regexFn
    );
    Assert.assertNotEquals(intervalFilter3.getCacheKey(), intervalFilter4.getCacheKey());
  }

  @Test
  public void testHashCode()
  {
    RegexDimExtractionFn regexFn = new RegexDimExtractionFn(".*", false, null);

    DimFilter intervalFilter1 = new IntervalDimFilter(
        ColumnHolder.TIME_COLUMN_NAME,
        Arrays.asList(
            Intervals.of("1970-01-01T00:00:00.001Z/1970-01-01T00:00:00.004Z"),
            Intervals.of("1975-01-01T00:00:00.001Z/1980-01-01T00:00:00.004Z")
        ),
        null
    );

    DimFilter intervalFilter2 = new IntervalDimFilter(
        ColumnHolder.TIME_COLUMN_NAME,
        Arrays.asList(
            Intervals.of("1970-01-01T00:00:00.001Z/1970-01-01T00:00:00.004Z"),
            Intervals.of("1975-01-01T00:00:00.001Z/1980-01-01T00:00:00.004Z")
        ),
        regexFn
    );

    DimFilter intervalFilter3 = new IntervalDimFilter(
        ColumnHolder.TIME_COLUMN_NAME,
        Arrays.asList(
            Intervals.of("1970-01-01T00:00:00.001Z/1970-01-01T00:00:00.004Z"),
            Intervals.of("1977-01-01T00:00:00.001Z/1980-01-01T00:00:00.004Z")
        ),
        null
    );

    Assert.assertNotEquals(intervalFilter1.hashCode(), intervalFilter2.hashCode());
    Assert.assertNotEquals(intervalFilter1.hashCode(), intervalFilter3.hashCode());

    DimFilter intervalFilter4 = new IntervalDimFilter(
        ColumnHolder.TIME_COLUMN_NAME,
        Arrays.asList(
            Intervals.of("1970-01-01T00:00:00.001Z/1970-01-01T00:00:00.004Z"),
            Intervals.of("1975-01-01T00:00:00.001Z/1977-01-01T00:00:00.004Z"),
            Intervals.of("1976-01-01T00:00:00.001Z/1980-01-01T00:00:00.004Z")
        ),
        null
    );

    Assert.assertEquals(intervalFilter1.hashCode(), intervalFilter4.hashCode());

    DimFilter intervalFilter5 = new IntervalDimFilter(
        "__thyme",
        Arrays.asList(
            Intervals.of("1970-01-01T00:00:00.001Z/1970-01-01T00:00:00.004Z"),
            Intervals.of("1975-01-01T00:00:00.001Z/1980-01-01T00:00:00.004Z")
        ),
        null
    );
    Assert.assertNotEquals(intervalFilter1.hashCode(), intervalFilter5.hashCode());
  }

  @Test
  public void testEquals()
  {
    RegexDimExtractionFn regexFn = new RegexDimExtractionFn(".*", false, null);

    DimFilter intervalFilter1 = new IntervalDimFilter(
        ColumnHolder.TIME_COLUMN_NAME,
        Arrays.asList(
            Intervals.of("1970-01-01T00:00:00.001Z/1970-01-01T00:00:00.004Z"),
            Intervals.of("1975-01-01T00:00:00.001Z/1980-01-01T00:00:00.004Z")
        ),
        null
    );

    DimFilter intervalFilter2 = new IntervalDimFilter(
        ColumnHolder.TIME_COLUMN_NAME,
        Arrays.asList(
            Intervals.of("1970-01-01T00:00:00.001Z/1970-01-01T00:00:00.004Z"),
            Intervals.of("1975-01-01T00:00:00.001Z/1980-01-01T00:00:00.004Z")
        ),
        regexFn
    );

    DimFilter intervalFilter3 = new IntervalDimFilter(
        ColumnHolder.TIME_COLUMN_NAME,
        Arrays.asList(
            Intervals.of("1970-01-01T00:00:00.001Z/1970-01-01T00:00:00.004Z"),
            Intervals.of("1977-01-01T00:00:00.001Z/1980-01-01T00:00:00.004Z")
        ),
        null
    );

    Assert.assertNotEquals(intervalFilter1, intervalFilter2);
    Assert.assertNotEquals(intervalFilter1, intervalFilter3);

    DimFilter intervalFilter4 = new IntervalDimFilter(
        ColumnHolder.TIME_COLUMN_NAME,
        Arrays.asList(
            Intervals.of("1970-01-01T00:00:00.001Z/1970-01-01T00:00:00.004Z"),
            Intervals.of("1975-01-01T00:00:00.001Z/1977-01-01T00:00:00.004Z"),
            Intervals.of("1976-01-01T00:00:00.001Z/1980-01-01T00:00:00.004Z")
        ),
        null
    );
    Assert.assertEquals(intervalFilter1, intervalFilter4);

    DimFilter intervalFilter5 = new IntervalDimFilter(
        "__thyme",
        Arrays.asList(
            Intervals.of("1970-01-01T00:00:00.001Z/1970-01-01T00:00:00.004Z"),
            Intervals.of("1975-01-01T00:00:00.001Z/1980-01-01T00:00:00.004Z")
        ),
        null
    );
    Assert.assertNotEquals(intervalFilter1, intervalFilter5);
  }

  @Test
  public void testGetRequiredColumns()
  {
    DimFilter intervalFilter = new IntervalDimFilter(
        ColumnHolder.TIME_COLUMN_NAME,
        Arrays.asList(
            Intervals.of("1970-01-01T00:00:00.001Z/1970-01-01T00:00:00.004Z"),
            Intervals.of("1975-01-01T00:00:00.001Z/1980-01-01T00:00:00.004Z")
        ),
        null
    );
    Assert.assertEquals(intervalFilter.getRequiredColumns(), Sets.newHashSet(ColumnHolder.TIME_COLUMN_NAME));
  }
}
