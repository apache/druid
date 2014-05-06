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

package io.druid.segment.indexing.granularity;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.metamx.common.Granularity;
import io.druid.jackson.DefaultObjectMapper;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;

public class UniformGranularityTest
{
  private static final ObjectMapper jsonMapper = new DefaultObjectMapper();

  @Test
  public void testSimple()
  {
    final GranularitySpec spec = new UniformGranularitySpec(
        Granularity.DAY,
        null,
        Lists.newArrayList(
            new Interval("2012-01-08T00Z/2012-01-11T00Z"),
            new Interval("2012-01-07T00Z/2012-01-08T00Z"),
            new Interval("2012-01-03T00Z/2012-01-04T00Z"),
            new Interval("2012-01-01T00Z/2012-01-03T00Z")
        ),
        null
    );

    Assert.assertEquals(
        Lists.newArrayList(
            new Interval("2012-01-01T00Z/P1D"),
            new Interval("2012-01-02T00Z/P1D"),
            new Interval("2012-01-03T00Z/P1D"),
            new Interval("2012-01-07T00Z/P1D"),
            new Interval("2012-01-08T00Z/P1D"),
            new Interval("2012-01-09T00Z/P1D"),
            new Interval("2012-01-10T00Z/P1D")
        ),
        Lists.newArrayList(spec.bucketIntervals().get())
    );

    Assert.assertEquals(
        "2012-01-03T00Z",
        Optional.of(new Interval("2012-01-03T00Z/2012-01-04T00Z")),
        spec.bucketInterval(new DateTime("2012-01-03T00Z"))
    );

    Assert.assertEquals(
        "2012-01-03T01Z",
        Optional.of(new Interval("2012-01-03T00Z/2012-01-04T00Z")),
        spec.bucketInterval(new DateTime("2012-01-03T01Z"))
    );

    Assert.assertEquals(
        "2012-01-04T01Z",
        Optional.<Interval>absent(),
        spec.bucketInterval(new DateTime("2012-01-04T01Z"))
    );

    Assert.assertEquals(
        "2012-01-07T23:59:59.999Z",
        Optional.of(new Interval("2012-01-07T00Z/2012-01-08T00Z")),
        spec.bucketInterval(new DateTime("2012-01-07T23:59:59.999Z"))
    );

    Assert.assertEquals(
        "2012-01-08T01Z",
        Optional.of(new Interval("2012-01-08T00Z/2012-01-09T00Z")),
        spec.bucketInterval(new DateTime("2012-01-08T01Z"))
    );
  }

  @Test
  public void testJson()
  {
    final GranularitySpec spec = new UniformGranularitySpec(
        Granularity.DAY,
        null,
        Lists.newArrayList(
            new Interval("2012-01-08T00Z/2012-01-11T00Z"),
            new Interval("2012-01-07T00Z/2012-01-08T00Z"),
            new Interval("2012-01-03T00Z/2012-01-04T00Z"),
            new Interval("2012-01-01T00Z/2012-01-03T00Z")
        ),
        null
    );

    try {
      final GranularitySpec rtSpec = jsonMapper.readValue(jsonMapper.writeValueAsString(spec), GranularitySpec.class);
      Assert.assertEquals(
          "Round-trip bucketIntervals",
          spec.bucketIntervals(),
          rtSpec.bucketIntervals()
      );
      Assert.assertEquals(
          "Round-trip granularity",
          spec.getSegmentGranularity(),
          rtSpec.getSegmentGranularity()
      );
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }
}
