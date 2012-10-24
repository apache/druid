/*
 * Druid - a distributed column store.
 * Copyright (C) 2012  Metamarkets Group Inc.
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

package com.metamx.druid.indexer;

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.metamx.druid.indexer.granularity.UniformGranularitySpec;
import com.metamx.druid.jackson.DefaultObjectMapper;
import org.codehaus.jackson.map.ObjectMapper;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;

public class HadoopDruidIndexerConfigTest
{
  private static final ObjectMapper jsonMapper = new DefaultObjectMapper();

  @Test
  public void testGranularitySpec() {
    final HadoopDruidIndexerConfig cfg;

    try {
      cfg = jsonMapper.readValue(
          "{"
          + "\"granularitySpec\":{"
          + "   \"type\":\"uniform\","
          + "   \"gran\":\"hour\","
          + "   \"intervals\":[\"2012-01-01/P1D\"]"
          + " }"
          + "}",
          HadoopDruidIndexerConfig.class
      );
    } catch(Exception e) {
      throw Throwables.propagate(e);
    }

    final UniformGranularitySpec granularitySpec = (UniformGranularitySpec) cfg.getGranularitySpec();

    Assert.assertEquals(
        "getIntervals",
        Lists.newArrayList(new Interval("2012-01-01/P1D")),
        granularitySpec.getIntervals()
    );

    Assert.assertEquals(
        "getGranularity",
        "HOUR",
        granularitySpec.getGranularity().toString()
    );
  }

  @Test
  public void testIntervalsAndSegmentGranularity() {
    // Deprecated and replaced by granularitySpec, but still supported
    final HadoopDruidIndexerConfig cfg;

    try {
      cfg = jsonMapper.readValue(
          "{"
          + "\"segmentGranularity\":\"day\","
          + "\"intervals\":[\"2012-02-01/P1D\"]"
          + "}",
          HadoopDruidIndexerConfig.class
      );
    } catch(Exception e) {
      throw Throwables.propagate(e);
    }

    final UniformGranularitySpec granularitySpec = (UniformGranularitySpec) cfg.getGranularitySpec();

    Assert.assertEquals(
        "getIntervals",
        Lists.newArrayList(new Interval("2012-02-01/P1D")),
        granularitySpec.getIntervals()
    );

    Assert.assertEquals(
        "getGranularity",
        "DAY",
        granularitySpec.getGranularity().toString()
    );
  }


  @Test
  public void testCmdlineAndSegmentGranularity() {
    // Deprecated and replaced by granularitySpec, but still supported
    final HadoopDruidIndexerConfig cfg;

    try {
      cfg = jsonMapper.readValue(
          "{"
          + "\"segmentGranularity\":\"day\""
          + "}",
          HadoopDruidIndexerConfig.class
      );
    } catch(Exception e) {
      throw Throwables.propagate(e);
    }

    cfg.setIntervals(Lists.newArrayList(new Interval("2012-03-01/P1D")));

    final UniformGranularitySpec granularitySpec = (UniformGranularitySpec) cfg.getGranularitySpec();

    Assert.assertEquals(
        "getIntervals",
        Lists.newArrayList(new Interval("2012-03-01/P1D")),
        granularitySpec.getIntervals()
    );

    Assert.assertEquals(
        "getGranularity",
        "DAY",
        granularitySpec.getGranularity().toString()
    );
  }

  @Test
  public void testInvalidCombination() {
    boolean thrown = false;
    try {
      final HadoopDruidIndexerConfig cfg = jsonMapper.readValue(
          "{"
          + "\"segmentGranularity\":\"day\","
          + "\"intervals\":[\"2012-02-01/P1D\"],"
          + "\"granularitySpec\":{"
          + "   \"type\":\"uniform\","
          + "   \"gran\":\"hour\","
          + "   \"intervals\":[\"2012-01-01/P1D\"]"
          + " }"
          + "}",
          HadoopDruidIndexerConfig.class
      );
    } catch(Exception e) {
      thrown = true;
    }

    Assert.assertTrue("Exception thrown", thrown);
  }
}
