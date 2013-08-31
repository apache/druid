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

package io.druid.indexing.common.task;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.timeline.DataSegment;
import io.druid.timeline.partition.NoneShardSpec;
import junit.framework.Assert;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.Test;

/**
 */
public class VersionConverterTaskTest
{
  @Test
  public void testSerializationSimple() throws Exception
  {
    final String dataSource = "billy";
    final Interval interval = new Interval(new DateTime().minus(1000), new DateTime());

    DefaultObjectMapper jsonMapper = new DefaultObjectMapper();

    VersionConverterTask task = VersionConverterTask.create(dataSource, interval);

    Task task2 = jsonMapper.readValue(jsonMapper.writerWithDefaultPrettyPrinter().writeValueAsString(task), Task.class);
    Assert.assertEquals(task, task2);

    DataSegment segment = new DataSegment(
        dataSource,
        interval,
        new DateTime().toString(),
        ImmutableMap.<String, Object>of(),
        ImmutableList.<String>of(),
        ImmutableList.<String>of(),
        new NoneShardSpec(),
        9,
        102937
    );

    task = VersionConverterTask.create(segment);

    task2 = jsonMapper.readValue(jsonMapper.writerWithDefaultPrettyPrinter().writeValueAsString(task), Task.class);
    Assert.assertEquals(task, task2);
  }
}
