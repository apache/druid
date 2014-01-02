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

package io.druid.examples.conjure;

import io.druid.data.input.Firehose;
import io.druid.data.input.InputRow;
import junit.framework.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

public class ConjureFirehoseFactoryTest
{
  private ConjureFirehoseFactory conjureFirehoseFactory;

  @Before
  public void setUp()
  {
    String file = ConjureFirehoseFactory.class.getResource("/conjure.json").getFile();
    conjureFirehoseFactory = new ConjureFirehoseFactory(file, 5000L, 50L, 2L);
  }

  @Test
  public void testDimensions() throws IOException
  {
    Set<Integer> intDim = new LinkedHashSet<>();
    Set<Double> doubleDim = new LinkedHashSet<>();
    Set<String> stringDim = new LinkedHashSet<>();
    Set<Long> longDim = new LinkedHashSet<>();

    Firehose firehose = conjureFirehoseFactory.connect();
    InputRow row;
    if (firehose.hasMore()) {
      row = firehose.nextRow();
    } else {
      throw new RuntimeException("No events to supply?");
    }

    List<String> dimensions = row.getDimensions();
    Assert.assertEquals("Total number of dims should be 5", 5, dimensions.size());

    List<String> sorted = Arrays.asList("Double150Card0", "Int10Card0", "Long200Card0", "String100Card0", "timestamp");

    Collections.sort(dimensions);
    Assert.assertEquals("Dimensions should be same.", dimensions, sorted);

    Assert.assertEquals("Double dimension.", row.getRaw("Double150Card0").getClass(), Double.class);
    Assert.assertEquals("String dimension.", row.getRaw("String100Card0").getClass(), String.class);
    Assert.assertEquals("Int dimension.", row.getRaw("Int10Card0").getClass(), Integer.class);
    Assert.assertEquals("Long dimension.", row.getRaw("Long200Card0").getClass(), Long.class);

    int count = 1;
    while (firehose.hasMore()) {
      row = firehose.nextRow();
      doubleDim.add((Double) row.getRaw("Double150Card0"));
      stringDim.add((String) row.getRaw("String100Card0"));
      longDim.add((Long) row.getRaw("Long200Card0"));
      intDim.add((Integer) row.getRaw("Int10Card0"));
      count++;
    }

    Assert.assertEquals("Double dim cardinality.", 150, doubleDim.size());
    Assert.assertEquals("Integer dim cardinality.", 10, intDim.size());
    Assert.assertEquals("Long dim cardinality.", 200, longDim.size());
    Assert.assertEquals("String dim cardinality.", 100, stringDim.size());
    Assert.assertEquals("Total events.", 5000, count);
  }
}
