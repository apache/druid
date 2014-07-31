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

package io.druid.query.aggregation.histogram;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.druid.jackson.DefaultObjectMapper;
import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;

public class QuantilesTest
{
  @Test
  public void testSerialization() throws Exception
  {
    ObjectMapper mapper = new DefaultObjectMapper();

    float[] probabilities = new float[]{0.25f, 0.5f, 0.75f};
    float[] quantiles = new float[]{0.25f, 0.5f, 0.75f};
    float min = 0f;
    float max = 4f;

    String theString = mapper.writeValueAsString(
        new Quantiles(probabilities, quantiles, min, max)
    );

    Object theObject = mapper.readValue(theString, Object.class);
    Assert.assertThat(theObject, CoreMatchers.instanceOf(LinkedHashMap.class));

    LinkedHashMap theMap = (LinkedHashMap) theObject;

    ArrayList theProbabilities = (ArrayList<Float>) theMap.get("probabilities");

    Assert.assertEquals(probabilities.length, theProbabilities.size());
    for (int i = 0; i < theProbabilities.size(); ++i) {
      Assert.assertEquals(probabilities[i], ((Number) theProbabilities.get(i)).floatValue(), 0.0001f);
    }

    ArrayList theQuantiles = (ArrayList<Float>) theMap.get("quantiles");

    Assert.assertEquals(quantiles.length, theQuantiles.size());
    for (int i = 0; i < theQuantiles.size(); ++i) {
      Assert.assertEquals(quantiles[i], ((Number) theQuantiles.get(i)).floatValue(), 0.0001f);
    }

    Assert.assertEquals(
        "serialized min. matches expected min.",
        min,
        ((Number) theMap.get("min")).floatValue(),
        0.0001f
    );
    Assert.assertEquals(
        "serialized max. matches expected max.",
        max,
        ((Number) theMap.get("max")).floatValue(),
        0.0001f
    );


  }
}
