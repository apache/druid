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

package org.apache.druid.query.aggregation.ddsketch;

import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.query.aggregation.PostAggregator;
import org.apache.druid.query.aggregation.post.ConstantPostAggregator;
import org.junit.Assert;
import org.junit.Test;

public class DDSketchToQuantilePostAggregatorTest
{
  @Test
  public void testSerde() throws Exception
  {
    DDSketchToQuantilePostAggregator there =
        new DDSketchToQuantilePostAggregator("post", new ConstantPostAggregator("", 100), 0.5);

    DefaultObjectMapper mapper = new DefaultObjectMapper();
    DDSketchToQuantilePostAggregator andBackAgain = mapper.readValue(
        mapper.writeValueAsString(there),
        DDSketchToQuantilePostAggregator.class
    );

    Assert.assertEquals(there, andBackAgain);
    Assert.assertArrayEquals(there.getCacheKey(), andBackAgain.getCacheKey());
    Assert.assertEquals(there.getDependentFields(), andBackAgain.getDependentFields());
  }

  @Test
  public void testToString()
  {
    PostAggregator postAgg =
        new DDSketchToQuantilePostAggregator("post", new ConstantPostAggregator("", 100), 0.5);

    Assert.assertEquals(
        "DDSketchToQuantilePostAggregator{name='post', field=ConstantPostAggregator{name='', constantValue=100}, fraction=0.5}",
        postAgg.toString()
    );
  }

  @Test
  public void testEquals()
  {
    EqualsVerifier.forClass(DDSketchToQuantilePostAggregator.class)
                  .withNonnullFields("name", "field", "fraction")
                  .usingGetClass()
                  .verify();
  }
}
