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

package org.apache.druid.query.aggregation.datasketches.theta;

import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.query.aggregation.PostAggregator;
import org.apache.druid.query.aggregation.TestObjectColumnSelector;
import org.apache.druid.query.aggregation.post.FieldAccessPostAggregator;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class SketchToStringPostAggregatorTest
{

  @Test
  public void test()
  {
    // not going to iterate over the selector since getting a summary of an empty sketch is sufficient
    final TestObjectColumnSelector selector = new TestObjectColumnSelector(new Object[0]);
    final Aggregator agg = new SketchAggregator(selector, 4096);

    final Map<String, Object> fields = new HashMap<>();
    fields.put("sketch", agg.get());

    final PostAggregator postAgg = new SketchToStringPostAggregator(
        "summary",
        new FieldAccessPostAggregator("field", "sketch")
    );

    final String summary = (String) postAgg.compute(fields);
    Assert.assertNotNull(summary);
    Assert.assertTrue(summary.contains("SUMMARY"));
  }

}
