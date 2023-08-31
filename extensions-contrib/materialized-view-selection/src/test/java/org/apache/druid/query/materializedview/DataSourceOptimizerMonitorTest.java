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

package org.apache.druid.query.materializedview;

import org.apache.druid.java.util.metrics.StubServiceEmitter;
import org.easymock.EasyMock;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

public class DataSourceOptimizerMonitorTest
{

  @Test
  public void testDoMonitor()
  {
    final DataSourceOptimizerStats wikiStats
        = new DataSourceOptimizerStats("wiki", 100, 200, 30, Collections.emptyMap(), Collections.emptyMap());
    final DataSourceOptimizerStats koalaStats
        = new DataSourceOptimizerStats("koala", 35, 62, 13, Collections.emptyMap(), Collections.emptyMap());

    DataSourceOptimizer optimizer = EasyMock.createMock(DataSourceOptimizer.class);
    EasyMock.expect(optimizer.getAndResetStats()).andReturn(
        Arrays.asList(wikiStats, koalaStats)
    ).anyTimes();
    EasyMock.replay(optimizer);

    DataSourceOptimizerMonitor monitor = new DataSourceOptimizerMonitor(optimizer);
    StubServiceEmitter emitter = new StubServiceEmitter("test", "localhost");
    monitor.doMonitor(emitter);

    // Verify metrics emitted for all the datasources
    Map<String, Object> wikiFilter = Collections.singletonMap("dataSource", "wiki");
    emitter.verifyValue("/materialized/view/query/totalNum", wikiFilter, wikiStats.getTotalcount());
    emitter.verifyValue("/materialized/view/query/hits", wikiFilter, wikiStats.getHitcount());
    emitter.verifyValue("/materialized/view/query/hitRate", wikiFilter, wikiStats.getHitRate());
    emitter.verifyValue("/materialized/view/select/avgCostMS", wikiFilter, wikiStats.getOptimizerCost());

    Map<String, Object> koalaFilter = Collections.singletonMap("dataSource", "koala");
    emitter.verifyValue("/materialized/view/query/totalNum", koalaFilter, koalaStats.getTotalcount());
    emitter.verifyValue("/materialized/view/query/hits", koalaFilter, koalaStats.getHitcount());
    emitter.verifyValue("/materialized/view/query/hitRate", koalaFilter, koalaStats.getHitRate());
    emitter.verifyValue("/materialized/view/select/avgCostMS", koalaFilter, koalaStats.getOptimizerCost());


    EasyMock.verify(optimizer);
  }

}
