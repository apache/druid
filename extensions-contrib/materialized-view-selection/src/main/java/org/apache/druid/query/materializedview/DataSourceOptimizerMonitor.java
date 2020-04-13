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

import com.google.inject.Inject;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.java.util.metrics.AbstractMonitor;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class DataSourceOptimizerMonitor extends AbstractMonitor 
{
  private final DataSourceOptimizer optimizer;

  @Inject
  public DataSourceOptimizerMonitor(DataSourceOptimizer optimizer)
  {
    this.optimizer = optimizer;
  }

  @Override
  public boolean doMonitor(ServiceEmitter emitter) 
  {
    final List<DataSourceOptimizerStats> stats = optimizer.getAndResetStats();
    for (DataSourceOptimizerStats stat : stats) {
      final ServiceMetricEvent.Builder builder = new ServiceMetricEvent.Builder();
      builder.setDimension("dataSource", stat.getBase());
      emitter.emit(builder.build("/materialized/view/query/totalNum", stat.getTotalcount()));
      emitter.emit(builder.build("/materialized/view/query/hits", stat.getHitcount()));
      emitter.emit(builder.build("/materialized/view/query/hitRate", stat.getHitRate()));
      emitter.emit(builder.build("/materialized/view/select/avgCostMS", stat.getOptimizerCost()));
      Map<String, Long> derivativesStats = stat.getDerivativesHitCount();
      for (Map.Entry<String, Long> derivative : derivativesStats.entrySet()) {
        builder.setDimension("derivative", derivative.getKey());
        emitter.emit(builder.build("/materialized/view/derivative/numSelected", derivative.getValue()));
      }
      final ServiceMetricEvent.Builder builder2 = new ServiceMetricEvent.Builder();
      builder2.setDimension("dataSource", stat.getBase());
      for (Set<String> fields : stat.getMissFields().keySet()) {
        builder2.setDimension("fields", fields.toString());
        emitter.emit(builder2.build("/materialized/view/missNum", stat.getMissFields().get(fields).get()));
      }
    }
    return true;
  }
}
