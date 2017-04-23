/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package io.druid.emitter.dropwizard;

import com.codahale.metrics.MetricRegistry;
import io.druid.java.util.common.logger.Logger;
import io.druid.math.expr.Expr;


public class HistogramMetricManager implements DropwizardMetricManager {
    private static Logger log = new Logger(HistogramMetricManager.class);
    @Override
    public void updateMetric(MetricRegistry metricsRegistry,DropwizardEvent dropwizardEvent) {
        if(dropwizardEvent==null){
            log.error("Dropwizard event passed to " + getClass() + " is null");
            return;
        }
        metricsRegistry.histogram(dropwizardEvent.getMetricName()).update(dropwizardEvent.getValue().longValue());
    }

    @Override
    public boolean equals(Object o){
        return true;
    }

    @Override
    public int hashCode() {
        return 1;
    }
}
