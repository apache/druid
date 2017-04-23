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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collections;
import java.util.List;


public class DropwizardEmitterConfig {
    @JsonProperty("eventConverter")
    final private DruidToDropwizardEventConverter druidToDropwizardEventConverter;
    @JsonProperty("reporter")
    final private DropwizardReporter dropwizardReporter;
    @JsonProperty("metric")
    final private DropwizardMetricManager dropwizardMetricManager;
    @JsonProperty
    final private List<String> alertEmitters;

    @JsonCreator
    public DropwizardEmitterConfig(@JsonProperty("reporter") DropwizardReporter dropwizardReporter ,@JsonProperty("eventConverter") DruidToDropwizardEventConverter druidToDropwizardEventConverter,@JsonProperty("metric") DropwizardMetricManager dropwizardMetricManager,@JsonProperty("alertEmitters") List<String> alertEmitters ) {
        this.dropwizardReporter = dropwizardReporter;
        this.druidToDropwizardEventConverter = druidToDropwizardEventConverter;
        this.dropwizardMetricManager = dropwizardMetricManager==null? new HistogramMetricManager():dropwizardMetricManager;
        this.alertEmitters = alertEmitters == null ? Collections.<String>emptyList() : alertEmitters;
    }


    public DruidToDropwizardEventConverter getDruidToDropwizardEventConverter() {
        return druidToDropwizardEventConverter;
    }

    public DropwizardReporter getDropwizardReporter() {
        return dropwizardReporter;
    }

    public List<String> getAlertEmitters() {
        return alertEmitters;
    }

    public DropwizardMetricManager getDropwizardMetricManager() {
        return dropwizardMetricManager;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        DropwizardEmitterConfig that = (DropwizardEmitterConfig) o;

        if (alertEmitters != null ? !alertEmitters.equals(that.alertEmitters) : that.alertEmitters != null)
            return false;
        if (dropwizardMetricManager != null ? !dropwizardMetricManager.equals(that.dropwizardMetricManager) : that.dropwizardMetricManager != null)
            return false;
        if (dropwizardReporter != null ? !dropwizardReporter.equals(that.dropwizardReporter) : that.dropwizardReporter != null)
            return false;
        if (druidToDropwizardEventConverter != null ? !druidToDropwizardEventConverter.equals(that.druidToDropwizardEventConverter) : that.druidToDropwizardEventConverter != null)
            return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = druidToDropwizardEventConverter != null ? druidToDropwizardEventConverter.hashCode() : 0;
        result = 31 * result + (dropwizardReporter != null ? dropwizardReporter.hashCode() : 0);
        result = 31 * result + (dropwizardMetricManager != null ? dropwizardMetricManager.hashCode() : 0);
        result = 31 * result + (alertEmitters != null ? alertEmitters.hashCode() : 0);
        return result;
    }
}
