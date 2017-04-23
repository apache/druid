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
package io.druid.emitter.dropwizard.eventconverters;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedSet;
import com.metamx.emitter.service.ServiceMetricEvent;
import io.druid.emitter.dropwizard.DropwizardEmitter;
import io.druid.emitter.dropwizard.DropwizardEvent;
import io.druid.emitter.dropwizard.DruidToDropwizardEventConverter;


public class SendAllDropwizardEventConverter implements DruidToDropwizardEventConverter {
    @JsonProperty
    private  boolean ignoreHostname;
    @JsonProperty
    private  boolean ignoreServiceName;
    @JsonProperty
    private final String namespacePrefix;

    public String getNamespacePrefix() {
        return namespacePrefix;
    }

    @JsonCreator
    public SendAllDropwizardEventConverter(
            @JsonProperty("namespacePrefix") String namespacePrefix,
            @JsonProperty("ignoreHostname") Boolean ignoreHostname,
            @JsonProperty("ignoreServiceName") Boolean ignoreServiceName
    )
    {
        this.ignoreHostname = ignoreHostname == null ? false : ignoreHostname;
        this.ignoreServiceName = ignoreServiceName == null ? false : ignoreServiceName;
        this.namespacePrefix = namespacePrefix;
    }

    @JsonProperty
    public void setIgnoreHostname(boolean ignoreHostname) {
        this.ignoreHostname = ignoreHostname;
    }

    @JsonProperty
    public void setIgnoreServiceName(boolean ignoreServiceName) {
        this.ignoreServiceName = ignoreServiceName;
    }

    @JsonProperty
    public boolean isIgnoreServiceName()
    {
        return ignoreServiceName;
    }

    @JsonProperty
    public boolean isIgnoreHostname()
    {
        return ignoreHostname;
    }

    @Override
    public DropwizardEvent druidEventToDropwizard(ServiceMetricEvent serviceMetricEvent) {
        ImmutableList.Builder metricPathBuilder = new ImmutableList.Builder<String>();

        if (!Strings.isNullOrEmpty(this.getNamespacePrefix()))
            metricPathBuilder.add(this.getNamespacePrefix());

        if (!this.isIgnoreServiceName()) {
            metricPathBuilder.add(DropwizardEmitter.sanitize(serviceMetricEvent.getService()));
        }
        if (!this.isIgnoreHostname()) {
            metricPathBuilder.add(DropwizardEmitter.sanitize(serviceMetricEvent.getHost()));
        }

        ImmutableSortedSet<String> dimNames = ImmutableSortedSet.copyOf(serviceMetricEvent.getUserDims().keySet());
        for (String dimName : dimNames) {
            metricPathBuilder.add(DropwizardEmitter.sanitize(String.valueOf(serviceMetricEvent.getUserDims()
                    .get(dimName))));
        }
        metricPathBuilder.add(DropwizardEmitter.sanitize(serviceMetricEvent.getMetric()));

        return new DropwizardEvent(Joiner.on(".").join(metricPathBuilder.build()),
                serviceMetricEvent.getValue().doubleValue());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()){
            return false;
        }

        SendAllDropwizardEventConverter that = (SendAllDropwizardEventConverter) o;

        if (ignoreHostname != that.ignoreHostname) {
            return false;
        }
        if (ignoreServiceName != that.ignoreServiceName) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = (ignoreHostname ? 1 : 0);
        result = 31 * result + (ignoreServiceName ? 1 : 0);
        return result;
    }
}
