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

package org.apache.druid.emitter.prometheus;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Getter;
import com.google.common.base.Strings;
import org.apache.druid.emitter.prometheus.metrics.Metric;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.logger.Logger;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.Collections;
import java.util.Map;

public class Metrics {
    private static final Logger log = new Logger(Metrics.class);

    private final ObjectMapper mapper = new ObjectMapper();
    @Getter
    private final Map<String, Metric<?>> registeredMetrics;

    public Metrics(PrometheusEmitterConfig emitterConfig) {
        this.registeredMetrics = Collections.unmodifiableMap(readConfig(
                emitterConfig.getDimensionMapPath()));
        registeredMetrics.forEach((name, metric) -> metric.createCollector(name, emitterConfig));
    }

    private Map<String, Metric<?>> readConfig(@Nullable String path) {
       try {
           InputStream is;
           if (Strings.isNullOrEmpty(path)) {
               log.info("Using default metric configuration");
               is = this.getClass().getClassLoader().getResourceAsStream("defaultMetrics.json");
           } else {
              log.info("Using metric configuration at [%s]", path);
              is = Files.newInputStream(new File(path).toPath());
           }
           return mapper.readerFor(new TypeReference<Map<String, Metric<?>>>() {
           }).readValue(is);
       } catch(IOException e) {
           throw new ISE(e, "Failed to parse metric configuration.");
       }
    }

    public Metric<?> getByName(String name, String service) {
        if (registeredMetrics.containsKey(name)) {
            return registeredMetrics.get(name);
        } else {
            return registeredMetrics.getOrDefault(service + "_" + name, null);
        }
    }
}
