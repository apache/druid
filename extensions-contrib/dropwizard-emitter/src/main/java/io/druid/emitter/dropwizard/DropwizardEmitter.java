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
import com.metamx.emitter.core.Emitter;
import com.metamx.emitter.core.Event;
import com.metamx.emitter.service.AlertEvent;
import com.metamx.emitter.service.ServiceMetricEvent;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.logger.Logger;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;

public class DropwizardEmitter implements Emitter {
    private static Logger log = new Logger(DropwizardEmitter.class);
    private final MetricRegistry metricsRegistry = new MetricRegistry();
    private final DropwizardMetricManager dropwizardMetricManager;
    private final AtomicBoolean started = new AtomicBoolean(false);
    private final DruidToDropwizardEventConverter druidToDropwizardEventConverter;
    private final List<Emitter> emitterList;
    //TODO make this a list of reporters
    private final DropwizardReporter dropwizardReporter;

    public DropwizardEmitter(DropwizardEmitterConfig dropwizardEmitterConfig,List<Emitter> emitterList) {
        this.dropwizardMetricManager = dropwizardEmitterConfig.getDropwizardMetricManager();
        this.druidToDropwizardEventConverter = dropwizardEmitterConfig.getDruidToDropwizardEventConverter();
        this.emitterList = emitterList;
        this.dropwizardReporter = dropwizardEmitterConfig.getDropwizardReporter();
    }

    @Override
    public void start() {
        try {
            startReporters();
        } catch (IOException e) {
            log.error(e,"Error while starting Dropwizard reporters");
        }
        started.set(true);
    }

    @Override
    public void emit(Event event) {
        if (!started.get()) {
            throw new ISE("Emit was called while emitter is yet not initialized");
        }
        if (event instanceof ServiceMetricEvent) {
            DropwizardEvent dropwizardEvent = druidToDropwizardEventConverter.druidEventToDropwizard((ServiceMetricEvent) event);
            if(dropwizardEvent!=null) {
                dropwizardMetricManager.updateMetric(metricsRegistry,dropwizardEvent);
            }else{
                log.debug("Dropping the service event "+event);
                return;
            }
        } else if (!emitterList.isEmpty() && event instanceof AlertEvent) {
            for (Emitter emitter : emitterList) {
                emitter.emit(event);
            }
        } else if (event instanceof AlertEvent) {
            AlertEvent alertEvent = (AlertEvent) event;
            log.error(
                    "The following alert is dropped, description is [%s], severity is [%s]",
                    alertEvent.getDescription(), alertEvent.getSeverity()
            );
        } else {
            log.error("unknown event type [%s]", event.getClass());
        }
    }

    @Override
    public void flush() throws IOException {
        dropwizardReporter.flush();
    }

    @Override
    public void close() throws IOException {
        dropwizardReporter.close();
    }

    private void startReporters() throws IOException {
        dropwizardReporter.start(metricsRegistry);
    }

    public static String sanitize(String namespace)
    {
        Pattern DOT_OR_WHITESPACE = Pattern.compile("[\\s]+|[.]+");
        return DOT_OR_WHITESPACE.matcher(namespace).replaceAll("_");
    }
}
