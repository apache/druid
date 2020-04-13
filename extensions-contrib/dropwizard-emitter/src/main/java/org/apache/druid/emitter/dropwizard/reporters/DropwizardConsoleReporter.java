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

package org.apache.druid.emitter.dropwizard.reporters;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.emitter.dropwizard.DropwizardReporter;

import java.util.concurrent.TimeUnit;

public class DropwizardConsoleReporter implements DropwizardReporter
{
  private long emitIntervalInSecs;
  private TimeUnit rates = TimeUnit.SECONDS;
  private TimeUnit durations = TimeUnit.MILLISECONDS;
  private ConsoleReporter consoleReporter;

  @JsonProperty
  public long getEmitIntervalInSecs()
  {
    return emitIntervalInSecs;
  }

  @JsonProperty
  public void setEmitIntervalInSecs(long emitIntervalInSecs)
  {
    this.emitIntervalInSecs = emitIntervalInSecs;
  }

  @JsonProperty
  public TimeUnit getRates()
  {
    return rates;
  }

  @JsonProperty
  public void setRates(String rates)
  {
    this.rates = TimeUnit.valueOf(rates);
  }

  @JsonProperty
  public TimeUnit getDurations()
  {
    return durations;
  }

  @JsonProperty
  public void setDurations(String durations)
  {
    this.durations = TimeUnit.valueOf(durations);
  }

  @Override
  public void start(MetricRegistry metricRegistry)
  {
    consoleReporter = ConsoleReporter.forRegistry(metricRegistry)
                                     .convertDurationsTo(durations)
                                     .convertRatesTo(rates)
                                     .build();
    consoleReporter.start(emitIntervalInSecs, TimeUnit.SECONDS);

  }

  @Override
  public void flush()
  {
    // no-op
  }

  @Override
  public void close()
  {
    consoleReporter.stop();
  }


  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    DropwizardConsoleReporter that = (DropwizardConsoleReporter) o;

    if (emitIntervalInSecs != that.emitIntervalInSecs) {
      return false;
    }
    if (consoleReporter != null ? !consoleReporter.equals(that.consoleReporter) : that.consoleReporter != null) {
      return false;
    }
    if (durations != that.durations) {
      return false;
    }
    if (rates != that.rates) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    int result = (int) (emitIntervalInSecs ^ (emitIntervalInSecs >>> 32));
    result = 31 * result + (rates != null ? rates.hashCode() : 0);
    result = 31 * result + (durations != null ? durations.hashCode() : 0);
    result = 31 * result + (consoleReporter != null ? consoleReporter.hashCode() : 0);
    return result;
  }

  @Override
  public String toString()
  {
    return "DropwizardConsoleReporter{" +
           "emitIntervalInSecs=" + emitIntervalInSecs +
           ", rates=" + rates +
           ", durations=" + durations +
           '}';
  }
}
