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

package io.druid.indexing.overlord.autoscaling;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.joda.time.DateTime;
import org.joda.time.Period;

/**
 */
public class ProvisioningSchedulerConfig
{
  @JsonProperty
  private boolean doAutoscale = false;

  @JsonProperty
  private Period provisionPeriod = new Period("PT1M");

  @JsonProperty
  private Period terminatePeriod = new Period("PT5M");

  @JsonProperty
  private DateTime originTime = new DateTime("2012-01-01T00:55:00.000Z");

  public boolean isDoAutoscale()
  {
    return doAutoscale;
  }

  public Period getProvisionPeriod()
  {
    return provisionPeriod;
  }

  public Period getTerminatePeriod()
  {
    return terminatePeriod;
  }

  public DateTime getOriginTime()
  {
    return originTime;
  }
}
