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

package org.apache.druid.metadata;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.joda.time.Period;

/**
 */
public class MetadataRuleManagerConfig
{
  /**
   * Default value of {@code druid.manager.rules.defaultRule}, i.e. the datasource against which
   * the cluster-level default rules are stored when an operator has not configured another name.
   */
  public static final String DEFAULT_RULE_NAME = "_default";

  @JsonProperty
  private String defaultRule = DEFAULT_RULE_NAME;

  @JsonProperty
  private Period pollDuration = new Period("PT1M");

  @JsonProperty
  private Period alertThreshold = new Period("PT10M");

  /**
   * Datasource name against which the cluster-level default rules are stored
   * in the metadata store.
   */
  public String getDefaultRule()
  {
    return defaultRule;
  }

  public Period getPollDuration()
  {
    return pollDuration;
  }

  public Period getAlertThreshold()
  {
    return alertThreshold;
  }
}
