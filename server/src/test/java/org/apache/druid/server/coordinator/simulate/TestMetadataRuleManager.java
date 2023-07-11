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

package org.apache.druid.server.coordinator.simulate;

import org.apache.druid.audit.AuditInfo;
import org.apache.druid.metadata.MetadataRuleManager;
import org.apache.druid.server.coordinator.rules.ForeverLoadRule;
import org.apache.druid.server.coordinator.rules.Rule;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestMetadataRuleManager implements MetadataRuleManager
{
  private final Map<String, List<Rule>> rules = new HashMap<>();

  private static final String DEFAULT_DATASOURCE = "_default";

  public TestMetadataRuleManager()
  {
    rules.put(
        DEFAULT_DATASOURCE,
        Collections.singletonList(new ForeverLoadRule(null))
    );
  }

  @Override
  public void start()
  {
    // do nothing
  }

  @Override
  public void stop()
  {
    // do nothing
  }

  @Override
  public void poll()
  {
    // do nothing
  }

  @Override
  public Map<String, List<Rule>> getAllRules()
  {
    return rules;
  }

  @Override
  public List<Rule> getRules(final String dataSource)
  {
    List<Rule> retVal = rules.get(dataSource);
    return retVal == null ? new ArrayList<>() : retVal;
  }

  @Override
  public List<Rule> getRulesWithDefault(final String dataSource)
  {
    List<Rule> retVal = new ArrayList<>();
    final Map<String, List<Rule>> theRules = rules;
    if (theRules.get(dataSource) != null) {
      retVal.addAll(theRules.get(dataSource));
    }
    if (theRules.get(DEFAULT_DATASOURCE) != null) {
      retVal.addAll(theRules.get(DEFAULT_DATASOURCE));
    }
    return retVal;
  }

  @Override
  public boolean overrideRule(final String dataSource, final List<Rule> newRules, final AuditInfo auditInfo)
  {
    rules.put(dataSource, newRules);
    return true;
  }

  @Override
  public int removeRulesForEmptyDatasourcesOlderThan(long timestamp)
  {
    return 0;
  }

  public void removeRulesForDatasource(String dataSource)
  {
    if (!DEFAULT_DATASOURCE.equals(dataSource)) {
      rules.remove(dataSource);
    }
  }
}
