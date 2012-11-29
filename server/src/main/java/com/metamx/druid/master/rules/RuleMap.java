package com.metamx.druid.master.rules;

import com.google.common.collect.Lists;

import java.util.List;
import java.util.Map;

/**
 */
public class RuleMap
{
  private final Map<String, List<Rule>> rules;
  private final List<Rule> defaultRules;

  public RuleMap(Map<String, List<Rule>> rules, List<Rule> defaultRules)
  {
    this.rules = rules;
    this.defaultRules = (defaultRules == null) ? Lists.<Rule>newArrayList() : defaultRules;
  }

  public List<Rule> getRules(String dataSource)
  {
    List<Rule> retVal = Lists.newArrayList();
    retVal.addAll((rules.get(dataSource) == null) ? Lists.<Rule>newArrayList() : rules.get(dataSource));
    retVal.addAll(defaultRules);
    return retVal;
  }
}
