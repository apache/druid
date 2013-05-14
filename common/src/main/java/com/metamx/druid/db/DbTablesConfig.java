package com.metamx.druid.db;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.metamx.common.ISE;

import javax.validation.constraints.NotNull;

/**
 */
public class DbTablesConfig
{
  public static DbTablesConfig fromBase(String base)
  {
    return new DbTablesConfig(base, null, null);
  }

  @NotNull
  private final String base;

  @NotNull
  private final String segmentsTable;

  @NotNull
  private final String ruleTable;

  @JsonCreator
  public DbTablesConfig(
      @JsonProperty("base") String base,
      @JsonProperty("segments") String segmentsTable,
      @JsonProperty("rules") String rulesTable
  )
  {

    this.base = base;
    this.segmentsTable = makeTableName(segmentsTable, "segments");
    this.ruleTable = makeTableName(rulesTable, "rules");
  }

  private String makeTableName(String explicitTableName, String defaultSuffix)
  {
    if (explicitTableName == null) {
      if (base == null) {
        throw new ISE("table[%s] unknown!  Both base and %s were null!", defaultSuffix, defaultSuffix);
      }
      return String.format("%s_%s", base, defaultSuffix);
    }

    return explicitTableName;
  }

  @JsonProperty
  public String getBase()
  {
    return base;
  }

  @JsonProperty("segments")
  public String getSegmentsTable()
  {
    return segmentsTable;
  }

  @JsonProperty("rules")
  public String getRulesTable()
  {
    return ruleTable;
  }
}