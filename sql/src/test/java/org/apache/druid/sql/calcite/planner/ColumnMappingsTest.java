package org.apache.druid.sql.calcite.planner;

import nl.jqno.equalsverifier.EqualsVerifier;
import org.junit.Test;

public class ColumnMappingsTest
{
  @Test
  public void testEquals()
  {
    EqualsVerifier.simple().forClass(ColumnMappings.class)
                  .usingGetClass()
                  .withIgnoredFields("outputColumnNameToPositionMap", "queryColumnNameToPositionMap")
                  .verify();
  }
}
