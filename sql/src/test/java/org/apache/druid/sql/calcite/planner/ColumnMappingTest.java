package org.apache.druid.sql.calcite.planner;

import nl.jqno.equalsverifier.EqualsVerifier;
import org.junit.Test;

public class ColumnMappingTest
{
  @Test
  public void testEquals()
  {
    EqualsVerifier.simple().forClass(ColumnMapping.class)
                  .usingGetClass()
                  .verify();
  }
}
