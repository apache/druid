package org.apache.druid.msq.querykit;

import nl.jqno.equalsverifier.EqualsVerifier;
import org.junit.Test;

public class WindowOperatorQueryFrameProcessorFactoryTest
{
  @Test
  public void testEqualsAndHashcode()
  {
    EqualsVerifier.forClass(WindowOperatorQueryFrameProcessorFactory.class)
                  .withNonnullFields("query", "operatorList", "stageRowSignature", "isEmptyOver", "maxRowsMaterializedInWindow", "partitionColumnNames")
                  .usingGetClass()
                  .verify();
  }
}