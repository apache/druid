package org.apache.druid.msq.indexing.destination;

import com.fasterxml.jackson.databind.ObjectMapper;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.sql.http.ResultFormat;
import org.junit.Assert;
import org.junit.Test;

public class DurableStorageMSQDestinationTest
{
  @Test
  public void testEquals()
  {
    EqualsVerifier.forClass(DurableStorageMSQDestination.class)
                  .withNonnullFields("resultFormat")
                  .usingGetClass()
                  .verify();
  }

  @Test
  public void testSerde() throws Exception
  {
    final ObjectMapper mapper = TestHelper.makeJsonMapper();

    DurableStorageMSQDestination msqDestination = new DurableStorageMSQDestination(ResultFormat.OBJECTLINES);

    final DurableStorageMSQDestination msqDestination2 = mapper.readValue(
        mapper.writeValueAsString(msqDestination),
        DurableStorageMSQDestination.class
    );

    Assert.assertEquals(msqDestination, msqDestination2);
  }

  @Test
  public void testDefaultResultFormat() throws Exception
  {
    final ObjectMapper mapper = TestHelper.makeJsonMapper();
    String destinationString = "{\"type\":\"durableStorage\"}";


    final DurableStorageMSQDestination msqDestination = mapper.readValue(
        destinationString,
        DurableStorageMSQDestination.class
    );

    Assert.assertEquals(ResultFormat.DEFAULT_RESULT_FORMAT, msqDestination.getResultFormat());
  }

}