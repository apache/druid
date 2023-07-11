package org.apache.druid.msq.indexing.destination;

import com.fasterxml.jackson.databind.ObjectMapper;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.sql.http.ResultFormat;
import org.junit.Assert;
import org.junit.Test;

public class TaskReportMSQDestinationTest
{
  @Test
  public void testEquals()
  {
    EqualsVerifier.forClass(TaskReportMSQDestination.class)
                  .withNonnullFields("resultFormat")
                  .usingGetClass()
                  .verify();
  }

  @Test
  public void testSerde() throws Exception
  {
    final ObjectMapper mapper = TestHelper.makeJsonMapper();

    TaskReportMSQDestination msqDestination = new TaskReportMSQDestination(ResultFormat.OBJECTLINES);

    final TaskReportMSQDestination msqDestination2 = mapper.readValue(
        mapper.writeValueAsString(msqDestination),
        TaskReportMSQDestination.class
    );

    Assert.assertEquals(msqDestination, msqDestination2);
  }

  @Test
  public void testDefaultResultFormat() throws Exception
  {
    final ObjectMapper mapper = TestHelper.makeJsonMapper();
    String destinationString = "{\"type\":\"taskReport\"}";


    final TaskReportMSQDestination msqDestination = mapper.readValue(
        destinationString,
        TaskReportMSQDestination.class
    );

    Assert.assertEquals(ResultFormat.DEFAULT_RESULT_FORMAT, msqDestination.getResultFormat());
  }
}