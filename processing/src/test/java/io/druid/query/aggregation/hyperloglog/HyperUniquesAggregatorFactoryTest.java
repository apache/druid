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

package io.druid.query.aggregation.hyperloglog;

import org.junit.Assert;
import org.junit.Test;

public class HyperUniquesAggregatorFactoryTest
{
  final static HyperUniquesAggregatorFactory aggregatorFactory = new HyperUniquesAggregatorFactory(
      "hyperUnique",
      "uniques"
  );
  final static String V0_BASE64 = "AAYbEyQwFyQVASMCVFEQQgEQIxIhM4ISAQMhUkICEDFDIBMhMgFQFAFAMjAAEhEREyVAEiUBAhIjISATMCECMiERIRIiVRFRAyIAEgFCQSMEJAITATAAEAMQgCEBEjQiAyUTAyEQASJyAGURAAISAwISATETQhAREBYDIVIlFTASAzJgERIgRCcmUyAwNAMyEJMjIhQXQhEWECABQDETATEREjIRAgEyIiMxMBQiAkBBMDYAMEQQACMzMhIkMTQSkYIRABIBADMBAhIEISAENkEBQDAxETMAIEEwEzQiQSEVQSFBBAQDICIiAVIAMTAQIQYBIRABADMDEzEAQSMkEiAYFBAQI0AmECEyQSARRTIVMhEkMiKAMCUBxUghAkIBI3EmMAQiACEAJDJCAAADOzESEDBCRjMgEUQQETQwEWIhA6MlAiAAZDI1AgEIIDUyFDIHMQEEAwIRBRABBStCZCQhAgJSMQIiQEEURTBmM1MxACIAETGhMgQnBRICNiIREyIUNAEAAkABAwQSEBJBIhIhIRERAiIRACUhEUAVMkQGEVMjECYjACBwEQQSIRIgAAEyExQUFSEAIBJCIDIDYTAgMiNBIUADUiETADMoFEADETMCIwUEQkIAESMSIzIABDERIXEhIiACQgUSEgJiQCAUARIRAREDQiEUAkQgAgQiIEAzIxRCARIgBAAVAzMAECEwE0Qh8gAAASEhEiAiMhUxcRImIVABATYyUBAwIoE1QhRDIiYBIBEBEiQSQyERAAADMAARAEACFYUwQSQBIRIgURITARFSEzEHEBACOTMREBIAMjIgEhU0cxEQIRIhIi1wEgMRUBEgMQIRAnAVASURMHQBAiEyBSAAEBQTAWQ5EQA0IUMSISAUEiASIjIhMhMFJBBSEjEAECEwACASEQFBAjARITEQIgYTEKEAeAAiMkEyARowARFBAicRISIBIxAQAgEBARMCIRQgMSIVIAkjMxIAIEMyADASMgFRIjEyKjEjBBIEQCUAARYBEQMxMCIBACNCACRCMlEzUUAAUDM1MhAjEgAxAAISAVFQECAhQAMBMhEzEgASNxAhFRIxECMRJBQAERAToBgQMhJSRQFAEhAwMiIhMQAwAgQiBQJiIGMQQhEiQxR1MiAjIAIEEiAkARECEzQlMjECIRATBgIhEBQAIQAEATEjBCMwAgMBMhAhIyFBIxQAARI1AAEABCIDFBIRUzMBIgAgEiARQCASMQQDQCFBAQAUJwMUElAyIAIRBSIRITICEAIxMAEUBEYTcBMBEEIxMREwIRIDAGIAEgYxBAEANCAhBAI2UhIiIgIRABIEVRAwNEIQERQgEFMhFCQSIAEhQDMTEQMiAjJyEQ==";

  @Test
  public void testDeserializeV0() throws Exception
  {
    Object v0 = aggregatorFactory.deserialize(V0_BASE64);
    Assert.assertEquals("deserialized value is HLLCV0", HLLCV0.class, v0.getClass());
  }

  @Test
  public void testCombineStartValueV0() throws Exception
  {
    Object combined = aggregatorFactory.getAggregatorStartValue();
    aggregatorFactory.combine(combined, aggregatorFactory.deserialize(V0_BASE64));
  }


}
