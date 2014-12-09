/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.query.aggregation.hyperloglog;

import com.google.common.base.Charsets;
import org.apache.commons.codec.binary.Base64;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;

public class HyperUniquesAggregatorFactoryTest
{
  final static HyperUniquesAggregatorFactory aggregatorFactory = new HyperUniquesAggregatorFactory(
      "hyperUnique",
      "uniques"
  );
  final static String V0_BASE64 = "AAYbEyQwFyQVASMCVFEQQgEQIxIhM4ISAQMhUkICEDFDIBMhMgFQFAFAMjAAEhEREyVAEiUBAhIjISATMCECMiERIRIiVRFRAyIAEgFCQSMEJAITATAAEAMQgCEBEjQiAyUTAyEQASJyAGURAAISAwISATETQhAREBYDIVIlFTASAzJgERIgRCcmUyAwNAMyEJMjIhQXQhEWECABQDETATEREjIRAgEyIiMxMBQiAkBBMDYAMEQQACMzMhIkMTQSkYIRABIBADMBAhIEISAENkEBQDAxETMAIEEwEzQiQSEVQSFBBAQDICIiAVIAMTAQIQYBIRABADMDEzEAQSMkEiAYFBAQI0AmECEyQSARRTIVMhEkMiKAMCUBxUghAkIBI3EmMAQiACEAJDJCAAADOzESEDBCRjMgEUQQETQwEWIhA6MlAiAAZDI1AgEIIDUyFDIHMQEEAwIRBRABBStCZCQhAgJSMQIiQEEURTBmM1MxACIAETGhMgQnBRICNiIREyIUNAEAAkABAwQSEBJBIhIhIRERAiIRACUhEUAVMkQGEVMjECYjACBwEQQSIRIgAAEyExQUFSEAIBJCIDIDYTAgMiNBIUADUiETADMoFEADETMCIwUEQkIAESMSIzIABDERIXEhIiACQgUSEgJiQCAUARIRAREDQiEUAkQgAgQiIEAzIxRCARIgBAAVAzMAECEwE0Qh8gAAASEhEiAiMhUxcRImIVABATYyUBAwIoE1QhRDIiYBIBEBEiQSQyERAAADMAARAEACFYUwQSQBIRIgURITARFSEzEHEBACOTMREBIAMjIgEhU0cxEQIRIhIi1wEgMRUBEgMQIRAnAVASURMHQBAiEyBSAAEBQTAWQ5EQA0IUMSISAUEiASIjIhMhMFJBBSEjEAECEwACASEQFBAjARITEQIgYTEKEAeAAiMkEyARowARFBAicRISIBIxAQAgEBARMCIRQgMSIVIAkjMxIAIEMyADASMgFRIjEyKjEjBBIEQCUAARYBEQMxMCIBACNCACRCMlEzUUAAUDM1MhAjEgAxAAISAVFQECAhQAMBMhEzEgASNxAhFRIxECMRJBQAERAToBgQMhJSRQFAEhAwMiIhMQAwAgQiBQJiIGMQQhEiQxR1MiAjIAIEEiAkARECEzQlMjECIRATBgIhEBQAIQAEATEjBCMwAgMBMhAhIyFBIxQAARI1AAEABCIDFBIRUzMBIgAgEiARQCASMQQDQCFBAQAUJwMUElAyIAIRBSIRITICEAIxMAEUBEYTcBMBEEIxMREwIRIDAGIAEgYxBAEANCAhBAI2UhIiIgIRABIEVRAwNEIQERQgEFMhFCQSIAEhQDMTEQMiAjJyEQ==";
  final static double cardinality = 2950.0827550144822;
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

  @Test
  public void testEstimateCardinalityNull(){
    Object o = aggregatorFactory.estimateCardinality(null);
    Assert.assertEquals(0.0, o);
  }
  @Test
  public void testEstimateCardinalityByteArray(){
    Object o = aggregatorFactory.estimateCardinality(Base64.decodeBase64((V0_BASE64).getBytes(Charsets.UTF_8)));
    Assert.assertEquals(cardinality, o);
  }
  @Test
  public void testEstimateCardinalityByteBuffer(){
    Object o = aggregatorFactory.estimateCardinality(ByteBuffer.wrap(Base64.decodeBase64((V0_BASE64).getBytes(Charsets.UTF_8))));
    Assert.assertEquals(cardinality, o);
  }
  @Test(expected = IllegalArgumentException.class)
  public void testBadEstimateCardinalityObject(){
    aggregatorFactory.estimateCardinality(0);
  }
}
