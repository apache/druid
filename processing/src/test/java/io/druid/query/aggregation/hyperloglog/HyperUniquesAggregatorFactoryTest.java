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
import com.google.common.collect.ImmutableList;
import org.apache.commons.codec.binary.Base64;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;

@RunWith(Parameterized.class)
public class HyperUniquesAggregatorFactoryTest
{
  @Parameterized.Parameters
  public static Collection<Object[]> getParameters()
  {
    return ImmutableList.<Object[]>of(
        Arrays.asList(
            "AAYbEyQwFyQVASMCVFEQQgEQIxIhM4ISAQMhUkICEDFDIBMhMgFQFAFAMjAAEhEREyVAEiUBAhIjISATMCECMiERIRIiVRFRAyIAEgFCQSMEJAITATAAEAMQgCEBEjQiAyUTAyEQASJyAGURAAISAwISATETQhAREBYDIVIlFTASAzJgERIgRCcmUyAwNAMyEJMjIhQXQhEWECABQDETATEREjIRAgEyIiMxMBQiAkBBMDYAMEQQACMzMhIkMTQSkYIRABIBADMBAhIEISAENkEBQDAxETMAIEEwEzQiQSEVQSFBBAQDICIiAVIAMTAQIQYBIRABADMDEzEAQSMkEiAYFBAQI0AmECEyQSARRTIVMhEkMiKAMCUBxUghAkIBI3EmMAQiACEAJDJCAAADOzESEDBCRjMgEUQQETQwEWIhA6MlAiAAZDI1AgEIIDUyFDIHMQEEAwIRBRABBStCZCQhAgJSMQIiQEEURTBmM1MxACIAETGhMgQnBRICNiIREyIUNAEAAkABAwQSEBJBIhIhIRERAiIRACUhEUAVMkQGEVMjECYjACBwEQQSIRIgAAEyExQUFSEAIBJCIDIDYTAgMiNBIUADUiETADMoFEADETMCIwUEQkIAESMSIzIABDERIXEhIiACQgUSEgJiQCAUARIRAREDQiEUAkQgAgQiIEAzIxRCARIgBAAVAzMAECEwE0Qh8gAAASEhEiAiMhUxcRImIVABATYyUBAwIoE1QhRDIiYBIBEBEiQSQyERAAADMAARAEACFYUwQSQBIRIgURITARFSEzEHEBACOTMREBIAMjIgEhU0cxEQIRIhIi1wEgMRUBEgMQIRAnAVASURMHQBAiEyBSAAEBQTAWQ5EQA0IUMSISAUEiASIjIhMhMFJBBSEjEAECEwACASEQFBAjARITEQIgYTEKEAeAAiMkEyARowARFBAicRISIBIxAQAgEBARMCIRQgMSIVIAkjMxIAIEMyADASMgFRIjEyKjEjBBIEQCUAARYBEQMxMCIBACNCACRCMlEzUUAAUDM1MhAjEgAxAAISAVFQECAhQAMBMhEzEgASNxAhFRIxECMRJBQAERAToBgQMhJSRQFAEhAwMiIhMQAwAgQiBQJiIGMQQhEiQxR1MiAjIAIEEiAkARECEzQlMjECIRATBgIhEBQAIQAEATEjBCMwAgMBMhAhIyFBIxQAARI1AAEABCIDFBIRUzMBIgAgEiARQCASMQQDQCFBAQAUJwMUElAyIAIRBSIRITICEAIxMAEUBEYTcBMBEEIxMREwIRIDAGIAEgYxBAEANCAhBAI2UhIiIgIRABIEVRAwNEIQERQgEFMhFCQSIAEhQDMTEQMiAjJyEQ==",
            2950.0827550144822,
            HLLCV0.class
        ).toArray(),
        Arrays.asList(
            "AQcH/xYEMXOjRTVSQ1NXVENEM1RTUlVTRDI1aEVnhkOjNUaCI2MkU2VVhVNkNyVTa4NEYkS0kjZYU1RDdEYzUjglNTUzVFM0NkU3ZFUjOVJCdlU0N2QjRDRUV1MyZjNmVDOUM2RVVFRzhnUzVXY1R1RHUnNziURUdmREM0VjVEQmU0aEInZYNzNZNVRFgzVFNolSJHNIQ3QklEZlNSNoNTJXpDk1dFWjJGNYNiQzQkZFNEYzc1NVhSczM2NmJDZlc3JJRCpVNiRlNEI3dmU1ZGI0Q1RCMhNFZEJDZDYyNFOCM3U0VmRlVlNIRVQ4VVw1djNDVURHVSaFU0VEY0U1JFNIVCYlVEJWM2NWU0eURDOjQ6YyNTYkZjNUVjR1ZDdnVkMzVHZFpjMzlmNEFHM0dHJlRYTHSEQjVZVVZkVVIzIjg2SUU0NSM0VFNDNCdGVlQkhBNENCVTZGZEVlxFQyQ0NYWkUmVUJUYzRlNqg4NVVTNThEJkRGNDNUNFSEYmgkR0dDR1JldCNhVEZGRENGc1NDRUNER3WJRTRHQ4JlOYZoJDVVVVMzZSREZ1Q1UjSHNkdUMlU0ODIzZThSNmNDNjQ1o2I0YiRGYyZkNUJYVEMyN2QpQyMkc2VTE4U2VCNHZFRDNTh0IzI2VFNTMlUkNGMlKTRCIyR3QiQzFUNkRTdDM6RDRFI3VyVlcyWCUlQ0YjNjU2Q2dEVFNTRyRlI7VElHVTVVNGk0JHJTQzQkQyVlV0NCVlRkhWYkQ0RVaDNYdFZHWEWFJEYpM0QjNjNVUzNCVzVkgzZGFzQkRZUzN2U1dUFGVWZTUzVUREZDciZEVVYVNjeCU0ZDdEhzIpU2RTOFRUQkWlk1OFRUVTN1MkZSM3ZFc1VDNnUmc2NKNUaUIzd3M0RWxEZTsiNENLVHU0NFUmQ2RWRFdCNUVENFkxZCEnRLQkNEU0RVNmVDQjl9ZmNkM1QVM0MzQkUjJlVHRkNEVWlENDVUIlUvRkM0RVY1UzY6OGVHVCRDIzRUUlUjM2RDWSVkVIU1U1ZiVFNlNDhTN1VWNTVEZ2RzNzVDQlY0ZUNENUM5NUdkRDJGYzRCUzIjRGR4UmJFI4GDRTUiQ0ZUhVY1ZEYoZSRoVDYnREYkQ1SUU0RWUycjp2RZIySVZkUmZDREZVJGQyVEc1JElBZENEU2VEQlVUUnNDQziLRTNidmNjVCtjRFU2Q0SGYzVHVpGTNoVDxFVSMlWTJFQyRJdV1EI3RDloYyNFQ0c1NVY0ZHVEY0dkM2QkQyVDVUVTNFUyamMUdSNrNz0mlFlERzZTSGhFRjVGM3NWU2NINDI2U1RERUhjY4FHNWNTVTV1U0U2I0VXNEZERWNDNUSjI1WmMmQ4U=",
            2440618.528853266,
            HLLCV1.class
        ).toArray()
    );
  }

  private final String base64;
  private final Double cardinality;
  private final Class<? extends HyperLogLogCollector> hllClazz;

  public HyperUniquesAggregatorFactoryTest(
      String base64,
      double cardinality,
      Class<? extends HyperLogLogCollector> hllClazz
  )
  {
    this.base64 = base64;
    this.cardinality = cardinality;
    this.hllClazz = hllClazz;
  }

  final static HyperUniquesAggregatorFactory aggregatorFactory = new HyperUniquesAggregatorFactory(
      "hyperUnique",
      "uniques"
  );

  @Test
  public void testDeserializeV0() throws Exception
  {
    Object v0 = aggregatorFactory.deserialize(base64);
    Assert.assertEquals("Deserialized class check", hllClazz, v0.getClass());
  }

  @Test
  public void testCombineStartValueV0() throws Exception
  {
    Object combined = aggregatorFactory.getAggregatorStartValue();
    aggregatorFactory.combine(combined, aggregatorFactory.deserialize(base64));
  }

  @Test
  public void testEstimateCardinalityNull()
  {
    Object o = HyperUniquesAggregatorFactory.estimateCardinality(null);
    Assert.assertEquals(0.0, o);
  }

  @Test
  public void testEstimateCardinalityByteArray()
  {
    Object o = HyperUniquesAggregatorFactory.estimateCardinality(Base64.decodeBase64((base64).getBytes(Charsets.UTF_8)));
    Assert.assertEquals(cardinality, o);
  }

  @Test
  public void testEstimateCardinalityByteBuffer()
  {
    Object o = HyperUniquesAggregatorFactory.estimateCardinality(
        ByteBuffer.wrap(
            Base64.decodeBase64(
                (base64).getBytes(
                    Charsets.UTF_8
                )
            )
        )
    );
    Assert.assertEquals(cardinality, o);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBadEstimateCardinalityObject()
  {
    HyperUniquesAggregatorFactory.estimateCardinality(0);
  }
}
