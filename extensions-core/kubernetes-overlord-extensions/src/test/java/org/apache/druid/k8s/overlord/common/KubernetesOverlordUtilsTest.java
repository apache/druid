/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.k8s.overlord.common;

import org.junit.Assert;
import org.junit.Test;

public class KubernetesOverlordUtilsTest
{

  @Test
  public void test_shortLabel()
  {
    Assert.assertEquals("data_source", KubernetesOverlordUtils.convertStringToK8sLabel("data_source"));
  }


  @Test
  public void test_stripDisallowedPatterns()
  {
    Assert.assertEquals("data_source-1.wikipedia", KubernetesOverlordUtils.convertStringToK8sLabel(
        "_.-data_source-1.wikipedia.-_"
    ));
  }

  @Test
  public void test_nullLabel()
  {
    Assert.assertEquals("", KubernetesOverlordUtils.convertStringToK8sLabel(null));
  }

  @Test
  public void test_stripTaskId()
  {
    Assert.assertEquals("apiissuedkillwikipedianewbalhnoib10000101t000000000z20230514t00", KubernetesOverlordUtils.convertTaskIdToK8sLabel(
        "api-issued_kill_wikipedia_new_balhnoib_1000-01-01T00:00:00.000Z_2023-05-14T00:00:00.000Z_2023-05-15T17:28:42.526Z"
    ));
  }

  @Test
  public void test_nullTaskId()
  {
    Assert.assertEquals("", KubernetesOverlordUtils.convertTaskIdToK8sLabel(null));
  }

  @Test
  public void test_stripJobName()
  {
    Assert.assertEquals("apiissuedkillwikipedianewbalhn-8916017dfd5469fe9a8881b1035497a2", KubernetesOverlordUtils.convertTaskIdToJobName(
        "api-issued_kill_wikipedia_new_balhnoib_1000-01-01T00:00:00.000Z_2023-05-14T00:00:00.000Z_2023-05-15T17:28:42.526Z"
    ));
  }

  @Test
  public void test_stripJobName_avoidDuplicatesWithLongDataSourceName()
  {
    String jobName1 = KubernetesOverlordUtils.convertTaskIdToJobName("coordinator-issued_compact_1234_telemetry_wikipedia_geteditfailuresinnorthamerica_agg_summ_116_pcgkebcl_2023-07-19T16:53:11.416Z");
    String jobName2 = KubernetesOverlordUtils.convertTaskIdToJobName("coordinator-issued_compact_1234_telemetry_wikipedia_geteditfailuresinnorthamerica_agg_summ_117_pcgkebcl_2023-07-19T16:53:11.416Z");
    Assert.assertNotEquals(jobName1, jobName2);
  }

  @Test
  public void test_longLabelEndsWithAlphanumeric()
  {
    // Test case for bug where truncation at 63 chars could result in a label ending with a special character
    // This string is 80 characters and would be truncated at position 63, landing on a hyphen
    String longLabelHyphen = "very-long-datasource-name-with-many-illegal_label_endings-that-exceeds-the--------limit-12345678";
    String longLabelPeriod = "very-long-datasource-name-with-many-illegal_label_endings-that-exceeds-the........limit-12345678";
    String longLabelUnderscore = "very-long-datasource-name-with-many-illegal_label_endings-that-exceeds-the________limit-12345678";

    for (String longLabel : new String[]{longLabelHyphen, longLabelPeriod, longLabelUnderscore}) {
      String result = KubernetesOverlordUtils.convertStringToK8sLabel(longLabel);

      // Result should not exceed 63 characters
      Assert.assertTrue("Label should not exceed 63 characters", result.length() <= 63);

      // Result should not be empty
      Assert.assertFalse("Label should not be empty", result.isEmpty());

      // Result should not end with a hyphen, underscore, or dot
      Assert.assertFalse("Label should not end with hyphen", result.endsWith("-"));
      Assert.assertFalse("Label should not end with underscore", result.endsWith("_"));
      Assert.assertFalse("Label should not end with dot", result.endsWith("."));

      // Result should end with an alphanumeric character
      char lastChar = result.charAt(result.length() - 1);
      Assert.assertTrue(
          "Label should end with alphanumeric character, but ends with: " + lastChar,
          Character.isLetterOrDigit(lastChar)
      );
    }
  }
}
