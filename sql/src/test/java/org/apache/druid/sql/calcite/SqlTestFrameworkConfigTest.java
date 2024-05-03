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

package org.apache.druid.sql.calcite;

import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.sql.calcite.SqlTestFrameworkConfig.MinTopNThreshold;
import org.apache.druid.sql.calcite.SqlTestFrameworkConfig.NumMergeBuffers;
import org.apache.druid.sql.calcite.SqlTestFrameworkConfig.SqlTestFrameworkConfigInstance;
import org.junit.jupiter.api.Test;
import org.junit.runner.JUnitCore;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;

import java.lang.annotation.Annotation;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class SqlTestFrameworkConfigTest
{
  public static void main(String[] args) {
    Result result = JUnitCore.runClasses(SqlTestFrameworkConfigTest.class);

    for (Failure failure : result.getFailures()) {
       System.out.println(failure.toString());
    }

    System.out.println(result.wasSuccessful());
 }
  @Test
  public void testEquals()
  {
    EqualsVerifier.forClass(SqlTestFrameworkConfig.SqlTestFrameworkConfigInstance.class)
        .usingGetClass()
        .verify();
  }


  @SqlTestFrameworkConfig(minTopNThreshold = 111)
  @NumMergeBuffers(33)
  static class C {
  }

  @SqlTestFrameworkConfig(numMergeBuffers =  222)
  @MinTopNThreshold(33)
  static class D {
  }

  @Test
  public void testA()
  {

    Annotation[] ann=new Annotation[] {
        C.class.getAnnotation(SqlTestFrameworkConfig.class),
        C.class.getAnnotation(SqlTestFrameworkConfig.NumMergeBuffers.class),
        D.class.getAnnotation(SqlTestFrameworkConfig.class)
    }
    ;
    SqlTestFrameworkConfigInstance s = new SqlTestFrameworkConfig.SqlTestFrameworkConfigInstance(ann);

    assertEquals(33, s.numMergeBuffers);
//    assertEquals(111, s.minTopNThreshold);
    System.out.println(s.numMergeBuffers);
  }

}
