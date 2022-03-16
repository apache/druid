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

package org.apache.druid.utils;

import org.apache.druid.java.util.common.ISE;
import org.junit.Assert;
import org.junit.Test;

public class ThrowablesTest
{
  @Test
  public void testGetCauseOfType_Itself()
  {
    Throwable th = new NoClassDefFoundError();
    Assert.assertSame(th, Throwables.getCauseOfType(th, NoClassDefFoundError.class));
  }

  @Test
  public void testGetCauseOfType_NestedThrowable()
  {
    Throwable th = new NoClassDefFoundError();
    Assert.assertSame(th,
        Throwables.getCauseOfType(new RuntimeException(th), NoClassDefFoundError.class)
    );
  }

  @Test
  public void testGetCauseOfType_NestedThrowableSubclass()
  {
    Throwable th = new ISE("something");
    Assert.assertSame(th,
        Throwables.getCauseOfType(new RuntimeException(th), IllegalStateException.class)
    );
  }

  @Test
  public void testGetCauseOfType_NonTarget()
  {
    Assert.assertNull(
        Throwables.getCauseOfType(new RuntimeException(new ClassNotFoundException()), NoClassDefFoundError.class)
    );
  }
}
