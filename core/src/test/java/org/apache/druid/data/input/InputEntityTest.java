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


package org.apache.druid.data.input;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

import static org.junit.Assert.assertThrows;


public class InputEntityTest
{
  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void testInputEntity() 
  {
    InputEntity inputEntity = new InputEntity()
    {
      @Nullable
      @Override
      public URI getUri()
      {
        return null;
      }

      @Override
      public InputStream open() throws IOException
      {
        throw new IOException();
      }
    };

    Assert.assertNull(inputEntity.getUri());
    assertThrows(IOException.class, () -> {
      inputEntity.open();
    });
    Assert.assertNotNull(inputEntity.getRetryCondition());
    assertThrows(IOException.class, () -> {
      inputEntity.fetch(temporaryFolder.newFolder(), null);
    });
    Assert.assertFalse(inputEntity.isFromTombstone());
  }
}

