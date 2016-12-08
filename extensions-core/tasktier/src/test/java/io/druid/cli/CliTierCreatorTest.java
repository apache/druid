/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Metamarkets licenses this file
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

package io.druid.cli;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import io.airlift.airline.Cli;
import io.airlift.airline.model.CommandGroupMetadata;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Nullable;

public class CliTierCreatorTest
{

  @Test
  public void testAddCommands() throws Exception
  {
    final Cli.CliBuilder builder = new Cli.CliBuilder("builder");
    final CliTierCreator creator = new CliTierCreator();
    creator.addCommands(builder);
    final Cli cli = builder.build();
    Assert.assertEquals(ImmutableList.of("tier"), Lists.transform(
        cli.getMetadata().getCommandGroups(),
        new Function<CommandGroupMetadata, String>()
        {
          @Nullable
          @Override
          public String apply(@Nullable CommandGroupMetadata input)
          {
            return input.getName();
          }
        }
    ));
  }
}

