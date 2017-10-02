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

package io.druid.output;

import com.google.common.io.Files;
import com.google.common.primitives.Ints;
import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;

@RunWith(Parameterized.class)
public class OutputBytesTest
{
  @Parameterized.Parameters
  public static Collection<Object[]> constructorFeeder() throws IOException
  {
    return Arrays.asList(
        new Object[] {new TmpFileOutputMedium(Files.createTempDir())},
        new Object[] {new OffHeapMemoryOutputMedium()},
        new Object[] {new OnHeapMemoryOutputMedium()}
    );
  }

  private final OutputMedium outputMedium;

  public OutputBytesTest(OutputMedium outputMedium)
  {
    this.outputMedium = outputMedium;
  }

  @Test
  public void testOutputBytes() throws IOException
  {
    OutputBytes outputBytes = outputMedium.makeOutputBytes();

    outputBytes.write('1');
    verifyContents(outputBytes, "1");

    outputBytes.writeInt(Ints.fromBytes((byte) '2', (byte) '3', (byte) '4', (byte) '5'));
    verifyContents(outputBytes, "12345");

    outputBytes.write(new byte[] {'a'});
    verifyContents(outputBytes, "12345a");

    outputBytes.write(new byte[] {'a', 'b', 'c'}, 1, 1);
    verifyContents(outputBytes, "12345ab");

    ByteBuffer bb = ByteBuffer.wrap(new byte[]{'a', 'b', 'c'});
    bb.position(2);
    outputBytes.write(bb);
    Assert.assertEquals(3, bb.position());
    verifyContents(outputBytes, "12345abc");
  }

  private void verifyContents(OutputBytes outputBytes, String expected) throws IOException
  {
    Assert.assertEquals(expected, IOUtils.toString(outputBytes.asInputStream(), StandardCharsets.US_ASCII));
  }
}
