/*
 * (c) 2019 Alessandro Colantonio
 * <mailto:colanton@mat.uniroma3.it>
 * <http://ricerca.mat.uniroma3.it/users/colanton>
 *
 * Modified at the Apache Software Foundation (ASF).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.druid.extendedset.intset;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.Timeout;

public class ConciseSetUtilsTest
{
  @Rule public final ExpectedException thrown = ExpectedException.none();

  @Rule public final Timeout globalTimeout = new Timeout(10000);

  /* testedClasses: ConciseSetUtils */
  // Test written by Diffblue Cover.
  @Test
  public void getLiteralBitCountInputPositiveOutputPositive()
  {
    // Arrange
    final int word = 4;

    // Act
    final int actual = ConciseSetUtils.getLiteralBitCount(word);

    // Assert result
    Assert.assertEquals(1, actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void getLiteralFromOneSeqFlipBitInputNegativeOutputNegative()
  {
    // Arrange
    final int word = -99;

    // Act
    final int actual = ConciseSetUtils.getLiteralFromOneSeqFlipBit(word);

    // Assert result
    Assert.assertEquals(-1_073_741_825, actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void getLiteralFromOneSeqFlipBitInputPositiveOutputNegative()
  {
    // Arrange
    final int word = 2;

    // Act
    final int actual = ConciseSetUtils.getLiteralFromOneSeqFlipBit(word);

    // Assert result
    Assert.assertEquals(-1, actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void getLiteralFromZeroSeqFlipBitInputNegativeOutputNegative()
  {
    // Arrange
    final int word = -99;

    // Act
    final int actual = ConciseSetUtils.getLiteralFromZeroSeqFlipBit(word);

    // Assert result
    Assert.assertEquals(-1_073_741_824, actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void getLiteralFromZeroSeqFlipBitInputPositiveOutputNegative()
  {
    // Arrange
    final int word = 2;

    // Act
    final int actual = ConciseSetUtils.getLiteralFromZeroSeqFlipBit(word);

    // Assert result
    Assert.assertEquals(-2_147_483_648, actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void getLiteralInputNegativeFalseOutputNegative()
  {
    // Arrange
    final int word = -99;
    final boolean simulateWAH = false;

    // Act
    final int actual = ConciseSetUtils.getLiteral(word, simulateWAH);

    // Assert result
    Assert.assertEquals(-99, actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void getLiteralInputPositiveFalseOutputNegative()
  {
    // Arrange
    final int word = 2;
    final boolean simulateWAH = false;

    // Act
    final int actual = ConciseSetUtils.getLiteral(word, simulateWAH);

    // Assert result
    Assert.assertEquals(-2_147_483_648, actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void getLiteralInputPositiveFalseOutputNegative2()
  {
    // Arrange
    final int word = 1_073_741_824;
    final boolean simulateWAH = false;

    // Act
    final int actual = ConciseSetUtils.getLiteral(word, simulateWAH);

    // Assert result
    Assert.assertEquals(-1, actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void getLiteralInputPositiveTrueOutputNegative()
  {
    // Arrange
    final int word = 2;
    final boolean simulateWAH = true;

    // Act
    final int actual = ConciseSetUtils.getLiteral(word, simulateWAH);

    // Assert result
    Assert.assertEquals(-2_147_483_648, actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void getLiteralInputPositiveTrueOutputNegative2()
  {
    // Arrange
    final int word = 1_073_741_824;
    final boolean simulateWAH = true;

    // Act
    final int actual = ConciseSetUtils.getLiteral(word, simulateWAH);

    // Assert result
    Assert.assertEquals(-1, actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void getSequenceNumWordsInputPositiveOutputPositive()
  {
    // Arrange
    final int word = 2;

    // Act
    final int actual = ConciseSetUtils.getSequenceNumWords(word);

    // Assert result
    Assert.assertEquals(3, actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void isAllOnesLiteralInputNegativeOutputTrue()
  {
    // Arrange
    final int word = -1;

    // Act
    final boolean actual = ConciseSetUtils.isAllOnesLiteral(word);

    // Assert result
    Assert.assertTrue(actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void isAllOnesLiteralInputPositiveOutputFalse()
  {
    // Arrange
    final int word = 2;

    // Act
    final boolean actual = ConciseSetUtils.isAllOnesLiteral(word);

    // Assert result
    Assert.assertFalse(actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void isAllZerosLiteralInputPositiveOutputFalse()
  {
    // Arrange
    final int word = 2;

    // Act
    final boolean actual = ConciseSetUtils.isAllZerosLiteral(word);

    // Assert result
    Assert.assertFalse(actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void isAllZerosLiteralInputZeroOutputTrue()
  {
    // Arrange
    final int word = 0;

    // Act
    final boolean actual = ConciseSetUtils.isAllZerosLiteral(word);

    // Assert result
    Assert.assertTrue(actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void isLiteralInputNegativeOutputTrue()
  {
    // Arrange
    final int word = -99;

    // Act
    final boolean actual = ConciseSetUtils.isLiteral(word);

    // Assert result
    Assert.assertTrue(actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void isLiteralInputPositiveOutputFalse()
  {
    // Arrange
    final int word = 2;

    // Act
    final boolean actual = ConciseSetUtils.isLiteral(word);

    // Assert result
    Assert.assertFalse(actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void isLiteralWithSingleOneBitInputNegativeOutputFalse()
  {
    // Arrange
    final int word = -99;

    // Act
    final boolean actual = ConciseSetUtils.isLiteralWithSingleOneBit(word);

    // Assert result
    Assert.assertFalse(actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void isLiteralWithSingleOneBitInputNegativeOutputTrue()
  {
    // Arrange
    final int word = -1_073_741_824;

    // Act
    final boolean actual = ConciseSetUtils.isLiteralWithSingleOneBit(word);

    // Assert result
    Assert.assertTrue(actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void isLiteralWithSingleOneBitInputPositiveOutputFalse()
  {
    // Arrange
    final int word = 2;

    // Act
    final boolean actual = ConciseSetUtils.isLiteralWithSingleOneBit(word);

    // Assert result
    Assert.assertFalse(actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void isLiteralWithSingleZeroBitInputNegativeOutputFalse()
  {
    // Arrange
    final int word = -99;

    // Act
    final boolean actual = ConciseSetUtils.isLiteralWithSingleZeroBit(word);

    // Assert result
    Assert.assertFalse(actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void isLiteralWithSingleZeroBitInputNegativeOutputTrue()
  {
    // Arrange
    final int word = -65;

    // Act
    final boolean actual = ConciseSetUtils.isLiteralWithSingleZeroBit(word);

    // Assert result
    Assert.assertTrue(actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void isLiteralWithSingleZeroBitInputPositiveOutputFalse()
  {
    // Arrange
    final int word = 2;

    // Act
    final boolean actual = ConciseSetUtils.isLiteralWithSingleZeroBit(word);

    // Assert result
    Assert.assertFalse(actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void isOneSequenceInputPositiveOutputFalse()
  {
    // Arrange
    final int word = 2;

    // Act
    final boolean actual = ConciseSetUtils.isOneSequence(word);

    // Assert result
    Assert.assertFalse(actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void isOneSequenceInputPositiveOutputTrue()
  {
    // Arrange
    final int word = 1_073_741_824;

    // Act
    final boolean actual = ConciseSetUtils.isOneSequence(word);

    // Assert result
    Assert.assertTrue(actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void isSequenceWithNoBitsInputPositiveOutputTrue()
  {
    // Arrange
    final int word = 2;

    // Act
    final boolean actual = ConciseSetUtils.isSequenceWithNoBits(word);

    // Assert result
    Assert.assertTrue(actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void isZeroSequenceInputPositiveOutputTrue()
  {
    // Arrange
    final int word = 2;

    // Act
    final boolean actual = ConciseSetUtils.isZeroSequence(word);

    // Assert result
    Assert.assertTrue(actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void maxLiteralLengthModulusInputNegativeOutputPositive()
  {
    // Arrange
    final int n = -33;

    // Act
    final int actual = ConciseSetUtils.maxLiteralLengthModulus(n);

    // Assert result
    Assert.assertEquals(2, actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void maxLiteralLengthModulusInputPositiveOutputPositive()
  {
    // Arrange
    final int n = 65_567;

    // Act
    final int actual = ConciseSetUtils.maxLiteralLengthModulus(n);

    // Assert result
    Assert.assertEquals(2, actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void maxLiteralLengthModulusInputPositiveOutputPositive2()
  {
    // Arrange
    final int n = 65_595;

    // Act
    final int actual = ConciseSetUtils.maxLiteralLengthModulus(n);

    // Assert result
    Assert.assertEquals(30, actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void maxLiteralLengthModulusInputPositiveOutputPositive3()
  {
    // Arrange
    final int n = 97_215;

    // Act
    final int actual = ConciseSetUtils.maxLiteralLengthModulus(n);

    // Assert result
    Assert.assertEquals(30, actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void maxLiteralLengthModulusInputPositiveOutputZero()
  {
    // Arrange
    final int n = 31;

    // Act
    final int actual = ConciseSetUtils.maxLiteralLengthModulus(n);

    // Assert result
    Assert.assertEquals(0, actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void maxLiteralLengthModulusInputPositiveOutputZero2()
  {
    // Arrange
    final int n = 1023;

    // Act
    final int actual = ConciseSetUtils.maxLiteralLengthModulus(n);

    // Assert result
    Assert.assertEquals(0, actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void maxLiteralLengthModulusInputPositiveOutputZero3()
  {
    // Arrange
    final int n = 508_927;

    // Act
    final int actual = ConciseSetUtils.maxLiteralLengthModulus(n);

    // Assert result
    Assert.assertEquals(0, actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void maxLiteralLengthModulusInputPositiveOutputZero4()
  {
    // Arrange
    final int n = 781_231;

    // Act
    final int actual = ConciseSetUtils.maxLiteralLengthModulus(n);

    // Assert result
    Assert.assertEquals(0, actual);
  }
}
