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

package org.apache.druid.math.expr.vector;

import com.google.common.math.LongMath;
import com.google.common.primitives.Ints;
import org.apache.druid.math.expr.ExpressionValidationException;
import org.apache.druid.math.expr.Function;

public class VectorMathProcessors
{
  public static Add plus()
  {
    return Add.INSTANCE;
  }

  public static Subtract subtract()
  {
    return Subtract.INSTANCE;
  }

  public static Multiply multiply()
  {
    return Multiply.INSTANCE;
  }

  public static Divide divide()
  {
    return Divide.INSTANCE;
  }

  public static LongDivide longDivide()
  {
    return LongDivide.INSTANCE;
  }

  public static Modulo modulo()
  {
    return Modulo.INSTANCE;
  }

  public static Negate negate()
  {
    return Negate.INSTANCE;
  }

  public static Power power()
  {
    return Power.INSTANCE;
  }

  public static DoublePower doublePower()
  {
    return DoublePower.INSTANCE;
  }

  public static Max max()
  {
    return Max.INSTANCE;
  }

  public static Min min()
  {
    return Min.INSTANCE;
  }

  public static Atan2 atan2()
  {
    return Atan2.INSTANCE;
  }

  public static CopySign copySign()
  {
    return CopySign.INSTANCE;
  }

  public static Hypot hypot()
  {
    return Hypot.INSTANCE;
  }

  public static Remainder remainder()
  {
    return Remainder.INSTANCE;
  }

  public static NextAfter nextAfter()
  {
    return NextAfter.INSTANCE;
  }

  public static Scalb scalb()
  {
    return Scalb.INSTANCE;
  }

  public static Abs abs()
  {
    return Abs.INSTANCE;
  }

  public static Acos acos()
  {
    return Acos.INSTANCE;
  }

  public static Asin asin()
  {
    return Asin.INSTANCE;
  }

  public static Atan atan()
  {
    return Atan.INSTANCE;
  }

  public static Cos cos()
  {
    return Cos.INSTANCE;
  }

  public static Cosh cosh()
  {
    return Cosh.INSTANCE;
  }

  public static Cot cot()
  {
    return Cot.INSTANCE;
  }

  public static Sin sin()
  {
    return Sin.INSTANCE;
  }

  public static Sinh sinh()
  {
    return Sinh.INSTANCE;
  }

  public static Tan tan()
  {
    return Tan.INSTANCE;
  }

  public static Tanh tanh()
  {
    return Tanh.INSTANCE;
  }

  public static Cbrt cbrt()
  {
    return Cbrt.INSTANCE;
  }

  public static Ceil ceil()
  {
    return Ceil.INSTANCE;
  }

  public static Floor floor()
  {
    return Floor.INSTANCE;
  }

  public static Exp exp()
  {
    return Exp.INSTANCE;
  }

  public static Expm1 expm1()
  {
    return Expm1.INSTANCE;
  }

  public static GetExponent getExponent()
  {
    return GetExponent.INSTANCE;
  }

  public static Log log()
  {
    return Log.INSTANCE;
  }

  public static Log10 log10()
  {
    return Log10.INSTANCE;
  }

  public static Log1p log1p()
  {
    return Log1p.INSTANCE;
  }

  public static NextUp nextUp()
  {
    return NextUp.INSTANCE;
  }

  public static Rint rint()
  {
    return Rint.INSTANCE;
  }

  public static Signum signum()
  {
    return Signum.INSTANCE;
  }

  public static Sqrt sqrt()
  {
    return Sqrt.INSTANCE;
  }

  public static ToDegrees toDegrees()
  {
    return ToDegrees.INSTANCE;
  }

  public static ToRadians toRadians()
  {
    return ToRadians.INSTANCE;
  }

  public static Ulp ulp()
  {
    return Ulp.INSTANCE;
  }

  public static BitwiseComplement bitwiseComplement()
  {
    return BitwiseComplement.INSTANCE;
  }

  public static BitwiseConvertDoubleToLongBits bitwiseConvertDoubleToLongBits()
  {
    return BitwiseConvertDoubleToLongBits.INSTANCE;
  }

  public static BitwiseConvertLongBitsToDouble bitwiseConvertLongBitsToDouble()
  {
    return BitwiseConvertLongBitsToDouble.INSTANCE;
  }

  public static BitwiseAnd bitwiseAnd()
  {
    return BitwiseAnd.INSTANCE;
  }

  public static BitwiseOr bitwiseOr()
  {
    return BitwiseOr.INSTANCE;
  }

  public static BitwiseXor bitwiseXor()
  {
    return BitwiseXor.INSTANCE;
  }

  public static BitwiseShiftLeft bitwiseShiftLeft()
  {
    return BitwiseShiftLeft.INSTANCE;
  }

  public static BitwiseShiftRight bitwiseShiftRight()
  {
    return BitwiseShiftRight.INSTANCE;
  }

  private VectorMathProcessors()
  {
    // No instantiation
  }

  /**
   * Vectorizized addition processor factory
   */
  public static final class Add extends SimpleVectorMathBivariateProcessorFactory
  {
    private static final Add INSTANCE = new Add();

    public Add()
    {
      super(Long::sum, Double::sum, Double::sum, Double::sum);
    }
  }

  public static final class Subtract extends SimpleVectorMathBivariateProcessorFactory
  {
    private static final Subtract INSTANCE = new Subtract();

    public Subtract()
    {
      super(
          (left, right) -> left - right,
          (left, right) -> (double) left - right,
          (left, right) -> left - (double) right,
          (left, right) -> left - right
      );
    }
  }

  public static final class Multiply extends SimpleVectorMathBivariateProcessorFactory
  {
    private static final Multiply INSTANCE = new Multiply();

    public Multiply()
    {
      super(Multiply::multiply, Multiply::multiply, Multiply::multiply, Multiply::multiply);
    }

    private static long multiply(long x, long y)
    {
      return x * y;
    }

    private static double multiply(long x, double y)
    {
      return (double) x * y;
    }

    private static double multiply(double x, long y)
    {
      return x * (double) y;
    }

    private static double multiply(double x, double y)
    {
      return x * y;
    }
  }

  public static final class Divide extends SimpleVectorMathBivariateProcessorFactory
  {
    private static final Divide INSTANCE = new Divide();

    public Divide()
    {
      super(
          (left, right) -> left / right,
          (left, right) -> (double) left / right,
          (left, right) -> left / (double) right,
          (left, right) -> left / right
      );
    }
  }

  public static final class LongDivide extends SimpleVectorMathBivariateLongProcessorFactory
  {
    private static final LongDivide INSTANCE = new LongDivide();

    public LongDivide()
    {
      super(
          (left, right) -> left / right,
          (left, right) -> (long) (left / right),
          (left, right) -> (long) (left / right),
          (left, right) -> (long) (left / right)
      );
    }
  }

  public static final class Modulo extends SimpleVectorMathBivariateProcessorFactory
  {
    private static final Modulo INSTANCE = new Modulo();

    public Modulo()
    {
      super(
          (left, right) -> left % right,
          (left, right) -> (double) left % right,
          (left, right) -> left % (double) right,
          (left, right) -> left % right
      );
    }
  }

  public static final class Negate extends SimpleVectorMathUnivariateProcessorFactory
  {
    private static final Negate INSTANCE = new Negate();

    public Negate()
    {
      super(
          input -> -input,
          input -> -input
      );
    }
  }

  public static final class Power extends SimpleVectorMathBivariateProcessorFactory
  {
    private static final Power INSTANCE = new Power();

    public Power()
    {
      super(
          (left, right) -> LongMath.pow(left, Ints.checkedCast(right)),
          Math::pow,
          Math::pow,
          Math::pow
      );
    }
  }

  public static final class DoublePower extends SimpleVectorMathBivariateDoubleProcessorFactory
  {
    private static final DoublePower INSTANCE = new DoublePower();

    public DoublePower()
    {
      super(Math::pow, Math::pow, Math::pow, Math::pow);
    }
  }

  public static class Max extends SimpleVectorMathBivariateProcessorFactory
  {
    private static final Max INSTANCE = new Max();

    public Max()
    {
      super(Math::max, Math::max, Math::max, Math::max);
    }
  }

  public static class Min extends SimpleVectorMathBivariateProcessorFactory
  {
    private static final Min INSTANCE = new Min();

    public Min()
    {
      super(Math::min, Math::min, Math::min, Math::min);
    }
  }

  public static class Atan2 extends SimpleVectorMathBivariateDoubleProcessorFactory
  {
    private static final Atan2 INSTANCE = new Atan2();

    public Atan2()
    {
      super(Math::atan2, Math::atan2, Math::atan2, Math::atan2);
    }
  }

  public static class CopySign extends SimpleVectorMathBivariateDoubleProcessorFactory
  {
    private static final CopySign INSTANCE = new CopySign();

    public CopySign()
    {
      super(
          (l1, l2) -> Math.copySign((double) l1, (double) l2),
          (l1, d2) -> Math.copySign((double) l1, d2),
          (d1, l2) -> Math.copySign(d1, (double) l2),
          Math::copySign
      );
    }
  }

  public static class Hypot extends SimpleVectorMathBivariateDoubleProcessorFactory
  {
    private static final Hypot INSTANCE = new Hypot();

    public Hypot()
    {
      super(Math::hypot, Math::hypot, Math::hypot, Math::hypot);
    }
  }

  public static class Remainder extends SimpleVectorMathBivariateDoubleProcessorFactory
  {
    private static final Remainder INSTANCE = new Remainder();

    public Remainder()
    {
      super(Math::IEEEremainder, Math::IEEEremainder, Math::IEEEremainder, Math::IEEEremainder);
    }
  }

  public static class NextAfter extends SimpleVectorMathBivariateDoubleProcessorFactory
  {
    private static final NextAfter INSTANCE = new NextAfter();

    public NextAfter()
    {
      super(
          (l1, l2) -> Math.nextAfter((double) l1, (double) l2),
          (l1, d2) -> Math.nextAfter((double) l1, d2),
          (d1, l2) -> Math.nextAfter(d1, (double) l2),
          Math::nextAfter
      );
    }
  }

  public static class Scalb extends SimpleVectorMathBivariateDoubleProcessorFactory
  {
    private static final Scalb INSTANCE = new Scalb();

    public Scalb()
    {
      super(
          (left, right) -> Math.scalb((double) left, (int) right),
          (left, right) -> Math.scalb((double) left, (int) right),
          (left, right) -> Math.scalb(left, (int) right),
          (left, right) -> Math.scalb(left, (int) right)
      );
    }
  }

  public static final class Abs extends SimpleVectorMathUnivariateProcessorFactory
  {
    private static final Abs INSTANCE = new Abs();

    public Abs()
    {
      super(Math::abs, Math::abs);
    }
  }

  public static final class Acos extends SimpleVectorMathUnivariateDoubleProcessorFactory
  {
    private static final Acos INSTANCE = new Acos();

    public Acos()
    {
      super(Math::acos, Math::acos);
    }
  }

  public static final class Asin extends SimpleVectorMathUnivariateDoubleProcessorFactory
  {
    private static final Asin INSTANCE = new Asin();

    public Asin()
    {
      super(Math::asin, Math::asin);
    }
  }

  public static final class Atan extends SimpleVectorMathUnivariateDoubleProcessorFactory
  {
    private static final Atan INSTANCE = new Atan();

    public Atan()
    {
      super(Math::atan, Math::atan);
    }
  }

  public static final class Cos extends SimpleVectorMathUnivariateDoubleProcessorFactory
  {
    private static final Cos INSTANCE = new Cos();

    public Cos()
    {
      super(Math::cos, Math::cos);
    }
  }

  public static final class Cosh extends SimpleVectorMathUnivariateDoubleProcessorFactory
  {
    private static final Cosh INSTANCE = new Cosh();

    public Cosh()
    {
      super(Math::cosh, Math::cosh);
    }
  }

  public static final class Cot extends SimpleVectorMathUnivariateDoubleProcessorFactory
  {
    private static final Cot INSTANCE = new Cot();

    public Cot()
    {
      super(
          (input) -> Math.cos(input) / Math.sin(input),
          (input) -> Math.cos(input) / Math.sin(input)
      );
    }
  }

  public static final class Sin extends SimpleVectorMathUnivariateDoubleProcessorFactory
  {
    private static final Sin INSTANCE = new Sin();

    public Sin()
    {
      super(Math::sin, Math::sin);
    }
  }

  public static final class Sinh extends SimpleVectorMathUnivariateDoubleProcessorFactory
  {
    private static final Sinh INSTANCE = new Sinh();

    public Sinh()
    {
      super(Math::sinh, Math::sinh);
    }
  }

  public static final class Tan extends SimpleVectorMathUnivariateDoubleProcessorFactory
  {
    private static final Tan INSTANCE = new Tan();

    public Tan()
    {
      super(Math::tan, Math::tan);
    }
  }

  public static final class Tanh extends SimpleVectorMathUnivariateDoubleProcessorFactory
  {
    private static final Tanh INSTANCE = new Tanh();

    public Tanh()
    {
      super(Math::tanh, Math::tanh);
    }
  }

  public static final class Cbrt extends SimpleVectorMathUnivariateDoubleProcessorFactory
  {
    private static final Cbrt INSTANCE = new Cbrt();

    public Cbrt()
    {
      super(Math::cbrt, Math::cbrt);
    }
  }

  public static final class Ceil extends SimpleVectorMathUnivariateDoubleProcessorFactory
  {
    private static final Ceil INSTANCE = new Ceil();

    public Ceil()
    {
      super(Math::ceil, Math::ceil);
    }
  }

  public static final class Floor extends SimpleVectorMathUnivariateDoubleProcessorFactory
  {
    private static final Floor INSTANCE = new Floor();

    public Floor()
    {
      super(Math::floor, Math::floor);
    }
  }

  public static final class Exp extends SimpleVectorMathUnivariateDoubleProcessorFactory
  {
    private static final Exp INSTANCE = new Exp();

    public Exp()
    {
      super(Math::exp, Math::exp);
    }
  }

  public static final class Expm1 extends SimpleVectorMathUnivariateDoubleProcessorFactory
  {
    private static final Expm1 INSTANCE = new Expm1();

    public Expm1()
    {
      super(Math::expm1, Math::expm1);
    }
  }

  public static class GetExponent extends SimpleVectorMathUnivariateLongProcessorFactory
  {
    private static final GetExponent INSTANCE = new GetExponent();

    public GetExponent()
    {
      super(Math::getExponent, Math::getExponent);
    }
  }

  public static final class Log extends SimpleVectorMathUnivariateDoubleProcessorFactory
  {
    private static final Log INSTANCE = new Log();

    public Log()
    {
      super(Math::log, Math::log);
    }
  }

  public static final class Log10 extends SimpleVectorMathUnivariateDoubleProcessorFactory
  {
    private static final Log10 INSTANCE = new Log10();

    public Log10()
    {
      super(Math::log10, Math::log10);
    }
  }
  public static final class Log1p extends SimpleVectorMathUnivariateDoubleProcessorFactory
  {
    private static final Log1p INSTANCE = new Log1p();

    public Log1p()
    {
      super(Math::log1p, Math::log1p);
    }
  }

  public static final class NextUp extends SimpleVectorMathUnivariateDoubleProcessorFactory
  {
    private static final NextUp INSTANCE = new NextUp();

    public NextUp()
    {
      super(l -> Math.nextUp((double) l), Math::nextUp);
    }
  }

  public static final class Rint extends SimpleVectorMathUnivariateDoubleProcessorFactory
  {
    private static final Rint INSTANCE = new Rint();

    public Rint()
    {
      super(Math::rint, Math::rint);
    }
  }

  public static final class Signum extends SimpleVectorMathUnivariateDoubleProcessorFactory
  {
    private static final Signum INSTANCE = new Signum();

    public Signum()
    {
      super(Math::signum, Math::signum);
    }
  }

  public static final class Sqrt extends SimpleVectorMathUnivariateDoubleProcessorFactory
  {
    private static final Sqrt INSTANCE = new Sqrt();

    public Sqrt()
    {
      super(Math::sqrt, Math::sqrt);
    }
  }

  public static final class ToDegrees extends SimpleVectorMathUnivariateDoubleProcessorFactory
  {
    private static final ToDegrees INSTANCE = new ToDegrees();

    public ToDegrees()
    {
      super(Math::toDegrees, Math::toDegrees);
    }
  }

  public static final class ToRadians extends SimpleVectorMathUnivariateDoubleProcessorFactory
  {
    private static final ToRadians INSTANCE = new ToRadians();

    public ToRadians()
    {
      super(Math::toRadians, Math::toRadians);
    }
  }

  public static final class Ulp extends SimpleVectorMathUnivariateDoubleProcessorFactory
  {
    private static final Ulp INSTANCE = new Ulp();

    public Ulp()
    {
      super(l -> Math.ulp((double) l), Math::ulp);
    }
  }


  public static final class BitwiseComplement extends SimpleVectorMathUnivariateLongProcessorFactory
  {
    private static final BitwiseComplement INSTANCE = new BitwiseComplement();

    public BitwiseComplement()
    {
      super(
          input -> ~input,
          input -> {
            if (input < Long.MIN_VALUE || input > Long.MAX_VALUE) {
              throw new ExpressionValidationException(
                  Function.BitwiseComplement.NAME,
                  "Possible data truncation, param [%f] is out of LONG value range",
                  input
              );
            }
            return ~((long) input);
          }
      );
    }
  }

  public static class BitwiseConvertDoubleToLongBits extends SimpleVectorMathUnivariateLongProcessorFactory
  {
    private static final BitwiseConvertDoubleToLongBits INSTANCE = new BitwiseConvertDoubleToLongBits();

    public BitwiseConvertDoubleToLongBits()
    {
      super(Double::doubleToLongBits, Double::doubleToLongBits);
    }
  }

  public static class BitwiseConvertLongBitsToDouble extends SimpleVectorMathUnivariateDoubleProcessorFactory
  {
    private static final BitwiseConvertLongBitsToDouble INSTANCE = new BitwiseConvertLongBitsToDouble();

    public BitwiseConvertLongBitsToDouble()
    {
      super(Double::longBitsToDouble, (input) -> Double.longBitsToDouble((long) input));
    }
  }

  public static class BitwiseAnd extends SimpleVectorMathBivariateLongProcessorFactory
  {
    private static final BitwiseAnd INSTANCE = new BitwiseAnd();

    public BitwiseAnd()
    {
      super(
          (left, right) -> left & right,
          (left, right) -> left & (long) right,
          (left, right) -> (long) left & right,
          (left, right) -> (long) left & (long) right
      );
    }
  }

  public static class BitwiseOr extends SimpleVectorMathBivariateLongProcessorFactory
  {
    private static final BitwiseOr INSTANCE = new BitwiseOr();

    public BitwiseOr()
    {
      super(
          (left, right) -> left | right,
          (left, right) -> left | (long) right,
          (left, right) -> (long) left | right,
          (left, right) -> (long) left | (long) right
      );
    }
  }

  public static class BitwiseXor extends SimpleVectorMathBivariateLongProcessorFactory
  {
    private static final BitwiseXor INSTANCE = new BitwiseXor();

    public BitwiseXor()
    {
      super(
          (left, right) -> left ^ right,
          (left, right) -> left ^ (long) right,
          (left, right) -> (long) left ^ right,
          (left, right) -> (long) left ^ (long) right
      );
    }
  }

  public static class BitwiseShiftLeft extends SimpleVectorMathBivariateLongProcessorFactory
  {
    private static final BitwiseShiftLeft INSTANCE = new BitwiseShiftLeft();

    public BitwiseShiftLeft()
    {
      super(
          (left, right) -> left << right,
          (left, right) -> left << (long) right,
          (left, right) -> (long) left << right,
          (left, right) -> (long) left << (long) right
      );
    }
  }

  public static class BitwiseShiftRight extends SimpleVectorMathBivariateLongProcessorFactory
  {
    private static final BitwiseShiftRight INSTANCE = new BitwiseShiftRight();

    public BitwiseShiftRight()
    {
      super(
          (left, right) -> left >> right,
          (left, right) -> left >> (long) right,
          (left, right) -> (long) left >> right,
          (left, right) -> (long) left >> (long) right
      );
    }
  }
}
