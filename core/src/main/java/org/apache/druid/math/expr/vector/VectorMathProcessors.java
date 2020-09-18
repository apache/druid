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
import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExprType;
import org.apache.druid.math.expr.Exprs;

public class VectorMathProcessors
{
  public static <T> VectorExprProcessor<T> plus(Expr.VectorInputBindingTypes inputTypes, Expr left, Expr right)
  {
    final ExprType leftType = left.getOutputType(inputTypes);
    final ExprType rightType = right.getOutputType(inputTypes);

    final int maxVectorSize = inputTypes.getMaxVectorSize();
    BivariateFunctionVectorProcessor<?, ?, ?> processor = null;
    if (ExprType.LONG.equals(leftType)) {
      if (ExprType.LONG.equals(rightType)) {
        processor = new LongOutLongsInFunctionVectorProcessor(
            left.buildVectorized(inputTypes),
            right.buildVectorized(inputTypes),
            maxVectorSize
        )
        {
          @Override
          public long apply(long left, long right)
          {
            return left + right;
          }
        };
      } else if (ExprType.DOUBLE.equals(rightType)) {
        processor = new DoubleOutLongDoubleInFunctionVectorProcessor(
            left.buildVectorized(inputTypes),
            right.buildVectorized(inputTypes),
            maxVectorSize
        )
        {
          @Override
          public double apply(long left, double right)
          {
            return (double) left + right;
          }
        };
      }
    } else if (ExprType.DOUBLE.equals(leftType)) {
      if (ExprType.LONG.equals(rightType)) {
        processor = new DoubleOutDoubleLongInFunctionVectorProcessor(
            left.buildVectorized(inputTypes),
            right.buildVectorized(inputTypes),
            maxVectorSize
        )
        {
          @Override
          public double apply(double left, long right)
          {
            return left + (double) right;
          }
        };
      } else if (ExprType.DOUBLE.equals(rightType)) {
        processor = new DoubleOutDoublesInFunctionVectorProcessor(
            left.buildVectorized(inputTypes),
            right.buildVectorized(inputTypes),
            maxVectorSize
        )
        {
          @Override
          public double apply(double left, double right)
          {
            return left + right;
          }
        };
      }
    }
    if (processor == null) {
      throw Exprs.cannotVectorize();
    }
    return (VectorExprProcessor<T>) processor;
  }

  public static <T> VectorExprProcessor<T> minus(Expr.VectorInputBindingTypes inputTypes, Expr left, Expr right)
  {
    final ExprType leftType = left.getOutputType(inputTypes);
    final ExprType rightType = right.getOutputType(inputTypes);

    final int maxVectorSize = inputTypes.getMaxVectorSize();
    BivariateFunctionVectorProcessor<?, ?, ?> processor = null;
    if (ExprType.LONG.equals(leftType)) {
      if (ExprType.LONG.equals(rightType)) {
        processor = new LongOutLongsInFunctionVectorProcessor(
            left.buildVectorized(inputTypes),
            right.buildVectorized(inputTypes),
            maxVectorSize
        )
        {
          @Override
          public long apply(long left, long right)
          {
            return left - right;
          }
        };
      } else if (ExprType.DOUBLE.equals(rightType)) {
        processor = new DoubleOutLongDoubleInFunctionVectorProcessor(
            left.buildVectorized(inputTypes),
            right.buildVectorized(inputTypes),
            maxVectorSize
        )
        {
          @Override
          public double apply(long left, double right)
          {
            return (double) left - right;
          }
        };
      }
    } else if (ExprType.DOUBLE.equals(leftType)) {
      if (ExprType.LONG.equals(rightType)) {
        processor = new DoubleOutDoubleLongInFunctionVectorProcessor(
            left.buildVectorized(inputTypes),
            right.buildVectorized(inputTypes),
            maxVectorSize
        )
        {
          @Override
          public double apply(double left, long right)
          {
            return left - (double) right;
          }
        };
      } else if (ExprType.DOUBLE.equals(rightType)) {
        processor = new DoubleOutDoublesInFunctionVectorProcessor(
            left.buildVectorized(inputTypes),
            right.buildVectorized(inputTypes),
            maxVectorSize
        )
        {
          @Override
          public double apply(double left, double right)
          {
            return left - right;
          }
        };
      }
    }
    if (processor == null) {
      throw Exprs.cannotVectorize();
    }
    return (VectorExprProcessor<T>) processor;
  }

  public static <T> VectorExprProcessor<T> multiply(Expr.VectorInputBindingTypes inputTypes, Expr left, Expr right)
  {
    final ExprType leftType = left.getOutputType(inputTypes);
    final ExprType rightType = right.getOutputType(inputTypes);

    final int maxVectorSize = inputTypes.getMaxVectorSize();
    BivariateFunctionVectorProcessor<?, ?, ?> processor = null;
    if (ExprType.LONG.equals(leftType)) {
      if (ExprType.LONG.equals(rightType)) {
        processor = new LongOutLongsInFunctionVectorProcessor(
            left.buildVectorized(inputTypes),
            right.buildVectorized(inputTypes),
            maxVectorSize
        )
        {
          @Override
          public long apply(long left, long right)
          {
            return left * right;
          }
        };
      } else if (ExprType.DOUBLE.equals(rightType)) {
        processor = new DoubleOutLongDoubleInFunctionVectorProcessor(
            left.buildVectorized(inputTypes),
            right.buildVectorized(inputTypes),
            maxVectorSize
        )
        {
          @Override
          public double apply(long left, double right)
          {
            return (double) left * right;
          }
        };
      }
    } else if (ExprType.DOUBLE.equals(leftType)) {
      if (ExprType.LONG.equals(rightType)) {
        processor = new DoubleOutDoubleLongInFunctionVectorProcessor(
            left.buildVectorized(inputTypes),
            right.buildVectorized(inputTypes),
            maxVectorSize
        )
        {
          @Override
          public double apply(double left, long right)
          {
            return left * (double) right;
          }
        };
      } else if (ExprType.DOUBLE.equals(rightType)) {
        processor = new DoubleOutDoublesInFunctionVectorProcessor(
            left.buildVectorized(inputTypes),
            right.buildVectorized(inputTypes),
            maxVectorSize
        )
        {
          @Override
          public double apply(double left, double right)
          {
            return left * right;
          }
        };
      }
    }
    if (processor == null) {
      throw Exprs.cannotVectorize();
    }
    return (VectorExprProcessor<T>) processor;
  }

  public static <T> VectorExprProcessor<T> divide(Expr.VectorInputBindingTypes inputTypes, Expr left, Expr right)
  {
    final ExprType leftType = left.getOutputType(inputTypes);
    final ExprType rightType = right.getOutputType(inputTypes);

    final int maxVectorSize = inputTypes.getMaxVectorSize();
    BivariateFunctionVectorProcessor<?, ?, ?> processor = null;
    if (ExprType.LONG.equals(leftType)) {
      if (ExprType.LONG.equals(rightType)) {
        processor = new LongOutLongsInFunctionVectorProcessor(
            left.buildVectorized(inputTypes),
            right.buildVectorized(inputTypes),
            maxVectorSize
        )
        {
          @Override
          public long apply(long left, long right)
          {
            return left / right;
          }
        };
      } else if (ExprType.DOUBLE.equals(rightType)) {
        processor = new DoubleOutLongDoubleInFunctionVectorProcessor(
            left.buildVectorized(inputTypes),
            right.buildVectorized(inputTypes),
            maxVectorSize
        )
        {
          @Override
          public double apply(long left, double right)
          {
            return (double) left / right;
          }
        };
      }
    } else if (ExprType.DOUBLE.equals(leftType)) {
      if (ExprType.LONG.equals(rightType)) {
        processor = new DoubleOutDoubleLongInFunctionVectorProcessor(
            left.buildVectorized(inputTypes),
            right.buildVectorized(inputTypes),
            maxVectorSize
        )
        {
          @Override
          public double apply(double left, long right)
          {
            return left / (double) right;
          }
        };
      } else if (ExprType.DOUBLE.equals(rightType)) {
        processor = new DoubleOutDoublesInFunctionVectorProcessor(
            left.buildVectorized(inputTypes),
            right.buildVectorized(inputTypes),
            maxVectorSize
        )
        {
          @Override
          public double apply(double left, double right)
          {
            return left / right;
          }
        };
      }
    }
    if (processor == null) {
      throw Exprs.cannotVectorize();
    }
    return (VectorExprProcessor<T>) processor;
  }

  public static <T> VectorExprProcessor<T> modulo(Expr.VectorInputBindingTypes inputTypes, Expr left, Expr right)
  {
    final ExprType leftType = left.getOutputType(inputTypes);
    final ExprType rightType = right.getOutputType(inputTypes);

    final int maxVectorSize = inputTypes.getMaxVectorSize();
    BivariateFunctionVectorProcessor<?, ?, ?> processor = null;
    if (ExprType.LONG.equals(leftType)) {
      if (ExprType.LONG.equals(rightType)) {
        processor = new LongOutLongsInFunctionVectorProcessor(
            left.buildVectorized(inputTypes),
            right.buildVectorized(inputTypes),
            maxVectorSize
        )
        {
          @Override
          public long apply(long left, long right)
          {
            return left % right;
          }
        };
      } else if (ExprType.DOUBLE.equals(rightType)) {
        processor = new DoubleOutLongDoubleInFunctionVectorProcessor(
            left.buildVectorized(inputTypes),
            right.buildVectorized(inputTypes),
            maxVectorSize
        )
        {
          @Override
          public double apply(long left, double right)
          {
            return (double) left % right;
          }
        };
      }
    } else if (ExprType.DOUBLE.equals(leftType)) {
      if (ExprType.LONG.equals(rightType)) {
        processor = new DoubleOutDoubleLongInFunctionVectorProcessor(
            left.buildVectorized(inputTypes),
            right.buildVectorized(inputTypes),
            maxVectorSize
        )
        {
          @Override
          public double apply(double left, long right)
          {
            return left % (double) right;
          }
        };
      } else if (ExprType.DOUBLE.equals(rightType)) {
        processor = new DoubleOutDoublesInFunctionVectorProcessor(
            left.buildVectorized(inputTypes),
            right.buildVectorized(inputTypes),
            maxVectorSize
        )
        {
          @Override
          public double apply(double left, double right)
          {
            return left % right;
          }
        };
      }
    }
    if (processor == null) {
      throw Exprs.cannotVectorize();
    }
    return (VectorExprProcessor<T>) processor;
  }

  public static <T> VectorExprProcessor<T> negate(Expr.VectorInputBindingTypes inputTypes, Expr arg)
  {
    final ExprType inputType = arg.getOutputType(inputTypes);

    final int maxVectorSize = inputTypes.getMaxVectorSize();
    VectorExprProcessor<?> processor = null;
    if (ExprType.LONG.equals(inputType)) {
      processor = new LongOutLongInFunctionVectorProcessor(arg.buildVectorized(inputTypes), maxVectorSize)
      {
        @Override
        public long apply(long input)
        {
          return -input;
        }
      };
    } else if (ExprType.DOUBLE.equals(inputType)) {
      processor = new DoubleOutDoubleInFunctionVectorProcessor(arg.buildVectorized(inputTypes), maxVectorSize)
      {
        @Override
        public double apply(double input)
        {
          return -input;
        }
      };
    }
    if (processor == null) {
      throw Exprs.cannotVectorize();
    }
    return (VectorExprProcessor<T>) processor;
  }

  public static <T> VectorExprProcessor<T> power(Expr.VectorInputBindingTypes inputTypes, Expr left, Expr right)
  {
    final ExprType leftType = left.getOutputType(inputTypes);
    final ExprType rightType = right.getOutputType(inputTypes);

    final int maxVectorSize = inputTypes.getMaxVectorSize();
    BivariateFunctionVectorProcessor<?, ?, ?> processor = null;
    if (ExprType.LONG.equals(leftType)) {
      if (ExprType.LONG.equals(rightType)) {
        processor = new LongOutLongsInFunctionVectorProcessor(
            left.buildVectorized(inputTypes),
            right.buildVectorized(inputTypes),
            maxVectorSize
        )
        {
          @Override
          public long apply(long left, long right)
          {
            return LongMath.pow(left, Ints.checkedCast(right));
          }
        };
      } else if (ExprType.DOUBLE.equals(rightType)) {
        processor = new DoubleOutLongDoubleInFunctionVectorProcessor(
            left.buildVectorized(inputTypes),
            right.buildVectorized(inputTypes),
            maxVectorSize
        )
        {
          @Override
          public double apply(long left, double right)
          {
            return Math.pow(left, right);
          }
        };
      }
    } else if (ExprType.DOUBLE.equals(leftType)) {
      if (ExprType.LONG.equals(rightType)) {
        processor = new DoubleOutDoubleLongInFunctionVectorProcessor(
            left.buildVectorized(inputTypes),
            right.buildVectorized(inputTypes),
            maxVectorSize
        )
        {
          @Override
          public double apply(double left, long right)
          {
            return Math.pow(left, right);
          }
        };
      } else if (ExprType.DOUBLE.equals(rightType)) {
        processor = new DoubleOutDoublesInFunctionVectorProcessor(
            left.buildVectorized(inputTypes),
            right.buildVectorized(inputTypes),
            maxVectorSize
        )
        {
          @Override
          public double apply(double left, double right)
          {
            return Math.pow(left, right);
          }
        };
      }
    }
    if (processor == null) {
      throw Exprs.cannotVectorize();
    }
    return (VectorExprProcessor<T>) processor;
  }

  public static <T> VectorExprProcessor<T> doublePower(Expr.VectorInputBindingTypes inputTypes, Expr left, Expr right)
  {
    BivariateFunctionVectorProcessor<?, ?, ?> processor = null;
    if (ExprType.LONG.equals(left.getOutputType(inputTypes))) {
      if (ExprType.LONG.equals(right.getOutputType(inputTypes))) {
        processor = new DoubleOutLongsInFunctionVectorProcessor(
            left.buildVectorized(inputTypes),
            right.buildVectorized(inputTypes),
            inputTypes.getMaxVectorSize()
        )
        {
          @Override
          public double apply(long left, long right)
          {
            return Math.pow(left, right);
          }
        };
      }
    }

    if (processor != null) {
      return (VectorExprProcessor<T>) processor;
    }
    return power(inputTypes, left, right);
  }

  public static <T> VectorExprProcessor<T> max(Expr.VectorInputBindingTypes inputTypes, Expr left, Expr right)
  {
    final ExprType leftType = left.getOutputType(inputTypes);
    final ExprType rightType = right.getOutputType(inputTypes);

    final int maxVectorSize = inputTypes.getMaxVectorSize();
    BivariateFunctionVectorProcessor<?, ?, ?> processor = null;
    if (ExprType.LONG.equals(leftType)) {
      if (ExprType.LONG.equals(rightType)) {
        processor = new LongOutLongsInFunctionVectorProcessor(
            left.buildVectorized(inputTypes),
            right.buildVectorized(inputTypes),
            maxVectorSize
        )
        {
          @Override
          public long apply(long left, long right)
          {
            return Math.max(left, right);
          }
        };
      } else if (ExprType.DOUBLE.equals(rightType)) {
        processor = new DoubleOutLongDoubleInFunctionVectorProcessor(
            left.buildVectorized(inputTypes),
            right.buildVectorized(inputTypes),
            maxVectorSize
        )
        {
          @Override
          public double apply(long left, double right)
          {
            return Math.max(left, right);
          }
        };
      }
    } else if (ExprType.DOUBLE.equals(leftType)) {
      if (ExprType.LONG.equals(rightType)) {
        processor = new DoubleOutDoubleLongInFunctionVectorProcessor(
            left.buildVectorized(inputTypes),
            right.buildVectorized(inputTypes),
            maxVectorSize
        )
        {
          @Override
          public double apply(double left, long right)
          {
            return Math.max(left, right);
          }
        };
      } else if (ExprType.DOUBLE.equals(rightType)) {
        processor = new DoubleOutDoublesInFunctionVectorProcessor(
            left.buildVectorized(inputTypes),
            right.buildVectorized(inputTypes),
            maxVectorSize
        )
        {
          @Override
          public double apply(double left, double right)
          {
            return Math.max(left, right);
          }
        };
      }
    }
    if (processor == null) {
      throw Exprs.cannotVectorize();
    }
    return (VectorExprProcessor<T>) processor;
  }

  public static <T> VectorExprProcessor<T> min(Expr.VectorInputBindingTypes inputTypes, Expr left, Expr right)
  {
    final ExprType leftType = left.getOutputType(inputTypes);
    final ExprType rightType = right.getOutputType(inputTypes);

    final int maxVectorSize = inputTypes.getMaxVectorSize();
    BivariateFunctionVectorProcessor<?, ?, ?> processor = null;
    if (ExprType.LONG.equals(leftType)) {
      if (ExprType.LONG.equals(rightType)) {
        processor = new LongOutLongsInFunctionVectorProcessor(
            left.buildVectorized(inputTypes),
            right.buildVectorized(inputTypes),
            maxVectorSize
        )
        {
          @Override
          public long apply(long left, long right)
          {
            return Math.min(left, right);
          }
        };
      } else if (ExprType.DOUBLE.equals(rightType)) {
        processor = new DoubleOutLongDoubleInFunctionVectorProcessor(
            left.buildVectorized(inputTypes),
            right.buildVectorized(inputTypes),
            maxVectorSize
        )
        {
          @Override
          public double apply(long left, double right)
          {
            return Math.min(left, right);
          }
        };
      }
    } else if (ExprType.DOUBLE.equals(leftType)) {
      if (ExprType.LONG.equals(rightType)) {
        processor = new DoubleOutDoubleLongInFunctionVectorProcessor(
            left.buildVectorized(inputTypes),
            right.buildVectorized(inputTypes),
            maxVectorSize
        )
        {
          @Override
          public double apply(double left, long right)
          {
            return Math.min(left, right);
          }
        };
      } else if (ExprType.DOUBLE.equals(rightType)) {
        processor = new DoubleOutDoublesInFunctionVectorProcessor(
            left.buildVectorized(inputTypes),
            right.buildVectorized(inputTypes),
            maxVectorSize
        )
        {
          @Override
          public double apply(double left, double right)
          {
            return Math.min(left, right);
          }
        };
      }
    }
    if (processor == null) {
      throw Exprs.cannotVectorize();
    }
    return (VectorExprProcessor<T>) processor;
  }

  public static <T> VectorExprProcessor<T> atan(Expr.VectorInputBindingTypes inputTypes, Expr arg)
  {
    ExprType inputType = arg.getOutputType(inputTypes);
    VectorExprProcessor<?> processor = null;
    if (ExprType.LONG.equals(inputType)) {
      processor = new LongOutDoubleInFunctionVectorProcessor(
          arg.buildVectorized(inputTypes),
          inputTypes.getMaxVectorSize()
      )
      {
        @Override
        public double apply(long input)
        {
          return Math.atan(input);
        }
      };
    } else if (ExprType.DOUBLE.equals(inputType)) {
      processor = new DoubleOutDoubleInFunctionVectorProcessor(
          arg.buildVectorized(inputTypes),
          inputTypes.getMaxVectorSize()
      )
      {
        @Override
        public double apply(double input)
        {
          return Math.atan(input);
        }
      };
    }

    if (processor == null) {
      throw Exprs.cannotVectorize();
    }

    return (VectorExprProcessor<T>) processor;
  }

  public static <T> VectorExprProcessor<T> cos(Expr.VectorInputBindingTypes inputTypes, Expr arg)
  {
    ExprType inputType = arg.getOutputType(inputTypes);
    VectorExprProcessor<?> processor = null;
    if (ExprType.LONG.equals(inputType)) {
      processor = new LongOutDoubleInFunctionVectorProcessor(
          arg.buildVectorized(inputTypes),
          inputTypes.getMaxVectorSize()
      )
      {
        @Override
        public double apply(long input)
        {
          return Math.cos(input);
        }
      };
    } else if (ExprType.DOUBLE.equals(inputType)) {
      processor = new DoubleOutDoubleInFunctionVectorProcessor(
          arg.buildVectorized(inputTypes),
          inputTypes.getMaxVectorSize()
      )
      {
        @Override
        public double apply(double input)
        {
          return Math.cos(input);
        }
      };
    }

    if (processor == null) {
      throw Exprs.cannotVectorize();
    }

    return (VectorExprProcessor<T>) processor;
  }

  public static <T> VectorExprProcessor<T> cosh(Expr.VectorInputBindingTypes inputTypes, Expr arg)
  {
    ExprType inputType = arg.getOutputType(inputTypes);
    VectorExprProcessor<?> processor = null;
    if (ExprType.LONG.equals(inputType)) {
      processor = new LongOutDoubleInFunctionVectorProcessor(
          arg.buildVectorized(inputTypes),
          inputTypes.getMaxVectorSize()
      )
      {
        @Override
        public double apply(long input)
        {
          return Math.cosh(input);
        }
      };
    } else if (ExprType.DOUBLE.equals(inputType)) {
      processor = new DoubleOutDoubleInFunctionVectorProcessor(
          arg.buildVectorized(inputTypes),
          inputTypes.getMaxVectorSize()
      )
      {
        @Override
        public double apply(double input)
        {
          return Math.cosh(input);
        }
      };
    }

    if (processor == null) {
      throw Exprs.cannotVectorize();
    }

    return (VectorExprProcessor<T>) processor;
  }

  public static <T> VectorExprProcessor<T> cot(Expr.VectorInputBindingTypes inputTypes, Expr arg)
  {
    ExprType inputType = arg.getOutputType(inputTypes);
    VectorExprProcessor<?> processor = null;
    if (ExprType.LONG.equals(inputType)) {
      processor = new LongOutDoubleInFunctionVectorProcessor(
          arg.buildVectorized(inputTypes),
          inputTypes.getMaxVectorSize()
      )
      {
        @Override
        public double apply(long input)
        {
          return Math.cos(input) / Math.sin(input);
        }
      };
    } else if (ExprType.DOUBLE.equals(inputType)) {
      processor = new DoubleOutDoubleInFunctionVectorProcessor(
          arg.buildVectorized(inputTypes),
          inputTypes.getMaxVectorSize()
      )
      {
        @Override
        public double apply(double input)
        {
          return Math.cos(input) / Math.sin(input);
        }
      };
    }

    if (processor == null) {
      throw Exprs.cannotVectorize();
    }

    return (VectorExprProcessor<T>) processor;
  }

  public static <T> VectorExprProcessor<T> sin(Expr.VectorInputBindingTypes inputTypes, Expr arg)
  {
    ExprType inputType = arg.getOutputType(inputTypes);
    VectorExprProcessor<?> processor = null;
    if (ExprType.LONG.equals(inputType)) {
      processor = new LongOutDoubleInFunctionVectorProcessor(
          arg.buildVectorized(inputTypes),
          inputTypes.getMaxVectorSize()
      )
      {
        @Override
        public double apply(long input)
        {
          return Math.sin(input);
        }
      };
    } else if (ExprType.DOUBLE.equals(inputType)) {
      processor = new DoubleOutDoubleInFunctionVectorProcessor(
          arg.buildVectorized(inputTypes),
          inputTypes.getMaxVectorSize()
      )
      {
        @Override
        public double apply(double input)
        {
          return Math.sin(input);
        }
      };
    }

    if (processor == null) {
      throw Exprs.cannotVectorize();
    }

    return (VectorExprProcessor<T>) processor;
  }

  public static <T> VectorExprProcessor<T> sinh(Expr.VectorInputBindingTypes inputTypes, Expr arg)
  {
    ExprType inputType = arg.getOutputType(inputTypes);
    VectorExprProcessor<?> processor = null;
    if (ExprType.LONG.equals(inputType)) {
      processor = new LongOutDoubleInFunctionVectorProcessor(
          arg.buildVectorized(inputTypes),
          inputTypes.getMaxVectorSize()
      )
      {
        @Override
        public double apply(long input)
        {
          return Math.sinh(input);
        }
      };
    } else if (ExprType.DOUBLE.equals(inputType)) {
      processor = new DoubleOutDoubleInFunctionVectorProcessor(
          arg.buildVectorized(inputTypes),
          inputTypes.getMaxVectorSize()
      )
      {
        @Override
        public double apply(double input)
        {
          return Math.sinh(input);
        }
      };
    }

    if (processor == null) {
      throw Exprs.cannotVectorize();
    }

    return (VectorExprProcessor<T>) processor;
  }

  public static <T> VectorExprProcessor<T> tan(Expr.VectorInputBindingTypes inputTypes, Expr arg)
  {
    ExprType inputType = arg.getOutputType(inputTypes);
    VectorExprProcessor<?> processor = null;
    if (ExprType.LONG.equals(inputType)) {
      processor = new LongOutDoubleInFunctionVectorProcessor(
          arg.buildVectorized(inputTypes),
          inputTypes.getMaxVectorSize()
      )
      {
        @Override
        public double apply(long input)
        {
          return Math.tan(input);
        }
      };
    } else if (ExprType.DOUBLE.equals(inputType)) {
      processor = new DoubleOutDoubleInFunctionVectorProcessor(
          arg.buildVectorized(inputTypes),
          inputTypes.getMaxVectorSize()
      )
      {
        @Override
        public double apply(double input)
        {
          return Math.tan(input);
        }
      };
    }

    if (processor == null) {
      throw Exprs.cannotVectorize();
    }

    return (VectorExprProcessor<T>) processor;
  }

  public static <T> VectorExprProcessor<T> tanh(Expr.VectorInputBindingTypes inputTypes, Expr arg)
  {
    ExprType inputType = arg.getOutputType(inputTypes);
    VectorExprProcessor<?> processor = null;
    if (ExprType.LONG.equals(inputType)) {
      processor = new LongOutDoubleInFunctionVectorProcessor(
          arg.buildVectorized(inputTypes),
          inputTypes.getMaxVectorSize()
      )
      {
        @Override
        public double apply(long input)
        {
          return Math.tanh(input);
        }
      };
    } else if (ExprType.DOUBLE.equals(inputType)) {
      processor = new DoubleOutDoubleInFunctionVectorProcessor(
          arg.buildVectorized(inputTypes),
          inputTypes.getMaxVectorSize()
      )
      {
        @Override
        public double apply(double input)
        {
          return Math.tanh(input);
        }
      };
    }

    if (processor == null) {
      throw Exprs.cannotVectorize();
    }

    return (VectorExprProcessor<T>) processor;
  }

  private VectorMathProcessors()
  {
    // No instantiation
  }
}
