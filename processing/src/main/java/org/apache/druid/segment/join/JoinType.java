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

package org.apache.druid.segment.join;

public enum JoinType
{
  INNER {
    @Override
    public boolean isLefty()
    {
      return false;
    }

    @Override
    public boolean isRighty()
    {
      return false;
    }
  },

  LEFT {
    @Override
    public boolean isLefty()
    {
      return true;
    }

    @Override
    public boolean isRighty()
    {
      return false;
    }
  },

  RIGHT {
    @Override
    public boolean isLefty()
    {
      return false;
    }

    @Override
    public boolean isRighty()
    {
      return true;
    }
  },

  FULL {
    @Override
    public boolean isLefty()
    {
      return true;
    }

    @Override
    public boolean isRighty()
    {
      return true;
    }
  };

  /**
   * "Lefty" joins (LEFT or FULL) always include the full left-hand side, and can generate nulls on the right.
   */
  public abstract boolean isLefty();

  /**
   * "Righty" joins (RIGHT or FULL) always include the full right-hand side, and can generate nulls on the left.
   */
  public abstract boolean isRighty();
}
