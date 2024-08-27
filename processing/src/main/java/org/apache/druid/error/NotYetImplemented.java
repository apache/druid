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

package org.apache.druid.error;

/**
 * A failure class that is used to indicate that something is just not implemented yet.  This is useful when a
 * developer builds something and they intentionally do not implement a specific branch of code or type of object.
 * <p>
 * The lack of implementation is not necessarily a statement that it SHOULDN'T be implemented, it's just an indication
 * that it has not YET been implemented.  When one of these exceptions is seen, it is usually an indication that it is
 * now time to actually implement the path that was previously elided.
 * <p>
 * Oftentimes, the code path wasn't implemented because the developer thought that it wasn't actually possible to
 * see it executed.  So, collecting and providing information about why the particular path got executed is often
 * extremely helpful in understanding why it happened and accelerating the implementation of what the correct behavior
 * should be.
 */
public class NotYetImplemented extends DruidException.Failure
{
  public static DruidException ex(Throwable t, String msg, Object... args)
  {
    return DruidException.fromFailure(new NotYetImplemented(t, msg, args));
  }

  private final Throwable t;
  private final String msg;
  private final Object[] args;

  public NotYetImplemented(Throwable t, String msg, Object[] args)
  {
    super("notYetImplemented");
    this.t = t;
    this.msg = msg;
    this.args = args;
  }


  @Override
  protected DruidException makeException(DruidException.DruidExceptionBuilder bob)
  {
    bob = bob.forPersona(DruidException.Persona.DEVELOPER)
             .ofCategory(DruidException.Category.DEFENSIVE);

    if (t == null) {
      return bob.build(msg, args);
    } else {
      return bob.build(t, msg, args);
    }
  }
}
