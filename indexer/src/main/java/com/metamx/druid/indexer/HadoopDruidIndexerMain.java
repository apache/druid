/*
 * Druid - a distributed column store.
 * Copyright (C) 2012  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package com.metamx.druid.indexer;

import com.metamx.common.lifecycle.Lifecycle;
import com.metamx.common.logger.Logger;

/**
 */
public class HadoopDruidIndexerMain
{
  private static final Logger log = new Logger(HadoopDruidIndexerMain.class);

  public static void main(String[] args) throws Exception
  {
    if (args.length < 1 || args.length > 2) {
      HadoopDruidIndexerNode.printHelp();
      System.exit(2);
    }

    HadoopDruidIndexerNode node = HadoopDruidIndexerNode.builder().build();

    node.setIntervalSpec(args.length == 1 ? null : args[0]);
    node.setArgumentSpec(args[args.length == 1 ? 0 : 1]);

    Lifecycle lifecycle = new Lifecycle();
    lifecycle.addManagedInstance(node);

    try {
      lifecycle.start();
    }
    catch (Throwable t) {
      log.info(t, "Throwable caught at startup, committing seppuku");
      Thread.sleep(500);
      HadoopDruidIndexerNode.printHelp();
      System.exit(1);
    }
  }
}
