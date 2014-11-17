/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
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

package io.druid.metadata;

import io.druid.server.coordinator.rules.Rule;

import java.util.List;
import java.util.Map;

/**
 */
public interface MetadataRuleManager
{
  public void start();

  public void stop();

  public void poll();

  public Map<String, List<Rule>> getAllRules();

  public List<Rule> getRules(final String dataSource);

  public List<Rule> getRulesWithDefault(final String dataSource);

  public boolean overrideRule(final String dataSource, final List<Rule> newRules);
}
