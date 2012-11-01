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

package com.metamx.phonebook;

import com.google.common.base.Supplier;
import com.google.common.collect.Constraint;
import com.google.common.collect.Constraints;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Sets;
import com.metamx.common.IAE;
import com.metamx.common.logger.Logger;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A class that collects announcements for you.  Can be used to simplify start/stop logic
 *
 * Not thread-safe
 */
class StoppedPhoneBook implements PhoneBook
{
  private static final Logger log = new Logger(StoppedPhoneBook.class);

  private final Map<String, Map<String, Object>> announcements = Maps.newHashMap();
  private final Multimap<String, PhoneBookPeon> listeners = Multimaps.newSetMultimap(
      Maps.<String, Collection<PhoneBookPeon>>newHashMap(),
      new Supplier<Set<PhoneBookPeon>>()
      {
        @Override
        public Set<PhoneBookPeon> get()
        {
          final HashSet<PhoneBookPeon> theSet = Sets.newHashSet();
          return Constraints.constrainedSet(
              theSet,
              new Constraint<PhoneBookPeon>()
              {
                @Override
                public PhoneBookPeon checkElement(PhoneBookPeon element)
                {
                  if (theSet.contains(element)) {
                    throw new IAE("Listener[%s] has already been registered", element);
                  }

                  return element;
                }
              });
        }
      }
  );

  @Override
  public void start()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public void stop()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isStarted()
  {
    return false;
  }

  @Override
  public <T> void announce(String serviceName, String nodeName, T properties)
  {
    Map<String, Object> serviceAnnouncements = announcements.get(serviceName);

    if (serviceAnnouncements == null) {
      serviceAnnouncements = Maps.newHashMap();
      announcements.put(serviceName, serviceAnnouncements);
    }

    serviceAnnouncements.put(nodeName, properties);
  }

  @Override
  public void unannounce(String serviceName, String nodeName)
  {
    Map<String, Object> serviceAnnouncements = announcements.get(serviceName);

    if (serviceAnnouncements == null) {
      throw new IAE("Cannot unannounce[%s]: No announcements for service[%s]", nodeName, serviceName);
    }

    if (! serviceAnnouncements.containsKey(nodeName)) {
      throw new IAE("Cannot unannounce node[%s] on service[%s]", nodeName, serviceName);
    }

    serviceAnnouncements.remove(nodeName);
  }

  @Override
  public <T> T lookup(String serviceName, Class<? extends T> clazz)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public <T> void post(String serviceName, String nodeName, T properties)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean unpost(String serviceName, String nodeName)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public <T> void postEphemeral(String serviceName, String nodeName, T properties)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public <T> void registerListener(String serviceName, PhoneBookPeon<T> peon)
  {
    listeners.put(serviceName, peon);
  }

  @Override
  public <T> void unregisterListener(String serviceName, PhoneBookPeon<T> peon)
  {
    if (! listeners.remove(serviceName, peon)) {
      throw new IAE("Cannot unregister listener[%s] on service[%s] that wasn't first registered.", serviceName, peon);
    }
  }

  @Override
  public String combineParts(List<String> parts)
  {
    throw new UnsupportedOperationException("This should never be called");
  }

  public Map<String, Map<String, Object>> getAnnouncements()
  {
    return announcements;
  }

  public Multimap<String, PhoneBookPeon> getListeners()
  {
    return listeners;
  }
}
