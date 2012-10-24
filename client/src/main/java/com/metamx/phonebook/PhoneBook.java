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

import java.util.List;

/**
 * A PhoneBook object is just like a phone book.  You can publish ("announce") your services to it as well as
 * find out about other people publishing their services (registerListener).
 *
 * Finding out about other people's announcements is accomplished by employing a Peon, who gets notified of
 * announcements coming and going and does something with them.
 */
public interface PhoneBook
{
  public void start();
  public void stop();
  public boolean isStarted();
  public <T> void announce(String serviceName, String nodeName, T properties);
  public void unannounce(String serviceName, String nodeName);
  public <T> T lookup(String serviceName, Class<? extends T> clazz);
  public <T> void post(String serviceName, String nodeName, T properties);
  public boolean unpost(String serviceName, String nodeName);
  public <T> void postEphemeral(String serviceName, String nodeName, T properties);
  public <T> void registerListener(String serviceName, PhoneBookPeon<T> peon);
  public <T> void unregisterListener(String serviceName, PhoneBookPeon<T> peon);

  /**
   * A method to combine a number of hierarchical parts into a String that would "work" for this PhoneBook implementation.
   *
   * I.e., a call to combineParts("A", "B") should return the String "serviceName" that can be used to register a
   * listener underneath something that was announced via a call to announce("A", "B", {})
   *
   * @param parts
   * @return
   */
  public String combineParts(List<String> parts);
}
