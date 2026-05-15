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

package org.apache.druid.server.coordinator.rules;

import org.apache.druid.error.DruidException;
import org.apache.druid.error.InvalidInput;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Shared glob compilation + matching helpers used by partial-load matchers that match operator-supplied glob patterns
 * against strings (projection names, stringified cluster-group values, etc.). Supported metacharacters:
 * <ul>
 *   <li>{@code *} — any sequence of characters (including empty)</li>
 *   <li>{@code ?} — any single character</li>
 *   <li>{@code \} — escapes the following character so it is treated literally; use {@code \*}, {@code \?}, or
 *       {@code \\} to match a literal {@code *}, {@code ?}, or {@code \}. A trailing unescaped {@code \} is
 *       rejected.</li>
 * </ul>
 * Other characters are literal; regex metacharacters in literal positions are escaped automatically.
 */
public final class Globs
{
  private Globs()
  {
    // no instantiation
  }

  /**
   * Translates a glob pattern into a regex pattern that matches the entire input string.
   *
   * @throws org.apache.druid.error.DruidException if {@code glob} ends with an unescaped backslash
   */
  public static String globToRegex(String glob)
  {
    final StringBuilder sb = new StringBuilder(glob.length() + 4);
    boolean escaping = false;
    for (int i = 0; i < glob.length(); i++) {
      final char c = glob.charAt(i);
      if (escaping) {
        appendLiteral(sb, c);
        escaping = false;
        continue;
      }
      switch (c) {
        case '\\':
          escaping = true;
          break;
        case '*':
          sb.append(".*");
          break;
        case '?':
          sb.append('.');
          break;
        default:
          appendLiteral(sb, c);
      }
    }
    if (escaping) {
      throw InvalidInput.exception("Glob pattern [%s] ends with an unescaped backslash", glob);
    }
    return sb.toString();
  }

  public static List<Pattern> compileAll(List<String> globs)
  {
    if (globs.isEmpty()) {
      return List.of();
    }
    final List<Pattern> compiled = new ArrayList<>(globs.size());
    for (String glob : globs) {
      compiled.add(Pattern.compile(globToRegex(glob)));
    }
    return List.copyOf(compiled);
  }

  public static boolean matchesAny(String name, List<Pattern> patterns)
  {
    for (Pattern pattern : patterns) {
      if (pattern.matcher(name).matches()) {
        return true;
      }
    }
    return false;
  }

  /**
   * Compile a glob, special-casing the literal {@code "*"} to a {@link CompiledGlob#matchAny}
   * marker that matches any value including null — mirrors the matcher's {@code CompiledGlob}.
   */
  static CompiledGlob compile(String glob)
  {
    if ("*".equals(glob)) {
      return CompiledGlob.MATCH_ANY;
    }
    return new CompiledGlob(false, Pattern.compile(globToRegex(glob)));
  }

  private static void appendLiteral(StringBuilder sb, char c)
  {
    switch (c) {
      case '.':
      case '(':
      case ')':
      case '[':
      case ']':
      case '{':
      case '}':
      case '+':
      case '|':
      case '^':
      case '$':
      case '\\':
      case '*':
      case '?':
        sb.append('\\').append(c);
        break;
      default:
        sb.append(c);
    }
  }

  /**
   * Compiled, per-column glob. The literal {@code "*"} short-circuits to a "match any value, including null" flag;
   * any other glob compiles to a regex matched against the rendered tuple value (and never matches null).
   */
  public static final class CompiledGlob
  {
    public static final CompiledGlob MATCH_ANY = new CompiledGlob(true, null);

    final boolean matchAny;
    @Nullable
    final Pattern pattern;

    private CompiledGlob(boolean matchAny, @Nullable Pattern pattern)
    {
      if (matchAny) {
        DruidException.conditionalDefensive(pattern == null, "matchAny=true must not carry a pattern");
      } else {
        DruidException.conditionalDefensive(pattern != null, "pattern must be non-null when matchAny=false");
      }
      this.matchAny = matchAny;
      this.pattern = pattern;
    }

    public boolean matches(@Nullable String value)
    {
      if (matchAny) {
        return true;
      }
      if (value == null) {
        return false;
      }
      return pattern.matcher(value).matches();
    }

    public boolean isMatchAny()
    {
      return matchAny;
    }
  }
}
