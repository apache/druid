/**
 * ******** NOTICE ************
 * https://github.com/FasterXML/jackson-databind/blob/2.16/LICENSE
 * Stating the Credits to FasterXML project based on Apache Lic.
 * No changes are done in the file other than attaching notice.
 * Copyright 2023 Apache Druid
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * ****************************
 * Container for standard {@link PropertyNamingStrategy} implementations
 * and singleton instances.
 * <p>
 * Added in Jackson 2.12 to resolve issue
 * <a href="https://github.com/FasterXML/jackson-databind/issues/2715">databind#2715</a>.
 *
 * @since 2.12
 *
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

package com.fasterxml.jackson.databind;

import com.fasterxml.jackson.databind.cfg.MapperConfig;
import com.fasterxml.jackson.databind.introspect.AnnotatedField;
import com.fasterxml.jackson.databind.introspect.AnnotatedMethod;
import com.fasterxml.jackson.databind.introspect.AnnotatedParameter;

import java.io.Serializable;
import java.util.Locale;


public abstract class PropertyNamingStrategies implements Serializable
{
  private static final long serialVersionUID = 2L;

    /*
    /**********************************************************************
    /* Static instances that may be referenced
    /**********************************************************************
     */

  /**
   * Naming convention used in Java, where words other than first are capitalized
   * and no separator is used between words. Since this is the native Java naming convention,
   * naming strategy will not do any transformation between names in data (JSON) and
   * POJOS.
   * <p>
   * Example external property names would be "numberValue", "namingStrategy", "theDefiniteProof".
   */
  public static final PropertyNamingStrategy LOWER_CAMEL_CASE = LowerCamelCaseStrategy.INSTANCE;

  /**
   * Naming convention used in languages like Pascal, where all words are capitalized
   * and no separator is used between words.
   * See {@link UpperCamelCaseStrategy} for details.
   * <p>
   * Example external property names would be "NumberValue", "NamingStrategy", "TheDefiniteProof".
   */
  public static final PropertyNamingStrategy UPPER_CAMEL_CASE = UpperCamelCaseStrategy.INSTANCE;

  /**
   * Naming convention used in languages like C, where words are in lower-case
   * letters, separated by underscores.
   * See {@link SnakeCaseStrategy} for details.
   * <p>
   * Example external property names would be "number_value", "naming_strategy", "the_definite_proof".
   */
  public static final PropertyNamingStrategy SNAKE_CASE = SnakeCaseStrategy.INSTANCE;

  /**
   * Naming convention in which the words are in upper-case letters, separated by underscores.
   * See {@link UpperSnakeCaseStrategy} for details.
   *
   * @since 2.13
   * <p>
   */
  public static final PropertyNamingStrategy UPPER_SNAKE_CASE = UpperSnakeCaseStrategy.INSTANCE;

  /**
   * Naming convention in which all words of the logical name are in lower case, and
   * no separator is used between words.
   * See {@link LowerCaseStrategy} for details.
   * <p>
   * Example external property names would be "numbervalue", "namingstrategy", "thedefiniteproof".
   */
  public static final PropertyNamingStrategy LOWER_CASE = LowerCaseStrategy.INSTANCE;

  /**
   * Naming convention used in languages like Lisp, where words are in lower-case
   * letters, separated by hyphens.
   * See {@link KebabCaseStrategy} for details.
   * <p>
   * Example external property names would be "number-value", "naming-strategy", "the-definite-proof".
   */
  public static final PropertyNamingStrategy KEBAB_CASE = KebabCaseStrategy.INSTANCE;

  /**
   * Naming convention widely used as configuration properties name, where words are in
   * lower-case letters, separated by dots.
   * See {@link LowerDotCaseStrategy} for details.
   * <p>
   * Example external property names would be "number.value", "naming.strategy", "the.definite.proof".
   */
  public static final PropertyNamingStrategy LOWER_DOT_CASE = LowerDotCaseStrategy.INSTANCE;

    /*
    /**********************************************************************
    /* Public base class for simple implementations
    /**********************************************************************
     */

  /**
   * Intermediate base class for simple implementations
   */
  public abstract static class NamingBase extends PropertyNamingStrategy
  {
    private static final long serialVersionUID = 2L;

    @Override
    public String nameForField(MapperConfig<?> config, AnnotatedField field, String defaultName)
    {
      return translate(defaultName);
    }

    @Override
    public String nameForGetterMethod(MapperConfig<?> config, AnnotatedMethod method, String defaultName)
    {
      return translate(defaultName);
    }

    @Override
    public String nameForSetterMethod(MapperConfig<?> config, AnnotatedMethod method, String defaultName)
    {
      return translate(defaultName);
    }

    @Override
    public String nameForConstructorParameter(MapperConfig<?> config, AnnotatedParameter ctorParam, String defaultName)
    {
      return translate(defaultName);
    }

    public abstract String translate(String propertyName);

    /**
     * Helper method to share implementation between snake and dotted case.
     */
    protected String translateLowerCaseWithSeparator(final String input, final char separator)
    {
      if (input == null || input.isEmpty()) {
        return input;
      }

      final int length = input.length();
      final StringBuilder result = new StringBuilder(length + (length >> 1));
      int upperCount = 0;
      for (int i = 0; i < length; ++i) {
        char ch = input.charAt(i);
        char lc = Character.toLowerCase(ch);

        if (lc == ch) { // lower-case letter means we can get new word
          // but need to check for multi-letter upper-case (acronym), where assumption
          // is that the last upper-case char is start of a new word
          if (upperCount > 1) {
            // so insert hyphen before the last character now
            result.insert(result.length() - 1, separator);
          }
          upperCount = 0;
        } else {
          // Otherwise starts new word, unless beginning of string
          if ((upperCount == 0) && (i > 0)) {
            result.append(separator);
          }
          ++upperCount;
        }
        result.append(lc);
      }
      return result.toString();
    }
  }

    /*
    /**********************************************************************
    /* Standard implementations
    /**********************************************************************
     */

  /**
   * A {@link PropertyNamingStrategy} that translates typical camel case Java
   * property names to lower case JSON element names, separated by
   * underscores.  This implementation is somewhat lenient, in that it
   * provides some additional translations beyond strictly translating from
   * camel case only.  In particular, the following translations are applied
   * by this PropertyNamingStrategy.
   *
   * <ul><li>Every upper case letter in the Java property name is translated
   * into two characters, an underscore and the lower case equivalent of the
   * target character, with three exceptions.
   * <ol><li>For contiguous sequences of upper case letters, characters after
   * the first character are replaced only by their lower case equivalent,
   * and are not preceded by an underscore.
   * <ul><li>This provides for reasonable translations of upper case acronyms,
   * e.g., &quot;theWWW&quot; is translated to &quot;the_www&quot;.</li></ul></li>
   * <li>An upper case character in the first position of the Java property
   * name is not preceded by an underscore character, and is translated only
   * to its lower case equivalent.
   * <ul><li>For example, &quot;Results&quot; is translated to &quot;results&quot;,
   * and not to &quot;_results&quot;.</li></ul></li>
   * <li>An upper case character in the Java property name that is already
   * preceded by an underscore character is translated only to its lower case
   * equivalent, and is not preceded by an additional underscore.
   * <ul><li>For example, &quot;user_Name&quot; is translated to
   * &quot;user_name&quot;, and not to &quot;user__name&quot; (with two
   * underscore characters).</li></ul></li></ol></li>
   * <li>If the Java property name starts with an underscore, then that
   * underscore is not included in the translated name, unless the Java
   * property name is just one character in length, i.e., it is the
   * underscore character.  This applies only to the first character of the
   * Java property name.</li></ul>
   * <p>
   * These rules result in the following additional example translations from
   * Java property names to JSON element names.
   * <ul><li>&quot;userName&quot; is translated to &quot;user_name&quot;</li>
   * <li>&quot;UserName&quot; is translated to &quot;user_name&quot;</li>
   * <li>&quot;USER_NAME&quot; is translated to &quot;user_name&quot;</li>
   * <li>&quot;user_name&quot; is translated to &quot;user_name&quot; (unchanged)</li>
   * <li>&quot;user&quot; is translated to &quot;user&quot; (unchanged)</li>
   * <li>&quot;User&quot; is translated to &quot;user&quot;</li>
   * <li>&quot;USER&quot; is translated to &quot;user&quot;</li>
   * <li>&quot;_user&quot; is translated to &quot;user&quot;</li>
   * <li>&quot;_User&quot; is translated to &quot;user&quot;</li>
   * <li>&quot;__user&quot; is translated to &quot;_user&quot;
   * (the first of two underscores was removed)</li>
   * <li>&quot;user__name&quot; is translated to &quot;user__name&quot;
   * (unchanged, with two underscores)</li></ul>
   */
  public static class SnakeCaseStrategy extends NamingBase
  {
    private static final long serialVersionUID = 2L;

    /**
     * @since 2.14
     */
    public static final PropertyNamingStrategies.SnakeCaseStrategy INSTANCE = new PropertyNamingStrategies.SnakeCaseStrategy();

    @Override
    public String translate(String input)
    {
      if (input == null) {
        return input; // garbage in, garbage out
      }
      int length = input.length();
      StringBuilder result = new StringBuilder(length * 2);
      int resultLength = 0;
      boolean wasPrevTranslated = false;
      for (int i = 0; i < length; i++) {
        char c = input.charAt(i);
        if (i > 0 || c != '_') { // skip first starting underscore
          if (Character.isUpperCase(c)) {
            if (!wasPrevTranslated && resultLength > 0 && result.charAt(resultLength - 1) != '_') {
              result.append('_');
              resultLength++;
            }
            c = Character.toLowerCase(c);
            wasPrevTranslated = true;
          } else {
            wasPrevTranslated = false;
          }
          result.append(c);
          resultLength++;
        }
      }
      return resultLength > 0 ? result.toString() : input;
    }
  }

  /**
   * A {@link PropertyNamingStrategy} that translates an input to the equivalent upper case snake
   * case. The class extends {@link PropertyNamingStrategies.SnakeCaseStrategy} to retain the
   * snake case conversion functionality offered by the strategy.
   *
   * @since 2.13
   */
  public static class UpperSnakeCaseStrategy extends SnakeCaseStrategy
  {
    private static final long serialVersionUID = 2L;

    /**
     * @since 2.14
     */
    public static final PropertyNamingStrategies.UpperSnakeCaseStrategy INSTANCE = new PropertyNamingStrategies.UpperSnakeCaseStrategy();

    @Override
    public String translate(String input)
    {
      String output = super.translate(input);
      if (output == null) {
        return null;
      }
      return output.toUpperCase(Locale.ENGLISH);
    }
  }

  /**
   * "No-operation" strategy that is equivalent to not specifying any
   * strategy: will simply return suggested standard bean naming as-is.
   */
  public static class LowerCamelCaseStrategy extends NamingBase
  {
    private static final long serialVersionUID = 2L;

    /**
     * @since 2.14
     */
    public static final PropertyNamingStrategies.LowerCamelCaseStrategy INSTANCE = new PropertyNamingStrategies.LowerCamelCaseStrategy();

    @Override
    public String translate(String input)
    {
      return input;
    }
  }

  /**
   * A {@link PropertyNamingStrategy} that translates typical camelCase Java
   * property names to PascalCase JSON element names (i.e., with a capital
   * first letter).  In particular, the following translations are applied by
   * this PropertyNamingStrategy.
   *
   * <ul><li>The first lower-case letter in the Java property name is translated
   * into its equivalent upper-case representation.</li></ul>
   * <p>
   * This rules result in the following example translation from
   * Java property names to JSON element names.
   * <ul><li>&quot;userName&quot; is translated to &quot;UserName&quot;</li></ul>
   */
  public static class UpperCamelCaseStrategy extends NamingBase
  {
    private static final long serialVersionUID = 2L;

    /**
     * @since 2.14
     */
    public static final PropertyNamingStrategies.UpperCamelCaseStrategy INSTANCE = new PropertyNamingStrategies.UpperCamelCaseStrategy();

    /**
     * Converts camelCase to PascalCase
     * <p>
     * For example, "userName" would be converted to
     * "UserName".
     *
     * @param input formatted as camelCase string
     * @return input converted to PascalCase format
     */
    @Override
    public String translate(String input)
    {
      if (input == null || input.isEmpty()) {
        return input; // garbage in, garbage out
      }
      // Replace first lower-case letter with upper-case equivalent
      char c = input.charAt(0);
      char uc = Character.toUpperCase(c);
      if (c == uc) {
        return input;
      }
      StringBuilder sb = new StringBuilder(input);
      sb.setCharAt(0, uc);
      return sb.toString();
    }
  }

  /**
   * Simple strategy where external name simply only uses lower-case characters,
   * and no separators.
   * Conversion from internal name like "someOtherValue" would be into external name
   * if "someothervalue".
   */
  public static class LowerCaseStrategy extends NamingBase
  {
    private static final long serialVersionUID = 2L;

    /**
     * @since 2.14
     */
    public static final PropertyNamingStrategies.LowerCaseStrategy INSTANCE = new PropertyNamingStrategies.LowerCaseStrategy();

    @Override
    public String translate(String input)
    {
      if (input == null || input.isEmpty()) {
        return input;
      }
      return input.toLowerCase(Locale.ENGLISH);
    }
  }

  /**
   * Naming strategy similar to {@link PropertyNamingStrategies.SnakeCaseStrategy},
   * but instead of underscores
   * as separators, uses hyphens. Naming convention traditionally used for languages
   * like Lisp.
   */
  public static class KebabCaseStrategy extends NamingBase
  {
    private static final long serialVersionUID = 2L;

    /**
     * @since 2.14
     */
    public static final PropertyNamingStrategies.KebabCaseStrategy INSTANCE = new PropertyNamingStrategies.KebabCaseStrategy();

    @Override
    public String translate(String input)
    {
      return translateLowerCaseWithSeparator(input, '-');
    }
  }

  /**
   * Naming strategy similar to {@link PropertyNamingStrategies.KebabCaseStrategy},
   * but instead of hyphens
   * as separators, uses dots. Naming convention widely used as configuration properties name.
   */
  public static class LowerDotCaseStrategy extends NamingBase
  {
    private static final long serialVersionUID = 2L;

    /**
     * @since 2.14
     */
    public static final PropertyNamingStrategies.LowerDotCaseStrategy INSTANCE = new PropertyNamingStrategies.LowerDotCaseStrategy();

    @Override
    public String translate(String input)
    {
      return translateLowerCaseWithSeparator(input, '.');
    }
  }
}
