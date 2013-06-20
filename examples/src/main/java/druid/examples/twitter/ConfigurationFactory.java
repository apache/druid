package druid.examples.twitter;

/*
 * Copyright 2007 Yusuke Yamamoto
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * @author Yusuke Yamamoto - yusuke at mac.com
 */
public interface ConfigurationFactory {
  /**
   * returns the root configuration
   *
   * @return root configuration
   */
  Configuration getInstance();

  /**
   * returns the configuration specified by the path
   *
   * @param configTreePath the path
   * @return the configuratoin
   */
  Configuration getInstance(String configTreePath);

  /**
   * clean up resources acquired by this factory.
   */
  void dispose();
}
