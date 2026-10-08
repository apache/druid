/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { parseHashRoute } from './hash-routing';
import { TableFilters } from './table-filters';

describe('hash-routing', () => {
  describe('parseHashRoute', () => {
    it('works for the home page', () => {
      expect(parseHashRoute('')).toEqual({ view: '', param: undefined });
    });

    it('works for a view', () => {
      expect(parseHashRoute('datasources')).toEqual({ view: 'datasources', param: undefined });
      expect(parseHashRoute('Datasources/')).toEqual({ view: 'datasources', param: undefined });
    });

    it('works for a view with a param', () => {
      expect(parseHashRoute('workbench/tab1')).toEqual({ view: 'workbench', param: 'tab1' });
      expect(parseHashRoute('tasks/type=kill/more')).toEqual({ view: 'tasks', param: 'type=kill' });
    });

    it('keeps reserved characters encoded so table filters survive', () => {
      const filters = TableFilters.eq({ datasource: 'a&b/c%d' });
      const route = parseHashRoute(`datasources/${filters.toString()}`);
      expect(route.view).toEqual('datasources');
      expect(TableFilters.fromString(route.param).toString()).toEqual(filters.toString());
    });

    it('keeps table filters with every sort of character', () => {
      const filters = TableFilters.eq({
        datasource: '<>|!@#$%^&`\'".,:;\\*()[]{}Україна 한국 中国!?~',
      });
      // As the browser has it in the URL, which percent-encodes the non-ASCII and some ASCII characters
      const route = parseHashRoute(encodeURI(`datasources/${filters.toString()}`));
      expect(route.view).toEqual('datasources');
      expect(TableFilters.fromString(route.param).toArray()[0].values).toEqual(
        filters.toArray()[0].values,
      );
    });

    it('decodes non reserved characters', () => {
      expect(parseHashRoute('segments/datasource=wiki%20pedia')).toEqual({
        view: 'segments',
        param: 'datasource=wiki pedia',
      });
    });

    it('ignores the query string', () => {
      expect(parseHashRoute('explore?x=1')).toEqual({ view: 'explore', param: undefined });
    });
  });
});
