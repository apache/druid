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

import { Button, ButtonGroup, Checkbox, InputGroup } from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import {
  closeSearchPanel,
  findNext,
  findPrevious,
  getSearchQuery,
  replaceAll,
  replaceNext,
  SearchQuery,
  selectMatches,
  setSearchQuery,
} from '@codemirror/search';
import type { EditorView, Panel, ViewUpdate } from '@codemirror/view';
import { runScopeHandlers } from '@codemirror/view';
import type React from 'react';
import { flushSync } from 'react-dom';
import type { Root } from 'react-dom/client';
import { createRoot } from 'react-dom/client';

interface SearchPanelProps {
  view: EditorView;
  query: SearchQuery;
  readOnly: boolean;
}

/**
 * The find (and replace) panel, it does what CodeMirror's own panel does but is made out of Blueprint components
 */
function SearchPanel(props: SearchPanelProps) {
  const { view, query, readOnly } = props;

  function changeQuery(
    change: Partial<
      Pick<SearchQuery, 'search' | 'caseSensitive' | 'regexp' | 'wholeWord' | 'replace'>
    >,
  ) {
    const { search, caseSensitive, literal, regexp, wholeWord, replace } = query;
    const newQuery = new SearchQuery({
      search,
      caseSensitive,
      literal,
      regexp,
      wholeWord,
      replace,
      ...change,
    });
    if (newQuery.eq(query)) return;
    view.dispatch({ effects: setSearchQuery.of(newQuery) });
  }

  function handleKeyDown(e: React.KeyboardEvent<HTMLElement>) {
    // The search keymap: Escape closes the panel, Mod-f selects the search field, Mod-g finds the next match, etc.
    if (runScopeHandlers(view, e.nativeEvent, 'search-panel')) {
      e.preventDefault();
    } else if (e.key === 'Enter' && e.target instanceof HTMLInputElement) {
      e.preventDefault();
      if (e.target.name === 'search') {
        (e.shiftKey ? findPrevious : findNext)(view);
      } else if (e.target.name === 'replace') {
        replaceNext(view);
      }
    }
  }

  return (
    <div className="code-editor-search-panel" onKeyDown={handleKeyDown}>
      <div className="search-row">
        <InputGroup
          className="search-field"
          size="small"
          name="search"
          placeholder="Find"
          aria-label="Find"
          leftIcon={IconNames.SEARCH}
          value={query.search}
          onChange={e => changeQuery({ search: e.target.value })}
          // openSearchPanel focuses this field when the panel is already open
          {...{ 'main-field': 'true' }}
        />
        <ButtonGroup size="small">
          <Button
            icon={IconNames.ARROW_UP}
            title="Previous match (Shift+Enter)"
            disabled={!query.valid}
            onClick={() => findPrevious(view)}
          />
          <Button
            icon={IconNames.ARROW_DOWN}
            title="Next match (Enter)"
            disabled={!query.valid}
            onClick={() => findNext(view)}
          />
          <Button text="Select all" disabled={!query.valid} onClick={() => selectMatches(view)} />
        </ButtonGroup>
        <Checkbox
          inline
          label="Match case"
          checked={query.caseSensitive}
          onChange={e => changeQuery({ caseSensitive: e.currentTarget.checked })}
        />
        <Checkbox
          inline
          label="Regexp"
          checked={query.regexp}
          onChange={e => changeQuery({ regexp: e.currentTarget.checked })}
        />
        <Checkbox
          inline
          label="By word"
          checked={query.wholeWord}
          onChange={e => changeQuery({ wholeWord: e.currentTarget.checked })}
        />
        <Button
          className="close-button"
          icon={IconNames.CROSS}
          size="small"
          variant="minimal"
          title="Close (Escape)"
          aria-label="Close"
          onClick={() => closeSearchPanel(view)}
        />
      </div>
      {!readOnly && (
        <div className="replace-row">
          <InputGroup
            className="search-field"
            size="small"
            name="replace"
            placeholder="Replace"
            aria-label="Replace"
            leftIcon={IconNames.EXCHANGE}
            value={query.replace}
            onChange={e => changeQuery({ replace: e.target.value })}
          />
          <ButtonGroup size="small">
            <Button text="Replace" disabled={!query.valid} onClick={() => replaceNext(view)} />
            <Button text="Replace all" disabled={!query.valid} onClick={() => replaceAll(view)} />
          </ButtonGroup>
        </div>
      )}
    </div>
  );
}

/**
 * For search({ createPanel }): renders SearchPanel into the panel's element and keeps it in sync with the search query
 */
export function createSearchPanel(view: EditorView): Panel {
  const dom = document.createElement('div');
  const root: Root = createRoot(dom);

  function render(): void {
    const { state } = view;
    root.render(
      <SearchPanel view={view} query={getSearchQuery(state)} readOnly={state.readOnly} />,
    );
  }

  // Render right away so that the search field is there to be focused when the panel is mounted
  flushSync(render);

  return {
    dom,
    mount() {
      const searchField = dom.querySelector<HTMLInputElement>('[main-field]');
      searchField?.focus();
      searchField?.select();
    },
    update(update: ViewUpdate) {
      if (
        getSearchQuery(update.state) !== getSearchQuery(update.startState) ||
        update.state.readOnly !== update.startState.readOnly
      ) {
        render();
      }
    },
    destroy() {
      // The panel can be destroyed while React is rendering (when the editor unmounts)
      setTimeout(() => root.unmount(), 0);
    },
  };
}
