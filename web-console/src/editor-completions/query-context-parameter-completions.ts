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

import { C, RefName } from 'druid-query-toolkit';

import type { QueryContextParameterMetadata } from '../druid-models';
import { DEFAULT_SERVER_QUERY_CONTEXT } from '../druid-models';
import { getLink } from '../links';

import type { EditorCompletion } from './editor-completion';

/**
 * The query context parameter names to suggest after SET: the parameters loaded from sys.query_context_parameters, or
 * the well-known context keys when they are not loaded
 */
export function getQueryContextCompletions(
  quote: boolean,
  queryContextParameters: readonly QueryContextParameterMetadata[] | undefined,
): EditorCompletion[] {
  if (!queryContextParameters?.length) {
    return Object.keys(DEFAULT_SERVER_QUERY_CONTEXT).map(key => ({
      label: quote ? key : contextKeyLabel(key),
      boost: 50,
      detail: 'context',
    }));
  }

  let completions = PARAMETER_COMPLETIONS.get(queryContextParameters);
  if (!completions) {
    completions = buildParameterCompletions(queryContextParameters);
    PARAMETER_COMPLETIONS.set(queryContextParameters, completions);
  }
  return quote ? completions.quoted : completions.unquoted;
}

interface ParameterCompletions {
  /** For typing after a double quote: the names as they are */
  quoted: EditorCompletion[];
  /** Otherwise: the names quoted where needed */
  unquoted: EditorCompletion[];
}

/**
 * The completions of each loaded list of parameters, built on first use: the docs only change when the parameters are
 * loaded again (a new list)
 */
const PARAMETER_COMPLETIONS = new WeakMap<
  readonly QueryContextParameterMetadata[],
  ParameterCompletions
>();

function buildParameterCompletions(
  queryContextParameters: readonly QueryContextParameterMetadata[],
): ParameterCompletions {
  const quoted = queryContextParameters.map(parameter => ({
    label: parameter.name,
    // Deprecated parameters sort after the others
    boost: parameter.deprecation ? 40 : 50,
    detail: 'context',
    doc: {
      name: parameter.name,
      descriptionMarkdown: queryContextParameterDocMarkdown(parameter),
    },
  }));
  return {
    quoted,
    unquoted: quoted.map(completion => ({
      ...completion,
      label: contextKeyLabel(completion.label),
    })),
  };
}

const HTML_ENTITIES: Record<string, string> = {
  amp: '&',
  lt: '<',
  gt: '>',
  quot: '"',
  apos: "'",
  nbsp: ' ',
};

function decodeHtmlEntities(text: string): string {
  return text.replace(/&(#\d+|[a-z]+);/gi, (entity, code: string) => {
    if (code.startsWith('#')) return String.fromCodePoint(Number(code.slice(1)));
    return HTML_ENTITIES[code.toLowerCase()] ?? entity;
  });
}

/**
 * The docs URL of a link in a parameter description. The descriptions come from the Druid docs, where links are
 * relative to the page they appear on. Every page that shows them is one directory below docs/, so a relative link
 * is resolved from querying/. A link to an anchor of the same page can not be resolved and gives undefined.
 */
export function resolveDocsLink(target: string): string | undefined {
  if (target.startsWith('https://')) return target;
  const [path, anchor] = target.split('#', 2);
  if (!path || /^[a-z]+:/i.test(path)) return;

  const segments = ['querying'];
  for (const segment of path.split('/')) {
    if (segment === '..') {
      segments.pop();
    } else if (segment && segment !== '.') {
      segments.push(segment);
    }
  }
  // Pages are served without .md, and index.md as its directory
  const page = segments.pop()!.replace(/\.md$/, '');
  if (page !== 'index') segments.push(page);
  const url = `${getLink('DOCS')}/${segments.join('/')}${page === 'index' ? '/' : ''}`;
  // The docs site makes anchors lowercase and hyphenated
  return anchor ? `${url}#${anchor.trim().toLowerCase().replace(/\s+/g, '-')}` : url;
}

/**
 * Converts text outside of code spans: links point to the docs site (or become their text when they can not be
 * resolved), <br /> becomes a line break, list items become "- "
 * lines, **strong** becomes *emphasis*, other HTML tags are dropped and HTML entities are decoded
 */
function convertHtmlText(text: string): string {
  return decodeHtmlEntities(
    stripHtmlTags(
      text
        .replace(/\[([^[\]]*)\]\(([^()]*)\)/g, (_, text: string, target: string) => {
          const href = resolveDocsLink(target);
          return href ? `[${text}](${href})` : text;
        })
        .replace(/<br\s*\/?>/gi, '\n')
        .replace(/<li>/gi, '\n- ')
        .replace(/<\/(ul|ol)>/gi, '\n'),
    ).replace(/\*\*([^*]+)\*\*/g, '*$1*'),
  );
}

/**
 * Drops HTML tags, repeatedly so that removing one tag can not leave another one behind (like "<<b>i>")
 */
function stripHtmlTags(text: string): string {
  let previous: string;
  let stripped = text;
  do {
    previous = stripped;
    stripped = stripped.replace(/<\/?[a-z][^>]*>/gi, '');
  } while (stripped !== previous);
  return stripped;
}

/**
 * Converts text outside of code spans, where <code> becomes a code span. Each part is decoded once: the content of a
 * <code> tag on its own, the text around it by convertHtmlText.
 */
function convertDescriptionText(text: string): string {
  return text
    .split(/<code>([^<]*)<\/code>/i)
    .map((part, i) => (i % 2 ? `\`${decodeHtmlEntities(part)}\`` : convertHtmlText(part)))
    .join('');
}

/**
 * Converts Markdown text without links: code spans are kept as they are, the text around them by
 * convertDescriptionText
 */
function convertMarkdownText(text: string): string {
  return text
    .split(/(`[^`]*`)/)
    .map((part, i) => (i % 2 ? part : convertDescriptionText(part)))
    .join('');
}

/**
 * Converts the Markdown of a query context parameter description, which comes from the Druid docs, into the "doc
 * markdown" that completion docs render. Code spans are kept as they are (angle brackets in them are text, not HTML).
 * A paragraph break (<br /><br />) stays as an empty line.
 */
export function queryContextDescriptionToDocMarkdown(description: string): string {
  let converted = '';
  let textStart = 0;
  // Links are converted before code spans are split out, since the text of a link can be a code span, like
  // [`indexSpec`](../ingestion/ingestion-spec.md). A code span that is not in a link is matched on its own so that
  // brackets in it are not taken for a link.
  for (const match of description.matchAll(/(`[^`]*`)|\[((?:`[^`]*`|[^[\]`])*)\]\(([^()]*)\)/g)) {
    if (match[1] !== undefined) continue;
    converted += convertMarkdownText(description.slice(textStart, match.index));
    const text = convertMarkdownText(match[2]);
    const href = resolveDocsLink(match[3]);
    converted += href ? `[${text}](${href})` : text;
    textStart = match.index + match[0].length;
  }
  converted += convertMarkdownText(description.slice(textStart));
  return converted
    .split('\n')
    .map(line => line.trim())
    .join('\n')
    .replace(/\n{3,}/g, '\n\n')
    .trim();
}

/**
 * The name of a query context parameter after SET: as is when it can be written unquoted (SET keys are case-sensitive
 * as typed, like `SET sqlTimeZone = 'UTC'`), quoted otherwise, such as for a reserved keyword
 */
function contextKeyLabel(key: string): string {
  return RefName.needsQuotes(key) ? String(C(key)) : key;
}

function docCode(value: string): string {
  return `\`${value.replace(/`/g, '')}\``;
}

const EFFECTIVE_SOURCE_LABELS: Record<string, string> = {
  DEFAULT_CONTEXT_PROPERTY: 'default query context',
  BROKER_DYNAMIC_CONFIG: 'Broker dynamic config',
};

/**
 * The runtime property of a default_value like `${druid.server.http.defaultQueryTimeout}`, if it is one
 */
export function defaultValueProperty(defaultValue: string | null): string | undefined {
  return defaultValue?.startsWith('${') && defaultValue.endsWith('}')
    ? defaultValue.slice(2, -1)
    : undefined;
}

/**
 * The declared default, like "built-in: `false`" or "runtime property `druid.server.http.defaultQueryTimeout`"
 */
function declaredDefault(defaultValue: string): string {
  const property = defaultValueProperty(defaultValue);
  return property ? `runtime property ${docCode(property)}` : `built-in: ${docCode(defaultValue)}`;
}

/**
 * Where the default comes from when it is not the built-in one, like " (Broker dynamic config; built-in: `false`)"
 */
function effectiveSourceNote(parameter: QueryContextParameterMetadata): string {
  if (parameter.effective_source === 'RUNTIME_PROPERTY' && parameter.default_value != null) {
    return ` (${declaredDefault(parameter.default_value)})`;
  }
  const label = parameter.effective_source
    ? EFFECTIVE_SOURCE_LABELS[parameter.effective_source]
    : undefined;
  if (!label) return '';
  return parameter.default_value != null
    ? ` (${label}; ${declaredDefault(parameter.default_value)})`
    : ` (${label})`;
}

/**
 * The doc of a query context parameter: its type, its default and then its description, separated by empty lines
 */
function queryContextParameterDocMarkdown(parameter: QueryContextParameterMetadata): string {
  const sections = [`Type: \`${parameter.type}\``];
  if (parameter.effective_value != null) {
    sections.push(
      `Default: ${docCode(parameter.effective_value)}${effectiveSourceNote(parameter)}`,
    );
  } else if (parameter.default_value != null) {
    // Without CONFIG read access the Broker's effective default is hidden, so the declared one may be overridden
    const property = defaultValueProperty(parameter.default_value);
    const mayOverride = parameter.effective_source === 'UNAUTHORIZED';
    sections.push(
      property
        ? `Default: value of runtime property ${docCode(property)}${
            mayOverride ? ' (the Broker may override it)' : ''
          }`
        : `Default: ${docCode(parameter.default_value)}${
            mayOverride ? ' (built-in; the Broker may override it)' : ''
          }`,
    );
  } else if (parameter.default_description) {
    sections.push(
      `Default: ${queryContextDescriptionToDocMarkdown(parameter.default_description)}`,
    );
  }
  sections.push(queryContextDescriptionToDocMarkdown(parameter.description));
  if (parameter.deprecation) {
    sections.push(`*Deprecated*: ${queryContextDescriptionToDocMarkdown(parameter.deprecation)}`);
  }
  return sections.join('\n\n');
}
