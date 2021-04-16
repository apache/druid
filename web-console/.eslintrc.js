module.exports = {
  env: {
    browser: true,
  },
  extends: [
    'eslint:recommended',
    'plugin:@typescript-eslint/recommended',
    'plugin:@typescript-eslint/recommended-requiring-type-checking',
    'plugin:react/recommended',
    'prettier',
  ],
  parser: '@typescript-eslint/parser',
  parserOptions: {
    project: 'tsconfig.json',
    sourceType: 'module',
  },
  plugins: [
    'eslint-plugin-header',
    'eslint-plugin-jsdoc',
    'eslint-plugin-react',
    'eslint-plugin-unicorn',
    'eslint-plugin-import',
    '@typescript-eslint',
  ],
  settings: {
    react: {
      version: 'detect',
    },
  },
  rules: {
    // eslint (added to 'recommended' rules)
    'eqeqeq': ['error', 'smart'],
    'max-classes-per-file': ['error', 1],
    'no-bitwise': 'error',
    'no-caller': 'error',
    'no-eval': 'error',
    'no-multiple-empty-lines': [
      'error',
      {
        max: 2,
      },
    ],
    'no-new-wrappers': 'error',
    'no-throw-literal': 'error',
    'no-undef-init': 'error',
    'one-var': ['error', 'never'],
    'quote-props': 'off',
    'radix': 'error',
    'sort-imports': [
      'warn',
      {
        ignoreCase: true,
        ignoreDeclarationSort: true, // covered by import/order
      },
    ],
    'spaced-comment': [
      'error',
      'always',
      {
        markers: ['/'],
      },
    ],

    // eslint (overrides 'recommended' rules)
    'no-empty': 'off',
    'no-fallthrough': 'off',
    'prefer-const': [
      'error',
      {
        destructuring: 'all',
      },
    ],

    // @typescript-eslint (added to 'recommended' rules)
    '@typescript-eslint/consistent-type-assertions': 'error',
    '@typescript-eslint/lines-between-class-members': [
      'warn',
      'always',
      { exceptAfterSingleLine: true },
    ],
    '@typescript-eslint/member-ordering': 'off', // TODO: use?
    '@typescript-eslint/naming-convention': 'off', // TODO: use?
    '@typescript-eslint/no-shadow': ['off', { hoist: 'all' }], // TODO: use?
    '@typescript-eslint/no-unused-expressions': 'error',
    '@typescript-eslint/prefer-for-of': 'off', // TODO: use?
    '@typescript-eslint/unified-signatures': 'error',

    // @typescript-eslint (overriding 'recommended' rules)
    '@typescript-eslint/explicit-module-boundary-types': 'off',
    '@typescript-eslint/no-empty-function': 'off',
    '@typescript-eslint/no-empty-interface': 'off',
    '@typescript-eslint/no-explicit-any': 'off',
    '@typescript-eslint/no-non-null-assertion': 'off',
    '@typescript-eslint/no-unused-vars': 'off',
    '@typescript-eslint/prefer-function-type': 'error',

    // @typescript-eslint with type checking (overriding 'recommended' rules)
    '@typescript-eslint/no-unsafe-assignment': 'off', // TODO: use?
    '@typescript-eslint/no-unsafe-call': 'off', // TODO: use?
    '@typescript-eslint/no-unsafe-member-access': 'off', // TODO: use?
    '@typescript-eslint/no-unsafe-return': 'off', // TODO: use?
    '@typescript-eslint/restrict-plus-operands': 'off',
    '@typescript-eslint/restrict-template-expressions': 'off',
    '@typescript-eslint/unbound-method': 'off',

    // eslint-plugin-header
    'header/header': [
      2,
      'block',
      { pattern: 'Licensed to the Apache Software Foundation \\(ASF\\).+' },
    ],

    // eslint-plugin-import
    'import/order': ['warn', { alphabetize: { order: 'asc', caseInsensitive: true } }],

    // eslint-plugin-jsdoc
    'jsdoc/check-alignment': 'warn',
    'jsdoc/check-indentation': 'off',
    'jsdoc/newline-after-description': 'off',

    // eslint-plugin-react (added to 'recommended' rules)
    'react/jsx-boolean-value': ['error', 'never'],
    'react/jsx-curly-brace-presence': ['warn', 'never'],
    'react/jsx-curly-spacing': [
      'error',
      {
        when: 'never',
      },
    ],
    'react/jsx-no-bind': ['error', { allowArrowFunctions: true }],
    'react/self-closing-comp': 'error',

    // eslint-plugin-react (overriding 'recommended' rules)
    'react/prop-types': 'off',

    // eslint-plugin-unicorn
    'unicorn/filename-case': 'error',
  },
};
