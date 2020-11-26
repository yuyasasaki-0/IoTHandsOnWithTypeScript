module.exports = {
  root: true,
  env: process.env.NODE_ENV === 'production'
    ? {
        node: true
      }
    : {
        node: true,
        jest: true
      },
  extends: [
    'standard'
  ],
  parserOptions: {
    ecmaVersion: 2020,
    sourceType: 'module'
  },
  plugins: [
  ],
  rules: {
    'no-console': process.env.NODE_ENV === 'production' ? 'error' : 'off',
    'no-debugger': process.env.NODE_ENV === 'production' ? 'error' : 'off',
    'prefer-template': 'warn'
  },
  overrides: [
    {
      files: ['**/*.ts', '**/*.tsx'],
      env: process.env.NODE_ENV === 'production'
        ? {
            node: true
          }
        : {
            node: true,
            jest: true
          },
      extends: [
        'standard-with-typescript',
        'plugin:@typescript-eslint/recommended',
        'plugin:@typescript-eslint/recommended-requiring-type-checking'
      ],
      parserOptions: {
        ecmaVersion: 2020,
        sourceType: 'module',
        project: './tsconfig.json'
      },
      plugins: [
        '@typescript-eslint'
      ],
      rules: {
        'no-console': process.env.NODE_ENV === 'production' ? 'error' : 'off',
        'no-debugger': process.env.NODE_ENV === 'production' ? 'error' : 'off',
        'prefer-template': 'warn'
      }
    }
  ]
}
