## About This Project

This project is to make a TypeScript boilerplate for lambda development and prepare for the coming client work.

## Usage

1. Download files and install with `yarn install` command.
1. Edit `index.ts` file in `src` directory. 
1. Build with `yarn build` command.
1. Deploy generated files in `dist` directory.

## Components

This boilerplate uses these libraries:

- [webpack](https://webpack.js.org/)
  - webpack extracts the dependencies of main code from node modules to deploy your code to lambda instantly and compactly.
- [ESLint](https://eslint.org/)
  - ESLint analyzes the code you write and checks the syntax and style errors.
    - In this project, we use [StandardJS](https://standardjs.com/) style.
      - You can fix most style problems with `eslint --fix` command.
- [Babel](https://babeljs.io/)
  - Babel compiles TypeScript code to javascript code.

## About TypeScript Settings

I have set the options of typescript compiler to strict settings, to reduce ambiguity and mistakes from your code.
