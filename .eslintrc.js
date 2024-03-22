module.exports = {
  env: {
    browser: true,
    es2021: true,
  },
  extends: "standard-with-typescript",
  parserOptions: {
    // Match the ECMAScript standard specified in our TypeScript config.
    ecmaVersion: 2021,
    // Use ECMAScript modules.
    sourceType: "module",
    tsconfigRootDir: __dirname,
  },
  rules: {},
};
