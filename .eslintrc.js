module.exports = {
    root: true,
    parser: "@typescript-eslint/parser",
    parserOptions: {
        project: "tsconfig.json",
    },
    ignorePatterns: [
        "*.hbs",
    ],
    plugins: [
        "@typescript-eslint",
        "eslint-plugin-tsdoc",
    ],
    extends: [
        "plugin:@typescript-eslint/recommended",
        // TODO: uncomment below
        // "plugin:@typescript-eslint/recommended-requiring-type-checking",
        "plugin:old-c-programmer/node",
    ],
    rules: {
        "tsdoc/syntax": "warn",
        "@typescript-eslint/no-floating-promises": "error",
        "@typescript-eslint/no-inferrable-types": "error",
        "@typescript-eslint/explicit-function-return-type": "error",
        "jsdoc/require-param-type": "off",
        "jsdoc/require-returns-type": "off",
        // "jsdoc/ignore-private": "on",
        // "jsdoc/check-properties": "on",
        "sort-imports": ["error", {
            ignoreCase: false,
            ignoreDeclarationSort: false,
            ignoreMemberSort: false,
            memberSyntaxSortOrder: ["none", "all", "multiple", "single"],
            allowSeparatedGroups: false,
        }],
        // fixes
        "no-unused-vars": "off",
        "@typescript-eslint/no-unused-vars": ["error", {argsIgnorePattern: "^_"}],
        "lines-between-class-members": "off",
        "@typescript-eslint/lines-between-class-members": ["error", "always", {exceptAfterSingleLine: true}],
        "@typescript-eslint/no-require-imports": "error",
        "no-dupe-class-members": "off",
        "@typescript-eslint/no-dupe-class-members": ["error"],
        "prefer-spread": ["off"],
        "no-redeclare": "off",
        "@typescript-eslint/no-empty-interface": ["error", {allowSingleExtends: true}],
        // TODO: turn these off
        "@typescript-eslint/no-var-requires": "off",
        "@typescript-eslint/no-unsafe-assignment": "off",
        "@typescript-eslint/no-unsafe-return": "off",
        "@typescript-eslint/unbound-method": "off",
        "@typescript-eslint/explicit-module-boundary-types": "off",
        "@typescript-eslint/no-non-null-assertion": "off",
    },
};
