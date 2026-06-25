/* eslint-disable no-undef */
const CustomFunctionsMetadataPlugin = require("custom-functions-metadata-plugin");
const CopyWebpackPlugin = require("copy-webpack-plugin");
const HtmlWebpackPlugin = require("html-webpack-plugin");
const devCerts = require("office-addin-dev-certs");

module.exports = async (env, options) => {
  const dev = options.mode === "development";
  const httpsOptions = await devCerts.getHttpsServerOptions();

  return {
    devtool: "source-map",
    entry: {
      functions: "./src/functions/functions.ts",
    },
    resolve: {
      extensions: [".ts", ".js"],
    },
    module: {
      rules: [{ test: /\.ts$/, use: "ts-loader", exclude: /node_modules/ }],
    },
    plugins: [
      // Generates functions.json from the @customfunction JSDoc in functions.ts.
      new CustomFunctionsMetadataPlugin({
        output: "functions.json",
        input: "./src/functions/functions.ts",
      }),
      new HtmlWebpackPlugin({
        filename: "functions.html",
        template: "./src/functions/functions.html",
        chunks: ["functions"],
      }),
      new CopyWebpackPlugin({
        patterns: [
          { from: "src/taskpane.html", to: "taskpane.html" },
          { from: "manifest.xml", to: "manifest.xml" },
        ],
      }),
    ],
    output: {
      clean: true,
    },
    devServer: {
      static: { directory: "./dist" },
      server: { type: "https", options: httpsOptions },
      port: 3000,
    },
  };
};
