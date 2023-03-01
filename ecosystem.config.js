module.exports = {
    apps : [{
      name   : "checkbook-ingest",
      script : "node_modules/ts-node/dist/index.js ingest.ts",
    }]
  }