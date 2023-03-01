module.exports = {
    apps : [{
      name   : "checkbook-ingest",
      script : "ts-node onceeveryhour.ts",
    }]
  }