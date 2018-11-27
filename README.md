# Cloud-Bigtable-OpenCensus

Here is a sample chart created out of data exported by OpenCensus using Stackdriver exporter using Dropwizard tool.

https://github.com/census-instrumentation/opencensus-java/tree/master/contrib/dropwizard

![alt text](https://github.com/mayurkale22/Cloud-Bigtable-OpenCensus/blob/master/Exported-Metrics-Stackdriver.png)

## Enable OpenCensus Observability
```
private static void enableOpenCensusObservability() throws IOException {
    DropwizardMetricRegistry registry = new DropwizardMetricRegistry();
    
    BigtableClientMetrics.setMetricRegistry(registry);

    Metrics.getExportComponent().getMetricProducerManager().add(
      new DropWizardMetrics(Collections.singletonList(registry.getRegistry())));

    // Register the Stackdriver exporter
    StackdriverStatsExporter.createAndRegister();

  }
  ```
