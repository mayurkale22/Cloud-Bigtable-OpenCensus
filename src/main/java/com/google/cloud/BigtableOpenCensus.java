package com.google.cloud;

import com.google.cloud.bigtable.hbase.BigtableConfiguration;
import com.google.cloud.bigtable.metrics.BigtableClientMetrics;
import com.google.cloud.bigtable.metrics.DropwizardMetricRegistry;
import io.opencensus.common.Scope;
import io.opencensus.contrib.dropwizard.DropWizardMetrics;
import io.opencensus.contrib.grpc.metrics.RpcViews;
import io.opencensus.exporter.stats.stackdriver.StackdriverStatsExporter;
import io.opencensus.exporter.trace.stackdriver.StackdriverTraceConfiguration;
import io.opencensus.exporter.trace.stackdriver.StackdriverTraceExporter;
import io.opencensus.metrics.Metrics;
import io.opencensus.stats.Aggregation;
import io.opencensus.stats.Aggregation.Distribution;
import io.opencensus.stats.BucketBoundaries;
import io.opencensus.stats.Measure.MeasureDouble;
import io.opencensus.stats.Stats;
import io.opencensus.stats.StatsRecorder;
import io.opencensus.stats.View;
import io.opencensus.stats.View.Name;
import io.opencensus.stats.ViewManager;
import io.opencensus.tags.TagContext;
import io.opencensus.tags.TagKey;
import io.opencensus.tags.TagValue;
import io.opencensus.tags.Tagger;
import io.opencensus.tags.Tags;
import io.opencensus.trace.Span;
import io.opencensus.trace.Tracer;
import io.opencensus.trace.Tracing;
import io.opencensus.trace.config.TraceConfig;
import io.opencensus.trace.samplers.Samplers;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class BigtableOpenCensus implements AutoCloseable {
  private static final String PROJECT_ID = "opencensus-java-stats-demo-app";
  private static final String INSTANCE_ID = "oc-metrics";
  private static final byte[] TABLE_NAME = Bytes.toBytes("Hello-Bigtabl35");
  private static final byte[] COLUMN_FAMILY_NAME = Bytes.toBytes("cf1");
  private static final byte[] COLUMN_NAME = Bytes.toBytes("greeting");
  private static final String[] GREETINGS = {
    "Hello World!", "Hello Cloud Bigtable!", "Hello HBase!"
  };
  private static final Tracer tracer = Tracing.getTracer();

  // Create the view manager
  private static final ViewManager vmgr = Stats.getViewManager();
  private static final StatsRecorder statsRecorder = Stats.getStatsRecorder();

  private static final Tagger tagger = Tags.getTagger();

  // The latency in milliseconds
  private static final MeasureDouble M_LATENCY_MS = MeasureDouble.create(
    "bigtable/latency", "The latency in milliseconds", "ms");

  private static final TagKey KEY_TABLE_NAME = TagKey.create("table");
  private static final TagKey KEY_METHOD = TagKey.create("method");

  private static final TagValue VALUE_TABLE_NAME = TagValue.create(new String(TABLE_NAME));
  private static final TagValue VALUE_WRITE = TagValue.create("write");
  private static final TagValue VALUE_READ = TagValue.create("read");

  private Admin admin;
  private Connection connection;

  public BigtableOpenCensus(String projectId, String instanceId) throws Exception {
    // Create the admin client to use for table creation and management
    this.connection = BigtableConfiguration.connect(projectId, instanceId);
    this.admin = this.connection.getAdmin();

    // Enable observability with OpenCensus
    this.enableOpenCensusObservability();
  }

  @Override
  public void close() throws IOException {
    this.connection.close();
  }

  public static void main(String... args) {
    // Create the Bigtable connection
    try (BigtableOpenCensus boc = new BigtableOpenCensus(PROJECT_ID, INSTANCE_ID)) {
      // Now for the application code

      HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(TABLE_NAME));

      // Create the table
      Table table = boc.createTable(descriptor);

      for(int i = 0; i < 100; i++) {
        try (Scope ss = tracer.spanBuilder("opencensus.Bigtable.Tutorial").startScopedSpan()) {
          // Write some data to the table
          boc.writeRows(table, GREETINGS[i % 3]);
          // Read the written rows
          boc.readRows(table);
        }
      }

      // Finally cleanup by deleting the created table
      boc.deleteTable(table);
      Thread.sleep(60000);
    } catch (Exception e) {
      System.err.println("Exception while running BigtableDropwizard: " + e.getMessage());
      e.printStackTrace();
      System.exit(1);
    }
  }

  private Table createTable(HTableDescriptor tableDesc) throws Exception {
    try (Scope ss = tracer.spanBuilder("CreateTable").startScopedSpan()) {
      tableDesc.addFamily(new HColumnDescriptor(COLUMN_FAMILY_NAME));
      System.out.println("Create table " + tableDesc.getNameAsString());
      this.admin.createTable(tableDesc);
      return this.connection.getTable(TableName.valueOf(TABLE_NAME));
    }
  }

  private void writeRows(Table table, String row) throws IOException, InterruptedException {
    try (Scope ss = tracer.spanBuilder("WriteRows").startScopedSpan()) {
      long startTimeNs = System.nanoTime();

      System.out.println("Write rows to the table");
      String rowKey = "greeting" + row;
      Put put = new Put(Bytes.toBytes(rowKey));
      put.addColumn(COLUMN_FAMILY_NAME, COLUMN_NAME, Bytes.toBytes(row));

      Span span = tracer.getCurrentSpan();
      randomSleep();
      span.addAnnotation("Preprocessing read operation...");
      randomSleep();
      table.put(put);

      TagContext tags = tagger.emptyBuilder().put(KEY_TABLE_NAME, VALUE_TABLE_NAME).
        put(KEY_METHOD, VALUE_WRITE).build();
      statsRecorder.newMeasureMap().put(M_LATENCY_MS, sinceInMilliseconds(startTimeNs)).record(tags);
    }
  }

  private void deleteTable(Table table) throws Exception {
    try (Scope ss = tracer.spanBuilder("DeleteTable").startScopedSpan()) {
      System.out.println("Deleting the table");
      this.admin.disableTable(table.getName());
      this.admin.deleteTable(table.getName());
    }
  }

  private void readRows(Table table) throws Exception {
    try (Scope ss = tracer.spanBuilder("ReadRows").startScopedSpan()) {
      long startTimeNs = System.nanoTime();
      System.out.println("Scan for all greetings:");
      Scan scan = new Scan();

      randomSleep();
      ResultScanner scanner = table.getScanner(scan);
      for (Result row : scanner) {
        byte[] valueBytes = row.getValue(COLUMN_FAMILY_NAME, COLUMN_NAME);
        System.out.println('\t' + Bytes.toString(valueBytes));
      }

      TagContext tags = tagger.emptyBuilder().put(KEY_TABLE_NAME, VALUE_TABLE_NAME).
        put(KEY_METHOD, VALUE_READ).build();
      statsRecorder.newMeasureMap().put(M_LATENCY_MS, sinceInMilliseconds(startTimeNs)).record(tags);
    }
  }

  private void enableOpenCensusObservability() throws IOException {

    // Register the views
    registerAllViews();

    // Start: enable observability with OpenCensus tracing and metrics
    // Create and register the Stackdriver Tracing exporter
    StackdriverTraceExporter.createAndRegister(
      StackdriverTraceConfiguration.builder().setProjectId(PROJECT_ID).build());

    StackdriverStatsExporter.createAndRegister();

    // Register all the gRPC views
    RpcViews.registerAllGrpcViews();

    // For demo purposes, we are enabling the always sampler
    TraceConfig traceConfig = Tracing.getTraceConfig();
    traceConfig.updateActiveTraceParams(
      traceConfig.getActiveTraceParams().toBuilder().setSampler(Samplers.alwaysSample()).build());
    // End: enable observability with OpenCensus

    // Export DropWizard Metrics to OpenCensus data model
    DropwizardMetricRegistry registry = new DropwizardMetricRegistry();
    BigtableClientMetrics.setMetricRegistry(registry);

    Metrics.getExportComponent().getMetricProducerManager().add(
      new DropWizardMetrics(Collections.singletonList(registry.getRegistry())));
  }

  private static void registerAllViews() {
    // Defining the distribution aggregations
    Aggregation latencyDistribution = Distribution.create(BucketBoundaries.create(
      Arrays.asList(
        // [>=0ms, >=25ms, >=50ms, >=75ms, >=100ms, >=200ms, >=400ms, >=600ms, >=800ms, >=1s, >=2s, >=4s, >=6s]
        0.0, 25.0, 50.0, 75.0, 100.0, 200.0, 400.0, 600.0, 800.0, 1000.0, 2000.0, 4000.0, 6000.0)
    ));

    // Define the count aggregation
    Aggregation countAggregation = Aggregation.Count.create();

    // So tagKeys
    List tagKeys = Arrays.asList(KEY_TABLE_NAME, KEY_METHOD);

    // Define the views
    View latenciesView = View.create(
      Name.create("bigtable/table/latency"),
      "The distribution of latencies",
      M_LATENCY_MS,
      latencyDistribution,
      tagKeys);


    // Define the views
    View operationsView = View.create(
      Name.create("bigtable/table/operations"),
      "The number of operations",
      M_LATENCY_MS,
      countAggregation,
      tagKeys);

    // Then finally register the views
    vmgr.registerView(latenciesView);
    vmgr.registerView(operationsView);
  }

  private void randomSleep() throws InterruptedException {
    Thread.sleep(1000 * new Random().nextInt(15));
  }

  private static double sinceInMilliseconds(long startTimeNs) {
    return (new Double(System.nanoTime() - startTimeNs))/1e6;
  }
}

