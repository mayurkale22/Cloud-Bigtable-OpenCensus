package com.google.cloud;

import com.google.cloud.bigtable.hbase.BigtableConfiguration;
import io.opencensus.common.Scope;
import io.opencensus.contrib.grpc.metrics.RpcViews;
import io.opencensus.exporter.stats.stackdriver.StackdriverStatsConfiguration;
import io.opencensus.exporter.stats.stackdriver.StackdriverStatsExporter;
import io.opencensus.exporter.trace.stackdriver.StackdriverTraceConfiguration;
import io.opencensus.exporter.trace.stackdriver.StackdriverTraceExporter;
import io.opencensus.trace.Tracer;
import io.opencensus.trace.Tracing;
import io.opencensus.trace.config.TraceConfig;
import io.opencensus.trace.samplers.Samplers;
import java.io.IOException;
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
  private static final String PROJECT_ID = "xxxx";
  private static final String INSTANCE_ID = "yyyy";
  private static final byte[] TABLE_NAME = Bytes.toBytes("Hello-Bigtable2");
  private static final byte[] COLUMN_FAMILY_NAME = Bytes.toBytes("cf1");
  private static final byte[] COLUMN_NAME = Bytes.toBytes("greeting");
  private static final String[] GREETINGS = {
    "Hello World!", "Hello Cloud Bigtable!", "Hello HBase!"
  };
  private static final Tracer tracer = Tracing.getTracer();

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
      try (Scope ss = tracer.spanBuilder("opencensus.Bigtable.Tutorial").startScopedSpan()) {
        HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(TABLE_NAME));

        // Create the table
        Table table = boc.createTable(descriptor);

        // Write some data to the table
        boc.writeRows(table, GREETINGS);

        // Read the written rows
        boc.readRows(table);

        // Finally cleanup by deleting the created table
        boc.deleteTable(table);

        Thread.sleep(65000);
      }
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

  private void writeRows(Table table, String[] rows) throws IOException {
    try (Scope ss = tracer.spanBuilder("WriteRows").startScopedSpan()) {
      System.out.println("Write rows to the table");
      for (int i = 0; i < 10; i++) {
        String rowKey = "greeting" + i;

        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(COLUMN_FAMILY_NAME, COLUMN_NAME, Bytes.toBytes(GREETINGS[i % 3]));
        table.put(put);
      }
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
      System.out.println("Scan for all greetings:");
      Scan scan = new Scan();

      ResultScanner scanner = table.getScanner(scan);
      for (Result row : scanner) {
        byte[] valueBytes = row.getValue(COLUMN_FAMILY_NAME, COLUMN_NAME);
        System.out.println('\t' + Bytes.toString(valueBytes));
      }
    }
  }

  private void enableOpenCensusObservability() throws IOException {
    // Start: enable observability with OpenCensus tracing and metrics
    // Create and register the Stackdriver Tracing exporter
    StackdriverTraceExporter.createAndRegister(
      StackdriverTraceConfiguration.builder().setProjectId(PROJECT_ID).build());

    // Create and register the Stackdriver Monitoring/Metrics exporter
    StackdriverStatsExporter.createAndRegister(
      StackdriverStatsConfiguration.builder().setProjectId(PROJECT_ID).build());

    // Register all the gRPC views
    RpcViews.registerAllGrpcViews();

    // For demo purposes, we are enabling the always sampler
    TraceConfig traceConfig = Tracing.getTraceConfig();
    traceConfig.updateActiveTraceParams(
      traceConfig.getActiveTraceParams().toBuilder().setSampler(Samplers.alwaysSample()).build());
    // End: enable observability with OpenCensus
  }
}

