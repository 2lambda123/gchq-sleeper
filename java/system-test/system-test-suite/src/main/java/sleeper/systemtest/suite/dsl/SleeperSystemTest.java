/*
 * Copyright 2022-2024 Crown Copyright
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package sleeper.systemtest.suite.dsl;

import software.amazon.awscdk.NestedStack;

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TableProperty;
import sleeper.core.record.Record;
import sleeper.core.schema.Schema;
import sleeper.systemtest.datageneration.GenerateNumberedValueOverrides;
import sleeper.systemtest.datageneration.RecordNumbers;
import sleeper.systemtest.drivers.ingest.IngestSourceFilesDriver;
import sleeper.systemtest.drivers.ingest.PurgeQueueDriver;
import sleeper.systemtest.drivers.instance.OptionalStacksDriver;
import sleeper.systemtest.drivers.instance.ReportingContext;
import sleeper.systemtest.drivers.instance.SleeperInstanceContext;
import sleeper.systemtest.drivers.instance.SystemTestDeploymentContext;
import sleeper.systemtest.drivers.instance.SystemTestInstanceConfiguration;
import sleeper.systemtest.drivers.instance.SystemTestParameters;
import sleeper.systemtest.suite.dsl.ingest.SystemTestIngest;
import sleeper.systemtest.suite.dsl.python.SystemTestPythonApi;
import sleeper.systemtest.suite.dsl.query.SystemTestQuery;
import sleeper.systemtest.suite.dsl.reports.SystemTestReporting;
import sleeper.systemtest.suite.dsl.reports.SystemTestReports;
import sleeper.systemtest.suite.dsl.sourcedata.SystemTestCluster;
import sleeper.systemtest.suite.dsl.sourcedata.SystemTestSourceFiles;
import sleeper.systemtest.suite.fixtures.SystemTestClients;
import sleeper.systemtest.suite.fixtures.SystemTestInstance;

import java.nio.file.Path;
import java.util.Map;
import java.util.stream.LongStream;

import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;

/**
 * This class is the entry point that all system tests use to interact with the system.
 * It's the starting point for all steps of the Domain Specific Language (DSL) for system tests.
 * The purpose of this is to make it as easy as possible to read and write system tests.
 * It should make it easy to find and reuse steps to interact with the systems you care about for your tests.
 * <p>
 * It does this by delegating to drivers to actually interact with the system. The DSL only defines
 * the steps and language to be used by tests, and not any of the implemented behaviour of the steps.
 * <p>
 * This class should match as closely as possible with a diagram of the components of the system. You can
 * expect to find the system you care about through a method on this class. Some core features of the system are
 * on this class directly, but we try to limit its size by grouping methods into components which you access through
 * a method, eg. {@link #ingest()}.
 * <p>
 * Most tests will use steps from different systems, but where multiple steps run against the same system in succession
 * we can use method chaining to avoid accessing that system repeatedly. If you go from one system to another and back,
 * assume you should re-access the first system from here again the second time.
 * Try to avoid assigning variables except for data you want to reuse.
 */
public class SleeperSystemTest {
    private static final SleeperSystemTest INSTANCE = new SleeperSystemTest();

    private final SystemTestParameters parameters = SystemTestParameters.loadFromSystemProperties();
    private final SystemTestClients clients = new SystemTestClients();
    private final SystemTestDeploymentContext systemTest = new SystemTestDeploymentContext(
            parameters, clients.getS3(), clients.getS3V2(), clients.getEcr(), clients.getCloudFormation());
    private final SleeperInstanceContext instance = new SleeperInstanceContext(
            parameters, systemTest, clients.getDynamoDB(), clients.getS3(), clients.getS3V2(),
            clients.getSts(), clients.getRegionProvider(), clients.getCloudFormation(), clients.getEcr());
    private final ReportingContext reportingContext = new ReportingContext(parameters);
    private final IngestSourceFilesDriver sourceFiles = new IngestSourceFilesDriver(systemTest, clients.getS3V2());
    private final PurgeQueueDriver purgeQueueDriver = new PurgeQueueDriver(instance, clients.getSqs());

    private SleeperSystemTest() {
    }

    public static SleeperSystemTest getInstance() {
        return INSTANCE.reset();
    }

    private SleeperSystemTest reset() {
        try {
            systemTest.deployIfMissing();
            systemTest.resetProperties();
            sourceFiles.emptySourceBucket();
            instance.disconnect();
            reportingContext.startRecording();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        return this;
    }

    public void connectToInstance(SystemTestInstance testInstance) {
        SystemTestInstanceConfiguration configuration = testInstance.getInstanceConfiguration(parameters);
        instance.connectTo(testInstance.getIdentifier(), configuration);
        instance.resetPropertiesAndTables();
    }

    public void connectToInstanceNoTables(SystemTestInstance testInstance) {
        SystemTestInstanceConfiguration configuration = testInstance.getInstanceConfiguration(parameters);
        instance.connectTo(testInstance.getIdentifier(), configuration);
        instance.resetPropertiesAndDeleteTables();
    }

    public InstanceProperties instanceProperties() {
        return instance.getInstanceProperties();
    }

    public TableProperties tableProperties() {
        return instance.getTableProperties();
    }

    public void updateTableProperties(Map<TableProperty, String> values) {
        instance.updateTableProperties(values);
    }

    public SystemTestSourceFiles sourceFiles() {
        return new SystemTestSourceFiles(instance, sourceFiles);
    }

    public SystemTestSourceFiles sourceFilesFromDataBucket() {
        return new SystemTestSourceFiles(instance,
                new IngestSourceFilesDriver(instanceProperties().get(DATA_BUCKET), clients.getS3V2()));
    }

    public SystemTestTableFiles tableFiles() {
        return new SystemTestTableFiles(instance);
    }

    public SystemTestPartitioning partitioning() {
        return new SystemTestPartitioning(instance, clients);
    }

    public SystemTestIngest ingest() {
        return new SystemTestIngest(instance, clients, sourceFiles, purgeQueueDriver);
    }

    public SystemTestIngest ingestFromDataBucket() {
        return new SystemTestIngest(instance, clients,
                new IngestSourceFilesDriver(instanceProperties().get(DATA_BUCKET), clients.getS3V2()), purgeQueueDriver);
    }

    public SystemTestQuery query() {
        return new SystemTestQuery(instance, clients);
    }

    public SystemTestQuery directQuery() {
        return query().direct();
    }

    public SystemTestCompaction compaction() {
        return new SystemTestCompaction(instance, clients);
    }

    public SystemTestReporting reporting() {
        return new SystemTestReporting(instance, clients, reportingContext);
    }

    public SystemTestReports.SystemTestBuilder reportsForExtension() {
        return SystemTestReports.builder(reportingContext, instance, clients);
    }

    public SystemTestCluster systemTestCluster() {
        return new SystemTestCluster(systemTest, instance, clients);
    }

    public SystemTestPythonApi pythonApi() {
        return new SystemTestPythonApi(instance, clients, parameters.getPythonDirectory());
    }

    public SystemTestLocalFiles localFiles(Path tempDir) {
        return new SystemTestLocalFiles(instance, tempDir);
    }

    public void setGeneratorOverrides(GenerateNumberedValueOverrides overrides) {
        instance.setGeneratorOverrides(overrides);
    }

    public Iterable<Record> generateNumberedRecords(LongStream numbers) {
        return () -> instance.generateNumberedRecords(numbers).iterator();
    }

    public Iterable<Record> generateNumberedRecords(Schema schema, LongStream numbers) {
        return () -> instance.generateNumberedRecords(schema, numbers).iterator();
    }

    public RecordNumbers scrambleNumberedRecords(LongStream longStream) {
        return RecordNumbers.scrambleNumberedRecords(longStream);
    }

    public Path getSplitPointsDirectory() {
        return parameters.getScriptsDirectory()
                .resolve("test/splitpoints");
    }

    public <T extends NestedStack> void enableOptionalStack(Class<T> stackClass) throws InterruptedException {
        new OptionalStacksDriver(instance).addOptionalStack(stackClass);
    }

    public <T extends NestedStack> void disableOptionalStack(Class<T> stackClass) throws InterruptedException {
        new OptionalStacksDriver(instance).removeOptionalStack(stackClass);
    }

    public SystemTestTables tables() {
        return new SystemTestTables(instance);
    }
}
