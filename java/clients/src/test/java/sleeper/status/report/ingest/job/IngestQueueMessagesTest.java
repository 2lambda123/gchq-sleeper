/*
 * Copyright 2022-2023 Crown Copyright
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

package sleeper.status.report.ingest.job;

import org.junit.jupiter.api.Test;

import sleeper.ToStringPrintStream;
import sleeper.configuration.properties.InstanceProperties;
import sleeper.job.common.QueueMessageCount;
import sleeper.job.common.QueueMessageCountsInMemory;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.configuration.properties.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.BULK_IMPORT_EKS_JOB_QUEUE_URL;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.BULK_IMPORT_EMR_JOB_QUEUE_URL;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.BULK_IMPORT_PERSISTENT_EMR_JOB_QUEUE_URL;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.INGEST_JOB_QUEUE_URL;
import static sleeper.job.common.QueueMessageCount.approximateNumberVisibleAndNotVisible;
import static sleeper.status.report.ingest.job.IngestJobStatusReporterTestData.ingestMessageCount;

class IngestQueueMessagesTest {
    @Test
    void shouldCountMessagesOnIngestQueue() {
        // Given
        InstanceProperties instanceProperties = createTestInstanceProperties();
        instanceProperties.set(INGEST_JOB_QUEUE_URL, "ingest-queue");
        QueueMessageCount.Client client = QueueMessageCountsInMemory.from(
                Map.of("ingest-queue", approximateNumberVisibleAndNotVisible(1, 2)));

        // When / Then
        assertThat(IngestQueueMessages.from(instanceProperties, client))
                .isEqualTo(IngestQueueMessages.builder()
                        .ingestMessages(1)
                        .build());
    }

    @Test
    void shouldCountMessagesOnAllQueues() {
        // Given
        InstanceProperties instanceProperties = createTestInstanceProperties();
        instanceProperties.set(INGEST_JOB_QUEUE_URL, "ingest-queue");
        instanceProperties.set(BULK_IMPORT_EMR_JOB_QUEUE_URL, "emr-queue");
        instanceProperties.set(BULK_IMPORT_PERSISTENT_EMR_JOB_QUEUE_URL, "persistent-emr-queue");
        instanceProperties.set(BULK_IMPORT_EKS_JOB_QUEUE_URL, "eks-queue");
        QueueMessageCount.Client client = QueueMessageCountsInMemory.from(Map.of(
                "ingest-queue", approximateNumberVisibleAndNotVisible(1, 2),
                "emr-queue", approximateNumberVisibleAndNotVisible(3, 4),
                "persistent-emr-queue", approximateNumberVisibleAndNotVisible(5, 6),
                "eks-queue", approximateNumberVisibleAndNotVisible(7, 8)));

        // When / Then
        assertThat(IngestQueueMessages.from(instanceProperties, client))
                .isEqualTo(IngestQueueMessages.builder()
                        .ingestMessages(1)
                        .emrMessages(3)
                        .persistentEmrMessages(5)
                        .eksMessages(7)
                        .build());
    }

    @Test
    void shouldReportMessagesWhenOnlyIngestQueueIsDeployed() {
        // Given
        IngestQueueMessages messages = ingestMessageCount(10);

        // When
        ToStringPrintStream out = new ToStringPrintStream();
        messages.print(out.getPrintStream());

        // Then
        assertThat(out).hasToString("Total jobs waiting in queue (excluded from report): 10\n");
    }
}
