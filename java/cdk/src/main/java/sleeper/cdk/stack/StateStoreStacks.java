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

package sleeper.cdk.stack;

import software.amazon.awscdk.services.iam.IGrantable;

import static sleeper.cdk.stack.StateStoreGrants.readActiveFilesAndPartitions;
import static sleeper.cdk.stack.StateStoreGrants.readActiveFilesReadWritePartitions;
import static sleeper.cdk.stack.StateStoreGrants.readAllFilesAndPartitions;
import static sleeper.cdk.stack.StateStoreGrants.readPartitions;
import static sleeper.cdk.stack.StateStoreGrants.readPartitionsReadWriteActiveFiles;
import static sleeper.cdk.stack.StateStoreGrants.readWriteActiveAndReadyForGCFiles;
import static sleeper.cdk.stack.StateStoreGrants.readWriteAllFilesAndPartitions;
import static sleeper.cdk.stack.StateStoreGrants.readWritePartitions;
import static sleeper.cdk.stack.StateStoreGrants.readWriteReadyForGCFiles;

public final class StateStoreStacks {

    private final DynamoDBStateStoreStack dynamo;
    private final S3StateStoreStack s3;
    private final TransactionLogStateStoreStack transactionLog;

    public StateStoreStacks(
            DynamoDBStateStoreStack dynamo, S3StateStoreStack s3,
            TransactionLogStateStoreStack transactionLog,
            ManagedPoliciesStack policiesStack) {
        this.dynamo = dynamo;
        this.s3 = s3;
        this.transactionLog = transactionLog;
        grantAccess(readPartitionsReadWriteActiveFiles(), policiesStack.getDirectIngestPolicyForGrants());
        grantAccess(readActiveFilesAndPartitions(), policiesStack.getQueryPolicyForGrants());
        grantAccess(readAllFilesAndPartitions(), policiesStack.getReportingPolicyForGrants());
        grantAccess(readWritePartitions(), policiesStack.getEditTablesPolicyForGrants());
    }

    public void grantReadActiveFilesAndPartitions(IGrantable grantee) {
        grantAccess(readActiveFilesAndPartitions(), grantee);
    }

    public void grantReadWriteAllFilesAndPartitions(IGrantable grantee) {
        grantAccess(readWriteAllFilesAndPartitions(), grantee);
    }

    public void grantReadActiveFilesReadWritePartitions(IGrantable grantee) {
        grantAccess(readActiveFilesReadWritePartitions(), grantee);
    }

    public void grantReadPartitionsReadWriteActiveFiles(IGrantable grantee) {
        grantAccess(readPartitionsReadWriteActiveFiles(), grantee);
    }

    public void grantReadPartitions(IGrantable grantee) {
        grantAccess(readPartitions(), grantee);
    }

    public void grantReadWriteActiveAndReadyForGCFiles(IGrantable grantee) {
        grantAccess(readWriteActiveAndReadyForGCFiles(), grantee);
    }

    public void grantReadWriteReadyForGCFiles(IGrantable grantee) {
        grantAccess(readWriteReadyForGCFiles(), grantee);
    }

    public void grantReadWritePartitions(IGrantable grantee) {
        grantAccess(readWritePartitions(), grantee);
    }

    public void grantAccess(StateStoreGrants grants, IGrantable grantee) {
        dynamo.grantAccess(grants, grantee);
        s3.grantAccess(grants, grantee);
        transactionLog.grantAccess(grants, grantee);
    }
}
