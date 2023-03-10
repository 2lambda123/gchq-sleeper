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

package sleeper.clients.teardown;

import com.amazonaws.services.ecr.AmazonECR;
import com.amazonaws.services.ecr.model.DeleteRepositoryRequest;
import com.amazonaws.services.ecr.model.RepositoryNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.InstanceProperty;

import java.util.List;

import static sleeper.configuration.properties.UserDefinedInstanceProperty.BULK_IMPORT_REPO;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.ECR_COMPACTION_REPO;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.ECR_INGEST_REPO;

public class RemoveECRRepositories {
    private static final Logger LOGGER = LoggerFactory.getLogger(RemoveECRRepositories.class);

    private RemoveECRRepositories() {
    }

    public static void remove(AmazonECR ecr, InstanceProperties properties, List<InstanceProperty> extraRepositories) {
        List.of(ECR_COMPACTION_REPO, ECR_INGEST_REPO, BULK_IMPORT_REPO).forEach(property ->
                deleteIfSet(ecr, properties, property));
        extraRepositories.forEach(property -> deleteIfSet(ecr, properties, property));
    }

    private static void deleteIfSet(AmazonECR ecr, InstanceProperties properties, InstanceProperty property) {
        if (properties.isSet(property)) {
            String repositoryName = properties.get(property);
            LOGGER.info("Deleting repository {}", repositoryName);
            try {
                ecr.deleteRepository(new DeleteRepositoryRequest()
                        .withRepositoryName(repositoryName)
                        .withForce(true));
            } catch (RepositoryNotFoundException e) {
                LOGGER.info("Repository not found: {}", repositoryName);
            }
        }
    }
}
