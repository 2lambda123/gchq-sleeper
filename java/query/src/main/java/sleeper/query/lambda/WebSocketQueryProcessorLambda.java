/*
 * Copyright 2022 Crown Copyright
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
package sleeper.query.lambda;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.apigatewaymanagementapi.AmazonApiGatewayManagementApi;
import com.amazonaws.services.apigatewaymanagementapi.AmazonApiGatewayManagementApiClientBuilder;
import com.amazonaws.services.apigatewaymanagementapi.model.PostToConnectionRequest;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.APIGatewayV2WebSocketEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayV2WebSocketResponse;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.google.gson.JsonParseException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.SystemDefinedInstanceProperty;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.query.model.Query;
import sleeper.query.model.QuerySerDe;
import sleeper.query.model.output.ResultsOutputConstants;
import sleeper.query.model.output.WebSocketResultsOutput;
import sleeper.query.tracker.QueryStatusReportListener;
import sleeper.query.tracker.WebSocketQueryStatusReportDestination;

public class WebSocketQueryProcessorLambda implements RequestHandler<APIGatewayV2WebSocketEvent, APIGatewayV2WebSocketResponse> {
    private static final Logger LOGGER = LoggerFactory.getLogger(WebSocketQueryProcessorLambda.class);

    private final QuerySerDe serde;
    private final AmazonSQS sqsClient;
    private final String queryQueueUrl;

    public WebSocketQueryProcessorLambda() throws IOException {
        this(
            AmazonS3ClientBuilder.defaultClient(),
            AmazonSQSClientBuilder.defaultClient(),
            System.getenv(SystemDefinedInstanceProperty.CONFIG_BUCKET.toEnvironmentVariable())
        );
    }

    public WebSocketQueryProcessorLambda(AmazonS3 s3Client, AmazonSQS sqsClient, String configBucket) throws IOException {
        this.sqsClient = sqsClient;
        InstanceProperties instanceProperties = new InstanceProperties();
        instanceProperties.loadFromS3(s3Client, configBucket);
        this.queryQueueUrl = instanceProperties.get(SystemDefinedInstanceProperty.QUERY_QUEUE_URL);
        TablePropertiesProvider tablePropertiesProvider = new TablePropertiesProvider(s3Client, instanceProperties);
        this.serde = new QuerySerDe(tablePropertiesProvider);
    }

    private void sendErrorToClient(String endpoint, String region, String connectionId, String errorMessage) {
        AmazonApiGatewayManagementApi client = AmazonApiGatewayManagementApiClientBuilder.standard()
            .withEndpointConfiguration(new EndpointConfiguration(endpoint, region))
            .build();

        String data = "{\"message\":\"error\",\"error\":\"" + errorMessage + "\"}";
        PostToConnectionRequest request = new PostToConnectionRequest()
            .withConnectionId(connectionId)
            .withData(ByteBuffer.wrap(data.getBytes()));

        client.postToConnection(request);
    }

    public void submitQueryForProcessing(Query query) {
        String message = serde.toJson(query);
        sqsClient.sendMessage(queryQueueUrl, message);
    }

    @Override
    public APIGatewayV2WebSocketResponse handleRequest(APIGatewayV2WebSocketEvent event, Context context) {
        LOGGER.info("Received WebSocket event: {}", event);

        if (event.getRequestContext().getEventType().equals("MESSAGE")) {
            String region = System.getenv("AWS_REGION");
            if (region == null) {
                throw new RuntimeException("Unable to detect current region!");
            }
            String endpoint = "https://" + event.getRequestContext().getApiId() + ".execute-api." + region + ".amazonaws.com/" + event.getRequestContext().getStage();

            Query query = null;
            try {
                query = serde.fromJson(event.getBody());
                LOGGER.info("Deserialised message to query: {}", query);
            } catch (JsonParseException e) {
                LOGGER.error("Failed to deserialise query", e);
                this.sendErrorToClient(endpoint, region, event.getRequestContext().getConnectionId(), "Received malformed query JSON request");
            }

            if (query != null) {
                if (query.getStatusReportDestinations() == null) {
                    query.setStatusReportDestinations(new ArrayList<>());
                }
                Map<String, String> statusReportDestination = new HashMap<>();
                statusReportDestination.put(QueryStatusReportListener.DESTINATION, WebSocketQueryStatusReportDestination.DESTINATION_NAME);
                statusReportDestination.put(WebSocketQueryStatusReportDestination.ENDPOINT, endpoint);
                statusReportDestination.put(WebSocketQueryStatusReportDestination.CONNECTION_ID, event.getRequestContext().getConnectionId());
                query.addStatusReportDestination(statusReportDestination);

                if (query.getResultsPublisherConfig() == null) query.setResultsPublisherConfig(new HashMap<>());
                // Default to sending results back to client via WebSocket connection
                if (
                    query.getResultsPublisherConfig().get(ResultsOutputConstants.DESTINATION) == null ||
                    query.getResultsPublisherConfig().get(ResultsOutputConstants.DESTINATION).equals(WebSocketResultsOutput.DESTINATION_NAME)
                ) {
                    query.getResultsPublisherConfig().put(ResultsOutputConstants.DESTINATION, WebSocketResultsOutput.DESTINATION_NAME);
                    query.getResultsPublisherConfig().put(WebSocketResultsOutput.ENDPOINT, endpoint);
                    query.getResultsPublisherConfig().put(WebSocketResultsOutput.CONNECTION_ID, event.getRequestContext().getConnectionId());
                }

                LOGGER.info("Query to be processed: {}", query);
                this.submitQueryForProcessing(query);
            }
        }

        APIGatewayV2WebSocketResponse response = new APIGatewayV2WebSocketResponse();
        response.setStatusCode(200);
        return response;
    }
}
