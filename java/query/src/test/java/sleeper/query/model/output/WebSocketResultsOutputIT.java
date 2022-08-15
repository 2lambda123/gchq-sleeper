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
package sleeper.query.model.output;

import com.github.tomakehurst.wiremock.junit.WireMockClassRule;
import com.github.tomakehurst.wiremock.matching.UrlPattern;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import sleeper.core.iterator.WrappedIterator;
import sleeper.core.record.Record;
import sleeper.query.model.Query;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.matchingJsonPath;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static org.assertj.core.api.Assertions.assertThat;

public class WebSocketResultsOutputIT {

    @ClassRule
    public static WireMockClassRule wireMockRule = new WireMockClassRule();

    @Rule
    public WireMockClassRule wireMock = wireMockRule;

    @Test
    public void shouldStopPublishingResultsWhenClientHasGone() {
        // Given
        String connectionId = "connection1";
        UrlPattern url = urlEqualTo("/@connections/" + connectionId);
        wireMock.stubFor(post(url).willReturn(aResponse()
                .withStatus(410)
                .withHeader("x-amzn-ErrorType", "GoneException")));

        Query query = new Query("table1", "query1", Collections.emptyList());

        Map<String, String> config = new HashMap<>();
        config.put(WebSocketResultsOutput.ENDPOINT, wireMock.baseUrl());
        config.put(WebSocketResultsOutput.REGION, "eu-west-1");
        config.put(WebSocketResultsOutput.CONNECTION_ID, connectionId);
        config.put(WebSocketResultsOutput.MAX_BATCH_SIZE, "1");
        config.put(WebSocketResultsOutput.ACCESS_KEY, "accessKey");
        config.put(WebSocketResultsOutput.SECRET_KEY, "secretKey");
        WebSocketResultsOutput out = new WebSocketResultsOutput(config);

        List<Record> records = new ArrayList<>();
        records.add(new Record(Collections.singletonMap("id", "record1")));
        records.add(new Record(Collections.singletonMap("id", "record2")));
        records.add(new Record(Collections.singletonMap("id", "record3")));
        records.add(new Record(Collections.singletonMap("id", "record4")));
        records.add(new Record(Collections.singletonMap("id", "record5")));

        // When
        ResultsOutputInfo result = out.publish(query, new WrappedIterator<>(records.iterator()));

        // Then
        wireMock.verify(1, postRequestedFor(url).withRequestBody(
                matchingJsonPath("$.queryId", equalTo("query1"))
                        .and(matchingJsonPath("$.message", equalTo("records")))
        ));
        assertThat(result.getRecordCount()).isEqualTo(0);
        assertThat(result.getError()).hasMessageContaining("GoneException");
    }

    @Test
    public void shouldBatchResultsAccordingToConfig() {
        // Given
        String connectionId = "connection1";
        UrlPattern url = urlEqualTo("/@connections/" + connectionId);
        wireMock.stubFor(post(url).willReturn(aResponse().withStatus(200)));

        Query query = new Query("table1", "query1", Collections.emptyList());

        Map<String, String> config = new HashMap<>();
        config.put(WebSocketResultsOutput.ENDPOINT, wireMock.baseUrl());
        config.put(WebSocketResultsOutput.REGION, "eu-west-1");
        config.put(WebSocketResultsOutput.CONNECTION_ID, connectionId);
        config.put(WebSocketResultsOutput.MAX_BATCH_SIZE, "1");
        config.put(WebSocketResultsOutput.ACCESS_KEY, "accessKey");
        config.put(WebSocketResultsOutput.SECRET_KEY, "secretKey");
        WebSocketResultsOutput out = new WebSocketResultsOutput(config);

        List<Record> records = new ArrayList<>();
        records.add(new Record(Collections.singletonMap("id", "record1")));
        records.add(new Record(Collections.singletonMap("id", "record2")));
        records.add(new Record(Collections.singletonMap("id", "record3")));
        records.add(new Record(Collections.singletonMap("id", "record4")));
        records.add(new Record(Collections.singletonMap("id", "record5")));

        // When
        out.publish(query, new WrappedIterator<>(records.iterator()));

        // Then
        wireMock.verify(records.size(), postRequestedFor(url).withRequestBody(
                matchingJsonPath("$.queryId", equalTo("query1"))
                        .and(matchingJsonPath("$.message", equalTo("records")))
        ));
    }
}
