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

package sleeper.build.github.api.containers;

import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import com.github.tomakehurst.wiremock.junit5.WireMockTest;
import com.github.tomakehurst.wiremock.matching.RequestPatternBuilder;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.StringReader;
import java.time.Instant;
import java.util.List;
import java.util.Properties;

import static com.github.tomakehurst.wiremock.client.WireMock.delete;
import static com.github.tomakehurst.wiremock.client.WireMock.deleteRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.verify;
import static sleeper.build.github.api.GitHubApiTestHelper.doWithGitHubApi;
import static sleeper.build.github.api.GitHubApiTestHelper.gitHubRequest;
import static sleeper.build.github.api.GitHubApiTestHelper.gitHubResponse;
import static sleeper.build.github.api.TestGitHubJson.gitHubJson;
import static sleeper.build.github.api.containers.DeleteGHCRImages.withApi;
import static sleeper.build.github.api.containers.TestGHCRImage.image;
import static sleeper.build.github.api.containers.TestGHCRImage.imageWithId;
import static sleeper.build.github.api.containers.TestGHCRImage.imageWithIdAndTags;
import static sleeper.build.testutil.TestResources.exampleString;

@WireMockTest
class DeleteGHCRImagesTest {

    @Test
    void shouldDeleteAnImage(WireMockRuntimeInfo runtimeInfo) {
        // Given
        packageVersionListReturns("sleeper-local",
                exampleString("examples/github-api/package-version-list-one-image.json"));
        packageVersionDeleteSucceeds("sleeper-local", 64403175);

        // When
        deleteImages(runtimeInfo, "imageName=sleeper-local");

        // Then
        verify(packageVersionDeleted("sleeper-local", 64403175));
    }

    @Test
    void shouldDeleteMultipleImages(WireMockRuntimeInfo runtimeInfo) {
        // Given
        packageVersionListReturns("test-image", imageWithId(1), imageWithId(2));
        packageVersionDeleteSucceeds("test-image", 1);
        packageVersionDeleteSucceeds("test-image", 2);

        // When
        deleteImages(runtimeInfo, "imageName=test-image");

        // Then
        verify(packageVersionDeleted("test-image", 1));
        verify(packageVersionDeleted("test-image", 2));
    }

    @Test
    void shouldNotDeleteSpecifiedTag(WireMockRuntimeInfo runtimeInfo) {
        // Given
        packageVersionListReturns("test-image", imageWithIdAndTags(123, "test-tag"));

        // When
        deleteImages(runtimeInfo, "" +
                "imageName=test-image\n" +
                "ignoreTagsPattern=test-tag");

        // Then
        verify(0, packageVersionDeleted("test-image", 123));
    }

    @Test
    void shouldKeepMostRecentImage(WireMockRuntimeInfo runtimeInfo) {
        // Given
        packageVersionListReturns("test-image",
                image().id(1).updatedAt(Instant.parse("2023-01-20T15:00:12Z")).build(),
                image().id(2).updatedAt(Instant.parse("2023-01-20T15:30:12Z")).build());
        packageVersionDeleteSucceeds("test-image", 1);

        // When
        deleteImages(runtimeInfo, "" +
                "imageName=test-image\n" +
                "keepMostRecent=1");

        // Then
        verify(1, packageVersionDeleted("test-image", 1));
        verify(0, packageVersionDeleted("test-image", 2));
    }

    @Test
    void shouldKeepMostRecentImageNotIgnoredByTagPattern(WireMockRuntimeInfo runtimeInfo) {
        // Given
        packageVersionListReturns("test-image",
                image().id(1).updatedAt(Instant.parse("2023-01-20T15:00:00Z")).tags("ignore-tag-1").build(),
                image().id(2).updatedAt(Instant.parse("2023-01-20T15:30:00Z")).tags("ignore-tag-2").build(),
                image().id(3).updatedAt(Instant.parse("2023-01-20T16:00:00Z")).tags("other-tag-1").build(),
                image().id(4).updatedAt(Instant.parse("2023-01-20T16:30:00Z")).tags("other-tag-2").build());
        packageVersionDeleteSucceeds("test-image", 3);

        //When
        deleteImages(runtimeInfo, "" +
                "imageName=test-image\n" +
                "ignoreTagsPattern=ignore-tag-.*\n" +
                "keepMostRecent=1");

        // Then
        verify(0, packageVersionDeleted("test-image", 1));
        verify(0, packageVersionDeleted("test-image", 2));
        verify(1, packageVersionDeleted("test-image", 3));
        verify(0, packageVersionDeleted("test-image", 4));
    }

    private void deleteImages(WireMockRuntimeInfo runtimeInfo, String propertiesStr) {
        Properties properties = loadProperties(propertiesStr);
        properties.setProperty("organization", "test-org");
        doWithGitHubApi(runtimeInfo, api ->
                withApi(api).properties(properties).build().deleteImages());
    }

    private void packageVersionListReturns(String packageName, TestGHCRImage... versions) {
        packageVersionListReturns(packageName, gitHubJson(List.of(versions)));
    }

    private void packageVersionListReturns(String packageName, String body) {
        stubFor(gitHubRequest(get("/orgs/test-org/packages/container/" + packageName + "/versions"))
                .willReturn(gitHubResponse()
                        .withStatus(200)
                        .withBody(body)));
    }

    private void packageVersionDeleteSucceeds(String packageName, int versionId) {
        stubFor(gitHubRequest(delete("/orgs/test-org/packages/container/" + packageName + "/versions/" + versionId))
                .willReturn(gitHubResponse().withStatus(204)));
    }

    private RequestPatternBuilder packageVersionDeleted(String packageName, int versionId) {
        return gitHubRequest(deleteRequestedFor(
                urlEqualTo("/orgs/test-org/packages/container/" + packageName + "/versions/" + versionId)));
    }

    private static Properties loadProperties(String propertiesStr) {
        Properties properties = new Properties();
        try {
            properties.load(new StringReader(propertiesStr));
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
        return properties;
    }
}
