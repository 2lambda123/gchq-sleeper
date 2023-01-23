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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.build.github.api.GitHubApi;

import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.GenericType;

import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class DeleteGHCRImages {

    private static final Logger LOGGER = LoggerFactory.getLogger(DeleteGHCRImages.class);

    private final GitHubApi api;
    private final String organization;
    private final String imageName;
    private final Pattern ignoreTags;
    private final int keepMostRecent;

    private DeleteGHCRImages(Builder builder) {
        api = Objects.requireNonNull(builder.api, "api must not be null");
        organization = Objects.requireNonNull(builder.organization, "organization must not be null");
        imageName = Objects.requireNonNull(builder.imageName, "imageName must not be null");
        ignoreTags = builder.ignoreTags;
        keepMostRecent = builder.keepMostRecent;
    }

    public static void main(String[] args) throws IOException {
        Properties properties = loadProperties(Paths.get(args[0]));
        try (GitHubApi api = GitHubApi.withToken(properties.getProperty("token"))) {
            withApi(api).properties(properties).build().deleteImages();
        }
    }

    private static Properties loadProperties(Path path) throws IOException {
        Properties properties = new Properties();
        try (Reader reader = Files.newBufferedReader(path)) {
            properties.load(reader);
        }
        return properties;
    }

    public static Builder withApi(GitHubApi api) {
        return new Builder().api(api);
    }

    public void deleteImages() {
        LOGGER.info("Deleting images for {}/{}, ignoring {} and keeping {}",
                organization, imageName, ignoreTags, keepMostRecent);
        List<GitHubPackageVersionResponse> all = getAllVersions();
        List<GitHubPackageVersionResponse> toDelete = getVersionsToDelete(all).collect(Collectors.toList());
        LOGGER.info("Deleting {} of {} versions", toDelete.size(), all.size());
        toDelete.forEach(this::deleteVersion);
    }

    @SuppressFBWarnings("SIC_INNER_SHOULD_BE_STATIC_ANON") // GenericType is intended to be used as an anonymous class
    private List<GitHubPackageVersionResponse> getAllVersions() {
        WebTarget target = containerPath().path("versions");
        return api.request(target).get(new GenericType<>() {
        });
    }

    private Stream<GitHubPackageVersionResponse> getVersionsToDelete(List<GitHubPackageVersionResponse> allVersions) {
        return allVersions.stream()
                .filter(this::hasNoIgnoredTags)
                .sorted(Comparator.comparing(GitHubPackageVersionResponse::getUpdatedAt).reversed())
                .skip(keepMostRecent);
    }

    private boolean hasNoIgnoredTags(GitHubPackageVersionResponse version) {
        return ignoreTags == null || version.getTags().stream().noneMatch(tag -> ignoreTags.matcher(tag).find());
    }

    private void deleteVersion(GitHubPackageVersionResponse version) {
        WebTarget target = containerPath().path("versions").path(version.getId());
        api.request(target).delete(Void.class);
    }

    private WebTarget containerPath() {
        return packagesBasePath().path("container").path(imageName);
    }

    private WebTarget packagesBasePath() {
        return api.path("orgs").path(organization).path("packages");
    }

    public static final class Builder {
        private GitHubApi api;
        private String organization;
        private String imageName;
        private Pattern ignoreTags;
        private int keepMostRecent;

        private Builder() {
        }

        public Builder api(GitHubApi api) {
            this.api = api;
            return this;
        }

        public Builder organization(String organization) {
            this.organization = organization;
            return this;
        }

        public Builder imageName(String imageName) {
            this.imageName = imageName;
            return this;
        }

        public Builder ignoreTags(Pattern ignoreTags) {
            this.ignoreTags = ignoreTags;
            return this;
        }

        public Builder ignoreTagsPattern(String ignoreTagsPattern) {
            return ignoreTags(ignoreTagsPattern == null ? null : Pattern.compile(ignoreTagsPattern));
        }

        public Builder keepMostRecent(int keepMostRecent) {
            this.keepMostRecent = keepMostRecent;
            return this;
        }

        public Builder keepMostRecent(String keepMostRecent) {
            return keepMostRecent(keepMostRecent == null ? 0 : Integer.parseInt(keepMostRecent));
        }

        public Builder properties(Properties properties) {
            return organization(properties.getProperty("organization"))
                    .imageName(properties.getProperty("imageName"))
                    .ignoreTagsPattern(properties.getProperty("ignoreTagsPattern"))
                    .keepMostRecent(properties.getProperty("keepMostRecent"));
        }

        public DeleteGHCRImages build() {
            return new DeleteGHCRImages(this);
        }
    }
}
