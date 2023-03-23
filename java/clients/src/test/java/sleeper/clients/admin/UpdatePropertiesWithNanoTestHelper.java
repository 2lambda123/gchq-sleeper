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

package sleeper.clients.admin;

import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.SleeperProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.util.RunCommand;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

import static sleeper.configuration.properties.PropertiesUtils.loadProperties;
import static sleeper.utils.RunCommandTestHelper.commandRunOn;

public class UpdatePropertiesWithNanoTestHelper {
    private final Path tempDir;
    private final Path expectedPropertiesFile;

    public UpdatePropertiesWithNanoTestHelper(Path tempDir) {
        this.tempDir = tempDir;
        this.expectedPropertiesFile = tempDir.resolve("sleeper/admin/temp.properties");
    }

    public String[] openInstancePropertiesGetCommandRun(InstanceProperties properties) throws Exception {
        return commandRunOn(runCommand ->
                updaterWithCommandHandler(runCommand).openPropertiesFile(properties));
    }

    public InstanceProperties openInstancePropertiesGetPropertiesWritten(InstanceProperties properties) throws Exception {
        return new InstanceProperties(openFileGetPropertiesWritten(updater -> updater.openPropertiesFile(properties)));
    }

    public Path openInstancePropertiesGetPathToFile(InstanceProperties properties) throws Exception {
        return openFileGetPathToFile(updater -> updater.openPropertiesFile(properties));
    }

    public UpdatePropertiesRequest<InstanceProperties> updateProperties(
            InstanceProperties before, InstanceProperties after) throws Exception {
        return updaterSavingProperties(after).openPropertiesFile(before);
    }

    public UpdatePropertiesRequest<TableProperties> updateProperties(
            TableProperties before, TableProperties after) throws Exception {
        return updaterSavingProperties(after).openPropertiesFile(before);
    }

    @FunctionalInterface
    public interface OpenFile<T extends SleeperProperties<?>> {
        UpdatePropertiesRequest<T> open(UpdatePropertiesWithNano updater) throws IOException, InterruptedException;
    }

    public <T extends SleeperProperties<?>> Properties openFileGetPropertiesWritten(OpenFile<T> openFile) throws Exception {
        AtomicReference<Properties> foundProperties = new AtomicReference<>();
        openFile.open(updaterWithCommandHandler(command -> {
            foundProperties.set(loadProperties(expectedPropertiesFile));
            return 0;
        }));
        return foundProperties.get();
    }

    public <T extends SleeperProperties<?>> Path openFileGetPathToFile(OpenFile<T> openFile) throws Exception {
        openFile.open(updaterWithCommandHandler(command -> 0));
        return expectedPropertiesFile;
    }

    private UpdatePropertiesWithNano updaterSavingProperties(SleeperProperties<?> after) {
        return updaterWithCommandHandler(command -> {
            after.save(expectedPropertiesFile);
            return 0;
        });
    }

    private UpdatePropertiesWithNano updaterWithCommandHandler(RunCommand runCommand) {
        return new UpdatePropertiesWithNano(tempDir, runCommand);
    }
}
