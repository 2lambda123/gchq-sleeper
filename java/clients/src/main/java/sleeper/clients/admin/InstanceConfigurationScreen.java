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
import sleeper.console.ConsoleInput;
import sleeper.console.ConsoleOutput;
import sleeper.console.menu.ChooseOne;
import sleeper.console.menu.ConsoleChoice;

import java.io.IOException;
import java.io.UncheckedIOException;

public class InstanceConfigurationScreen {
    private final ConsoleOutput out;
    private final ConsoleInput in;
    private final AdminConfigStore store;
    private final UpdatePropertiesWithNano editor;

    public InstanceConfigurationScreen(ConsoleOutput out, ConsoleInput in, AdminConfigStore store, UpdatePropertiesWithNano editor) {
        this.out = out;
        this.in = in;
        this.store = store;
        this.editor = editor;
    }

    public void viewAndEditProperties(String instanceId) throws InterruptedException {
        ChooseOne chooseOne = new ChooseOne(out, in);
        UpdatePropertiesRequest<InstanceProperties> request = openFile(instanceId);
        if (request.isChanged()) {
            request.print(out);
            chooseOne.chooseFrom(
                    ConsoleChoice.describedAs("Apply changes"),
                    ConsoleChoice.describedAs("Return to editor"),
                    ConsoleChoice.describedAs("Discard changes and return to main menu"));
        }
    }

    private UpdatePropertiesRequest<InstanceProperties> openFile(String instanceId) throws InterruptedException {
        try {
            return editor.openPropertiesFile(store.loadInstanceProperties(instanceId));
        } catch (IOException e1) {
            throw new UncheckedIOException(e1);
        }
    }

    // Make SpotBugs happy (remove later)
    public ConsoleInput getIn() {
        return in;
    }
}
