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
package sleeper.utils;


import org.junit.jupiter.api.Test;

import sleeper.util.PollWithRetries;

import java.util.Iterator;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PollWithRetriesTest {

    @Test
    void shouldRepeatPoll() throws Exception {
        // Given
        PollWithRetries poll = PollWithRetries.intervalAndMaxPolls(0, 2);
        Iterator<Boolean> iterator = List.of(false, true).iterator();

        // When
        poll.pollUntil("iterator returns true", iterator::next);

        // Then
        assertThat(iterator).isExhausted();
    }

    @Test
    void shouldFailIfMaxPollsReached() {
        // Given
        PollWithRetries poll = PollWithRetries.intervalAndMaxPolls(0, 2);
        Iterator<Boolean> iterator = List.of(false, false).iterator();

        // When / Then
        assertThatThrownBy(() -> poll.pollUntil("iterator returns true", iterator::next))
                .isInstanceOf(PollWithRetries.TimedOutException.class)
                .hasMessage("Timed out waiting until iterator returns true");
        assertThat(iterator).isExhausted();
    }

    @Test
    void shouldResetPollCountBetweenPollUntilCalls() throws Exception {
        // Given
        PollWithRetries poll = PollWithRetries.intervalAndMaxPolls(0, 2);
        Iterator<Boolean> iterator1 = List.of(false, true).iterator();
        Iterator<Boolean> iterator2 = List.of(false, true).iterator();

        // When
        poll.pollUntil("iterator returns true", iterator1::next);
        poll.pollUntil("iterator returns true", iterator2::next);

        // Then
        assertThat(iterator1).isExhausted();
        assertThat(iterator2).isExhausted();
    }
}
