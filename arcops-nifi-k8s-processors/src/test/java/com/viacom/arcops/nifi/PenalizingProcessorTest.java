package com.viacom.arcops.nifi;

import com.google.common.collect.ImmutableMap;
import lombok.extern.slf4j.Slf4j;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static com.viacom.arcops.nifi.NiFiProperties.SUCCESS;
import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
class PenalizingProcessorTest {

    private TestRunner testRunner;

    @BeforeEach
    void setUp() {
        testRunner = TestRunners.newTestRunner(new PenalizingProcessor());
    }

    @Test
    void shouldPenalize() {
        testRunner.enqueue("foo");

        testRunner.run();

        assertThat(testRunner.getFlowFilesForRelationship(SUCCESS).get(0).isPenalized()).isEqualTo(true);
    }

    @Test
    void shouldNotChangeFlowFile() {
        ImmutableMap<String, String> attributes = ImmutableMap.of("k1", "v1", "k2", "v2");
        String content = "some message";
        testRunner.enqueue(content, attributes);

        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(SUCCESS);
        List<MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship(SUCCESS);
        assertThat(flowFiles).hasSize(1);
        MockFlowFile flowFile = flowFiles.get(0);
        assertThat(flowFile.getAttributes()).containsAllEntriesOf(attributes);
        assertThat(testRunner.getContentAsByteArray(flowFile)).isEqualTo(content.getBytes());
    }
}
