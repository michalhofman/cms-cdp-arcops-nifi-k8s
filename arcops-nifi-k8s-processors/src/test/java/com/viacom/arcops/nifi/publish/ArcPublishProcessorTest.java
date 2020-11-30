package com.viacom.arcops.nifi.publish;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.*;
import com.viacom.arcops.uca.PublishResponse;
import com.viacom.arcops.uca.ServerConfig;
import com.viacom.arcops.uca.UcaWriteService;
import lombok.RequiredArgsConstructor;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;

import java.io.IOException;
import java.util.function.Function;
import java.util.stream.Stream;

import static com.google.common.base.Charsets.UTF_8;

import static com.viacom.arcops.nifi.NiFiProperties.ATTR_EXCEPTION_MESSAGE;
import static com.viacom.arcops.nifi.NiFiProperties.ATTR_EXCEPTION_STACKTRACE;
import static com.viacom.arcops.nifi.publish.ArcPublishProcessor.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class ArcPublishProcessorTest {

    private static final PublishInput VALID_PUBLISH_INPUT = PublishGen.publishInput();
    private static final ArcConfig DEFAULT_ARC_CONFIG = new ArcConfig(DEFAULT_ARC_TIMEOUT, DEFAULT_PUBLISH_STATUS_CHECK_RETRY_DELAY, DEFAULT_PUBLISH_STATUS_CHECK_RETRY_COUNT, DEFAULT_HTTP_REQUEST_RETRY_DELAY, DEFAULT_HTTP_REQUEST_RETRY_COUNT, DEFAULT_STAGING_TO_LIVE_DELAY);

    private static final String ARC_SERVER = "some.arc.url.com";
    private static final String ARC_ENVIRONMENT = "authoring-sandbox";
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final PublishResponse SUCCESS_PUBLISH_RESPONSE = mock(PublishResponse.class);

    private final UcaWriteService ucaWriteServiceMock = mock(UcaWriteService.class);
    private final Injector injector = Guice.createInjector(new MockArcPublishModule(ucaWriteServiceMock));
    private final TestRunner runner = TestRunners.newTestRunner(new ArcPublishProcessor(injector));

    private final ArgumentCaptor<ServerConfig> serverConfigArgumentCaptor = ArgumentCaptor.forClass(ServerConfig.class);

    @BeforeAll
    static void beforeAll() {
        OBJECT_MAPPER.registerModule(new GuavaModule());
        OBJECT_MAPPER.registerModule(new JavaTimeModule());
        when(SUCCESS_PUBLISH_RESPONSE.isSuccess()).thenReturn(true);
    }

    @BeforeEach
    void setUp() {
        runner.setProperty(ARC_SERVER_DESCRIPTOR, ARC_SERVER);
        runner.setProperty(ARC_ENVIRONMENT_DESCRIPTOR, ARC_ENVIRONMENT);
    }

    @ParameterizedTest
    @MethodSource("nonNegativeDescriptors")
    void shouldAllowNonNegativeValuesOnlyForDescriptor(PropertyDescriptor descriptor) {
        Stream<String> invalidValues = Stream.of("-1", "", " ", "\t\n\r", "foo");
        invalidValues.forEach(invalidValue -> {
            runner.setProperty(descriptor, invalidValue);

            runner.assertNotValid();
        });

        Stream<String> validValues = Stream.of("0", "1", Integer.toString(Integer.MAX_VALUE));
        validValues.forEach(validValue -> {
            runner.setProperty(descriptor, validValue);

            runner.assertValid();
        });
    }

    private static Stream<PropertyDescriptor> nonNegativeDescriptors() {
        return Stream.of(
                ARC_TIMEOUT_DESCRIPTOR,
                PUBLISH_STATUS_CHECK_RETRY_DELAY_DESCRIPTOR,
                PUBLISH_STATUS_CHECK_RETRY_COUNT_DESCRIPTOR,
                HTTP_REQUEST_RETRY_DELAY_DESCRIPTOR,
                HTTP_REQUEST_RETRY_COUNT_DESCRIPTOR,
                STAGING_TO_LIVE_DELAY_DESCRIPTOR
        );
    }

    @ParameterizedTest
    @MethodSource("nonBlankDescriptors")
    void shouldAllowNonBlankValuesOnlyForDescriptor(PropertyDescriptor descriptor) {
        runner.setProperty(descriptor, "");

        runner.assertNotValid();

        runner.setProperty(descriptor, " \t\n\r");

        runner.assertNotValid();

        runner.setProperty(descriptor, "foo");

        runner.assertValid();
    }

    private static Stream<PropertyDescriptor> nonBlankDescriptors() {
        return Stream.of(
                ARC_SERVER_DESCRIPTOR,
                ARC_ENVIRONMENT_DESCRIPTOR,
                SITE_DESCRIPTOR,
                USERNAME_DESCRIPTOR
        );
    }

    @ParameterizedTest
    @ValueSource(strings = {"authoring", "staging"})
    void shouldAllowStageDescriptorValues(String stage) {
        runner.setProperty(STAGE_DESCRIPTOR, stage);

        runner.assertValid();
    }

    @ParameterizedTest
    @ValueSource(strings = {"", " ", "\t\n\r", "foo"})
    void shouldNotAllowStageDescriptorValues(String invalidStage) {
        runner.setProperty(STAGE_DESCRIPTOR, invalidStage);

        runner.assertNotValid();
    }

    @ParameterizedTest
    @MethodSource("optionalDescriptors")
    void shouldAllowAbsentPropertyForDescriptor(PropertyDescriptor descriptor) {
        runner.removeProperty(descriptor);

        runner.assertValid();
    }

    private static Stream<PropertyDescriptor> optionalDescriptors() {
        return Stream.of(
                SITE_DESCRIPTOR,
                STAGE_DESCRIPTOR,
                USERNAME_DESCRIPTOR,
                ARC_TIMEOUT_DESCRIPTOR,
                PUBLISH_STATUS_CHECK_RETRY_DELAY_DESCRIPTOR,
                PUBLISH_STATUS_CHECK_RETRY_COUNT_DESCRIPTOR,
                HTTP_REQUEST_RETRY_DELAY_DESCRIPTOR,
                HTTP_REQUEST_RETRY_COUNT_DESCRIPTOR,
                STAGING_TO_LIVE_DELAY_DESCRIPTOR
        );
    }

    @ParameterizedTest
    @MethodSource("requiredDescriptors")
    void shouldNotAllowAbsentPropertyForDescriptor(PropertyDescriptor descriptor) {
        runner.removeProperty(descriptor);

        runner.assertNotValid();
    }

    private static Stream<PropertyDescriptor> requiredDescriptors() {
        return Stream.of(
                ARC_SERVER_DESCRIPTOR,
                ARC_ENVIRONMENT_DESCRIPTOR
        );
    }

    @ParameterizedTest
    @MethodSource("expressionLanguageAwareServerConfigDescriptors")
    void shouldResolveExpressionLanguageForDescriptor(PropertyDescriptor descriptor, Function<ServerConfig, Object> actualValueExtractor) {
        when(ucaWriteServiceMock.publishToArc(any(), any(), any(), anyBoolean(), any(), any())).thenReturn(mock(PublishResponse.class));
        runner.setProperty(descriptor, "${someAttribute}");
        runner.enqueue(toJson(VALID_PUBLISH_INPUT.withArcConfig(null)), ImmutableMap.of("someAttribute", "10"));

        runner.run();

        verify(ucaWriteServiceMock).publishToArc(any(), any(), any(), anyBoolean(), any(), serverConfigArgumentCaptor.capture());
        ServerConfig serverConfig = serverConfigArgumentCaptor.getValue();
        Object actualValue = actualValueExtractor.apply(serverConfig);
        if (Integer.class.equals(actualValue.getClass())) {
            assertThat(actualValue).isEqualTo(10);
        } else {
            assertThat(actualValue).isEqualTo(10L);
        }
    }

    private static Stream<Arguments> expressionLanguageAwareServerConfigDescriptors() {
        return Stream.of(
                Arguments.of(ARC_TIMEOUT_DESCRIPTOR, (Function<ServerConfig, Object>) ServerConfig::getArcTimeout),
                Arguments.of(PUBLISH_STATUS_CHECK_RETRY_DELAY_DESCRIPTOR, (Function<ServerConfig, Object>) ServerConfig::getPublishRetryDelay),
                Arguments.of(PUBLISH_STATUS_CHECK_RETRY_COUNT_DESCRIPTOR, (Function<ServerConfig, Object>) ServerConfig::getPublishRetryCount),
                Arguments.of(HTTP_REQUEST_RETRY_DELAY_DESCRIPTOR, (Function<ServerConfig, Object>) ServerConfig::getPostRetryDelay),
                Arguments.of(HTTP_REQUEST_RETRY_COUNT_DESCRIPTOR, (Function<ServerConfig, Object>) ServerConfig::getPostRetryCount),
                Arguments.of(STAGING_TO_LIVE_DELAY_DESCRIPTOR, (Function<ServerConfig, Object>) ServerConfig::getStagingToLiveDelay)
        );
    }

    @ParameterizedTest
    @MethodSource("nonExpressionLanguageServerConfigDescriptors")
    void shouldNotResolveExpressionLanguageForDescriptor(PropertyDescriptor descriptor, Function<ServerConfig, Object> actualValueExtractor) {
        when(ucaWriteServiceMock.publishToArc(any(), any(), any(), anyBoolean(), any(), any())).thenReturn(mock(PublishResponse.class));
        runner.setProperty(descriptor, "${someAttribute}");
        runner.enqueue(toJson(VALID_PUBLISH_INPUT.withArcConfig(null)), ImmutableMap.of("someAttribute", "10"));

        runner.run();

        verify(ucaWriteServiceMock).publishToArc(any(), any(), any(), anyBoolean(), any(), serverConfigArgumentCaptor.capture());
        ServerConfig serverConfig = serverConfigArgumentCaptor.getValue();
        assertThat(actualValueExtractor.apply(serverConfig)).isEqualTo("${someAttribute}");
    }

    private static Stream<Arguments> nonExpressionLanguageServerConfigDescriptors() {
        return Stream.of(
                Arguments.of(ARC_SERVER_DESCRIPTOR, (Function<ServerConfig, Object>) ServerConfig::getArcServer),
                Arguments.of(ARC_ENVIRONMENT_DESCRIPTOR, (Function<ServerConfig, Object>) ServerConfig::getArcEnvironment)
        );
    }

    @Test
    void shouldResolveSite() {
        when(ucaWriteServiceMock.publishToArc(any(), eq("some site"), any(), anyBoolean(), any(), any())).thenReturn(mock(PublishResponse.class));
        runner.setProperty(SITE_DESCRIPTOR, "${site}");
        runner.enqueue(toJson(VALID_PUBLISH_INPUT.withSite(null)), ImmutableMap.of("site", "some site"));

        runner.run();

        verify(ucaWriteServiceMock).publishToArc(any(), eq("some site"), any(), anyBoolean(), any(), any());
    }

    @ParameterizedTest
    @ValueSource(strings = {"authoring", "staging"})
    void shouldSetStage(String stage) {
        runner.setProperty(STAGE_DESCRIPTOR, stage);

        runner.assertValid();
    }

    @ParameterizedTest
    @ValueSource(strings = {"", " ", "foo", "${stage}"})
    void shouldNotAllowInvalidStage(String stage) {
        runner.setProperty(STAGE_DESCRIPTOR, stage);

        runner.assertNotValid();
    }

    @ParameterizedTest
    @MethodSource("invalidJsons")
    void shouldTransferToInputInvalidIfJsonInvalid(String problematicField, String problem, String invalidJson) {
        runner.enqueue(invalidJson);

        runner.run();

        runner.assertAllFlowFilesTransferred(INPUT_INVALID, 1);
        String expectedError = "merged publish input has " + problematicField + ' ' + problem;
        assertThat(getFlowFileExceptionMessage()).isEqualTo("Invalid input: " + expectedError);
        assertThat(getFlowFileExceptionStacktrace()).startsWith("java.lang.IllegalArgumentException: " + expectedError);
    }

    private static Stream<Arguments> invalidJsons() {
        return Stream.of(
                Arguments.of("site", "undefined", toJson(VALID_PUBLISH_INPUT.withSite(null))),
                Arguments.of("site", "empty", toJson(VALID_PUBLISH_INPUT.withSite(""))),
                Arguments.of("stage", "undefined", toJson(VALID_PUBLISH_INPUT.withStage(null))),
                Arguments.of("stage", "empty", toJson(VALID_PUBLISH_INPUT.withStage(""))),
                Arguments.of("username", "undefined", toJson(VALID_PUBLISH_INPUT.withUsername(null))),
                Arguments.of("username", "empty", toJson(VALID_PUBLISH_INPUT.withUsername(""))),
                Arguments.of("uuids", "undefined", toJson(VALID_PUBLISH_INPUT.withUuids(null))),
                Arguments.of("uuids", "empty", toJson(VALID_PUBLISH_INPUT.withUuids(ImmutableList.of())))
        );
    }

    @Test
    void shouldUseDefaultArcConfigIfNotSpecifiedInContentOfFlowFile() {
        when(ucaWriteServiceMock.publishToArc(any(), any(), any(), anyBoolean(), any(), any())).thenReturn(mock(PublishResponse.class));
        runner.enqueue(toJson(VALID_PUBLISH_INPUT.withArcConfig(null)));

        runner.run();

        verify(ucaWriteServiceMock).publishToArc(any(), any(), any(), anyBoolean(), any(), serverConfigArgumentCaptor.capture());
        ServerConfig serverConfig = serverConfigArgumentCaptor.getValue();
        assertThat(serverConfig.getPostRetryCount()).isEqualTo(DEFAULT_ARC_CONFIG.getHttpRequestRetryCount());
        assertThat(serverConfig.getPostRetryDelay()).isEqualTo(DEFAULT_ARC_CONFIG.getHttpRequestRetryDelay());
        assertThat(serverConfig.getPublishRetryCount()).isEqualTo(DEFAULT_ARC_CONFIG.getPublishStatusCheckRetryCount());
        assertThat(serverConfig.getPublishRetryDelay()).isEqualTo(DEFAULT_ARC_CONFIG.getPublishStatusCheckRetryDelay());
        assertThat(serverConfig.getArcTimeout()).isEqualTo(DEFAULT_ARC_CONFIG.getArcTimeout());
        assertThat(serverConfig.getStagingToLiveDelay()).isEqualTo(DEFAULT_ARC_CONFIG.getStagingToLiveDelay());
    }

    @ParameterizedTest
    @ValueSource(strings = {"true", "false"})
    void shouldTransferToPubsetCreationFailure(boolean isArcError) {
        PublishResponse failure = PublishGen.publishFailureBecauseOfPubsetCreationFailure(isArcError);
        when(ucaWriteServiceMock.publishToArc(any(), any(), any(), anyBoolean(), any(), any())).thenReturn(failure);
        runner.enqueue(toJson(VALID_PUBLISH_INPUT));

        runner.run();

        runner.assertAllFlowFilesTransferred(PUBLISH_FAILURE_PUBSET_CREATION, 1);
    }

    @Test
    void shouldCreateProcessorOutputByExtendInputWithPublishOutputFieldsInCaseOfSuccess() throws IOException {
        PublishResponse success = PublishGen.publishToLiveSuccess();
        when(ucaWriteServiceMock.publishToArc(any(), any(), any(), anyBoolean(), any(), any())).thenReturn(success);
        runner.enqueue(toJson(VALID_PUBLISH_INPUT));

        runner.run();

        PublishOutput publishOutput = extractPublishOutput(PUBLISH_SUCCESS);
        PublishOutputAssert.assertThat(publishOutput).containsPublishInput(VALID_PUBLISH_INPUT);
        PublishOutputAssert.assertThat(publishOutput).containsPublishResponse(success);
    }

    @Test
    void shouldNotIncludeNullFieldsInJson() {
        PublishResponse success = PublishGen.publishToLiveSuccess();
        when(ucaWriteServiceMock.publishToArc(any(), any(), any(), anyBoolean(), any(), any())).thenReturn(success);
        runner.enqueue(toJson(VALID_PUBLISH_INPUT));

        runner.run();

        MockFlowFile outputFlowFile = runner.getFlowFilesForRelationship(PUBLISH_SUCCESS).get(0);
        String content = new String(runner.getContentAsByteArray(outputFlowFile), UTF_8);
        assertThat(content).doesNotContain(":null");
    }

    @ParameterizedTest
    @ValueSource(strings = {"true", "false"})
    void shouldCreateProcessorOutputByExtendInputWithPublishOutputFieldsInCaseOfPubsetCreationFailure(boolean isArcError) throws IOException {
        PublishResponse failure = PublishGen.publishFailureBecauseOfPubsetCreationFailure(isArcError);
        when(ucaWriteServiceMock.publishToArc(any(), any(), any(), anyBoolean(), any(), any())).thenReturn(failure);
        runner.enqueue(toJson(VALID_PUBLISH_INPUT));

        runner.run();

        PublishOutput publishOutput = extractPublishOutput(PUBLISH_FAILURE_PUBSET_CREATION);
        PublishOutputAssert.assertThat(publishOutput).containsPublishInput(VALID_PUBLISH_INPUT);
        PublishOutputAssert.assertThat(publishOutput).containsPublishResponse(failure);
    }

    @ParameterizedTest
    @ValueSource(strings = {"true", "false"})
    void shouldCreateProcessorOutputByExtendInputWithPublishOutputFieldsInCaseOfFirstStagePublishRequestFailure(boolean isArcError) throws IOException {
        PublishResponse failure = PublishGen.publishFailureBecauseOfFirstStagePublishRequestFailure(isArcError);
        when(ucaWriteServiceMock.publishToArc(any(), any(), any(), anyBoolean(), any(), any())).thenReturn(failure);
        runner.enqueue(toJson(VALID_PUBLISH_INPUT));

        runner.run();

        // There is an inconsistency in arcops-ucacrud-processor project - if a stage fails because of publish request
        // failure, the failure code is PUBSET_CREATION_FAILED (and should be something like PUBLISH_REQUEST_CREATION_FAILED),
        // and hence the output from the processor is transferred to PUBLISH_FAILURE_PUBSET_CREATION:
        PublishOutput publishOutput = extractPublishOutput(PUBLISH_FAILURE_PUBSET_CREATION);
        PublishOutputAssert.assertThat(publishOutput).containsPublishInput(VALID_PUBLISH_INPUT);
        PublishOutputAssert.assertThat(publishOutput).containsPublishResponse(failure);
    }

    @Test
    void shouldCreateProcessorOutputByExtendInputWithPublishOutputFieldsInCaseOfCheckPublishStatusDraftRecordFailure() throws IOException {
        PublishResponse failure = PublishGen.publishFailureBecauseOfCheckPublishStatusDraftRecordFailure();
        when(ucaWriteServiceMock.publishToArc(any(), any(), any(), anyBoolean(), any(), any())).thenReturn(failure);
        runner.enqueue(toJson(VALID_PUBLISH_INPUT));

        runner.run();

        PublishOutput publishOutput = extractPublishOutput(PUBLISH_FAILURE_PUBSET_HAS_DRAFT_RECORDS);
        PublishOutputAssert.assertThat(publishOutput).containsPublishInput(VALID_PUBLISH_INPUT);
        PublishOutputAssert.assertThat(publishOutput).containsPublishResponse(failure);
    }

    @Test
    void shouldCreateProcessorOutputByExtendInputWithPublishOutputFieldsInCaseOfCheckPublishStatusFailure() throws IOException {
        PublishResponse failure = PublishGen.publishFailureBecauseOfCheckPublishStatusFailure();
        when(ucaWriteServiceMock.publishToArc(any(), any(), any(), anyBoolean(), any(), any())).thenReturn(failure);
        runner.enqueue(toJson(VALID_PUBLISH_INPUT));

        runner.run();

        PublishOutput publishOutput = extractPublishOutput(PUBLISH_FAILURE);
        PublishOutputAssert.assertThat(publishOutput).containsPublishInput(VALID_PUBLISH_INPUT);
        PublishOutputAssert.assertThat(publishOutput).containsPublishResponse(failure);
    }

    private PublishOutput extractPublishOutput(Relationship relationship) throws IOException {
        MockFlowFile processorOutput = runner.getFlowFilesForRelationship(relationship).get(0);
        return OBJECT_MAPPER.readValue(runner.getContentAsByteArray(processorOutput), PublishOutput.class);
    }

    private String getFlowFileExceptionMessage() {
        return runner.getFlowFilesForRelationship(ArcPublishProcessor.INPUT_INVALID).get(0).getAttribute(ATTR_EXCEPTION_MESSAGE);
    }

    private String getFlowFileExceptionStacktrace() {
        return runner.getFlowFilesForRelationship(ArcPublishProcessor.INPUT_INVALID).get(0).getAttribute(ATTR_EXCEPTION_STACKTRACE);
    }

    private static String toJson(Object obj) {
        try {
            return OBJECT_MAPPER.writeValueAsString(obj);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    @RequiredArgsConstructor
    private static class MockArcPublishModule extends AbstractModule {

        private final UcaWriteService ucaWriteServiceMock;

        @Override
        protected void configure() {
        }

        @Provides
        @Singleton
        UcaWriteService ucaWriteService() {
            return ucaWriteServiceMock;
        }
    }
}
