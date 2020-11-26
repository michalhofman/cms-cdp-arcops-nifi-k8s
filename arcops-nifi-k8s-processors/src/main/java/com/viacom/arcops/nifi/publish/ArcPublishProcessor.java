package com.viacom.arcops.nifi.publish;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import com.google.inject.Module;
import com.viacom.arcops.nifi.GuiceConfiguredProcessor;
import com.viacom.arcops.uca.PublishResponse;
import com.viacom.arcops.uca.ServerConfig;
import com.viacom.arcops.uca.UcaWriteService;
import lombok.extern.slf4j.Slf4j;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.viacom.arcops.nifi.NiFiProperties.ATTR_EXCEPTION_MESSAGE;
import static com.viacom.arcops.nifi.NiFiProperties.ATTR_EXCEPTION_STACKTRACE;
import static com.viacom.arcops.nifi.NiFiUtils.*;
import static java.lang.System.currentTimeMillis;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;
import static org.apache.commons.lang3.exception.ExceptionUtils.getStackTrace;
import static org.apache.nifi.processor.util.StandardValidators.NON_BLANK_VALIDATOR;
import static org.apache.nifi.processor.util.StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR;

@EventDriven
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"ARC", "WON", "PTS"})
@CapabilityDescription("CableLabsIngestionProcessor is used to ingest CableLabsMessage and returns Ingestion status and its response")
@Slf4j
public class ArcPublishProcessor extends GuiceConfiguredProcessor {

    static final long DEFAULT_ARC_TIMEOUT = 60000L;
    static final long DEFAULT_PUBLISH_STATUS_CHECK_RETRY_DELAY = 5000L;
    static final int DEFAULT_PUBLISH_STATUS_CHECK_RETRY_COUNT = 30;
    static final long DEFAULT_HTTP_REQUEST_RETRY_DELAY = 1000L;
    static final int DEFAULT_HTTP_REQUEST_RETRY_COUNT = 10;
    static final long DEFAULT_STAGING_TO_LIVE_DELAY = 30000L;

    static final PropertyDescriptor ARC_SERVER_DESCRIPTOR = new PropertyDescriptor.Builder()
            .name("publish.arc.server")
            .description("Arc server name")
            .required(true)
            .expressionLanguageSupported(false)
            .addValidator(NON_BLANK_VALIDATOR)
            .build();

    static final PropertyDescriptor ARC_ENVIRONMENT_DESCRIPTOR = new PropertyDescriptor.Builder()
            .name("publish.arc.environment")
            .description("Arc environment")
            .required(true)
            .expressionLanguageSupported(false)
            .addValidator(NON_BLANK_VALIDATOR)
            .build();

    static final PropertyDescriptor SITE_DESCRIPTOR = new PropertyDescriptor.Builder()
            .name("publish.site")
            .description("Site to publish to")
            .required(false)
            .expressionLanguageSupported(true)
            .addValidator(NON_BLANK_VALIDATOR)
            .build();

    static final PropertyDescriptor STAGE_DESCRIPTOR = new PropertyDescriptor.Builder()
            .name("publish.stage")
            .description("Stage to publish from: authoring (publishing from authoring to staging) or staging (publishing from staging to live)")
            .required(false)
            .expressionLanguageSupported(false)
            .allowableValues("authoring", "staging")
            .build();

    static final PropertyDescriptor USERNAME_DESCRIPTOR = new PropertyDescriptor.Builder()
            .name("publish.username")
            .description("Username to publish on behalf of")
            .required(false)
            .expressionLanguageSupported(true)
            .addValidator(NON_BLANK_VALIDATOR)
            .build();

    static final PropertyDescriptor UNPUBLISH_DESCRIPTOR = new PropertyDescriptor.Builder()
            .name("publish.unpublish")
            .description("Flag to toggle betweent publish and unpublish")
            .required(false)
            .expressionLanguageSupported(true)
            .addValidator(NON_BLANK_VALIDATOR)
            .build();

    static final PropertyDescriptor ARC_TIMEOUT_DESCRIPTOR = new PropertyDescriptor.Builder()
            .name("publish.arcTimeout")
            .description("Timeout in milliseconds used when talking to Arc (used as connection timeout as well as read timeout). 0 means infinity.")
            .required(false)
            .expressionLanguageSupported(true)
            .addValidator(NON_NEGATIVE_INTEGER_VALIDATOR)
            .defaultValue(Long.toString(DEFAULT_ARC_TIMEOUT))
            .build();

    static final PropertyDescriptor PUBLISH_STATUS_CHECK_RETRY_DELAY_DESCRIPTOR = new PropertyDescriptor.Builder()
            .name("publish.statusCheckRetry.delay")
            .description("Delay in milliseconds between two consecutive checks whether the publish succeeded")
            .required(false)
            .expressionLanguageSupported(true)
            .addValidator(NON_NEGATIVE_INTEGER_VALIDATOR)
            .defaultValue(Long.toString(DEFAULT_PUBLISH_STATUS_CHECK_RETRY_DELAY))
            .build();

    static final PropertyDescriptor PUBLISH_STATUS_CHECK_RETRY_COUNT_DESCRIPTOR = new PropertyDescriptor.Builder()
            .name("publish.statusCheckRetry.count")
            .description("Number of retries when checking whether the publish succeeded before assuming failure")
            .required(false)
            .expressionLanguageSupported(true)
            .addValidator(NON_NEGATIVE_INTEGER_VALIDATOR)
            .defaultValue(Integer.toString(DEFAULT_PUBLISH_STATUS_CHECK_RETRY_COUNT))
            .build();

    static final PropertyDescriptor HTTP_REQUEST_RETRY_DELAY_DESCRIPTOR = new PropertyDescriptor.Builder()
            .name("publish.httpRequestRetry.delay")
            .description("Delay in milliseconds before retying an http request (a publish request or a publish status check request)")
            .required(false)
            .expressionLanguageSupported(true)
            .addValidator(NON_NEGATIVE_INTEGER_VALIDATOR)
            .defaultValue(Long.toString(DEFAULT_HTTP_REQUEST_RETRY_DELAY))
            .build();

    static final PropertyDescriptor HTTP_REQUEST_RETRY_COUNT_DESCRIPTOR = new PropertyDescriptor.Builder()
            .name("publish.httpRequestRetry.count")
            .description("Number of retries for an http request (a publish request or publish status check request)")
            .required(false)
            .expressionLanguageSupported(true)
            .addValidator(NON_NEGATIVE_INTEGER_VALIDATOR)
            .defaultValue(Integer.toString(DEFAULT_HTTP_REQUEST_RETRY_COUNT))
            .build();

    static final PropertyDescriptor STAGING_TO_LIVE_DELAY_DESCRIPTOR = new PropertyDescriptor.Builder()
            .name("publish.stagingToLiveDelay")
            .description("Delay in milliseconds between the end of the successful publishing to staging (i.e. the end of the publishing status check that indicates success) and the beginning of the publishing to live")
            .required(false)
            .expressionLanguageSupported(true)
            .addValidator(NON_NEGATIVE_INTEGER_VALIDATOR)
            .defaultValue(Long.toString(DEFAULT_STAGING_TO_LIVE_DELAY))
            .build();

    static final Relationship INPUT_INVALID = new Relationship.Builder()
            .name("inputInvalid")
            .build();
    static final Relationship PUBLISH_SUCCESS = new Relationship.Builder()
            .name("publishSuccess")
            .build();
    static final Relationship PUBLISH_FAILURE_PUBSET_CREATION = new Relationship.Builder()
            .name("publishFailurePubsetCreation")
            .build();
    static final Relationship PUBLISH_FAILURE = new Relationship.Builder()
            .name("publishFailure")
            .build();
    static final Relationship PUBLISH_FAILURE_ARC_TIMEOUT = new Relationship.Builder()
            .name("publishFailureArcTimeout")
            .build();
    static final Relationship PUBLISH_FAILURE_ARC_INTERNAL_SERVER_ERROR = new Relationship.Builder()
            .name("publishFailureArcInternalServerError")
            .build();
    static final Relationship PUBLISH_FAILURE_PUBSET_HAS_DRAFT_RECORDS = new Relationship.Builder()
            .name("publishFailurePubsetHasDraftRecords")
            .build();
    static final Relationship PUBLISH_FAILURE_CHECK_PUBLISH_STATUS_ARC_TIMEOUT = new Relationship.Builder()
            .name("publishFailureCheckPublishStatusArcTimeout")
            .build();

    private final ObjectMapper objectMapper;

    private final AtomicReference<UcaWriteService> ucaWriteService = new AtomicReference<>();
    private final AtomicReference<String> arcServer = new AtomicReference<>();
    private final AtomicReference<String> arcEnvironment = new AtomicReference<>();
    private final AtomicReference<String> transitUri = new AtomicReference<>();

    @SuppressWarnings({"WeakerAccess"})
    public ArcPublishProcessor() {
        objectMapper = new ObjectMapper();
        objectMapper.registerModule(new GuavaModule());
        objectMapper.registerModule(new JavaTimeModule());
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        objectMapper.setSerializationInclusion(NON_NULL);
        if (log.isDebugEnabled()) {
            objectMapper.setDefaultPrettyPrinter(new DefaultPrettyPrinter());
        }
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        arcServer.set(context.getProperty(ARC_SERVER_DESCRIPTOR).getValue());
        arcEnvironment.set(context.getProperty(ARC_ENVIRONMENT_DESCRIPTOR).getValue());
        transitUri.set(arcEnvironment.get() + '@' + arcServer.get());
        ucaWriteService.set(initializeInjector(context).getInstance(UcaWriteService.class));


        log.info("Scheduled {} with arcServer={} and arcEnvironment={}", ArcPublishProcessor.class.getSimpleName(), arcServer.get(), arcEnvironment.get());

        FlowFile flowFile = session.get();

        if (flowFile == null) {
            return;
        }

        PublishInput publishInput;
        PublishInput flowFilePublishInput;
        try {
            flowFilePublishInput = extractPublishConfigFromContent(session, flowFile);
            PublishInput processorPublishInput = extractPublishConfigFromProcessorProperties(context, flowFile);
            publishInput = merge(flowFilePublishInput, processorPublishInput);
        } catch (RuntimeException e) {
            log.warn("Invalid input", e);
            transferWithException(INPUT_INVALID, session, flowFile, null, "Invalid input", e);
            return;
        }
        log.debug("Publish input: {}", publishInput);

        long start = currentTimeMillis();
        PublishResponse publishResponse = publish(arcServer.get(), arcEnvironment.get(), publishInput);
        if (publishResponse.isSuccess()) {
            session.getProvenanceReporter().send(flowFile, transitUri.get(), publishInput.toString(), currentTimeMillis() - start);
            log.debug("Successfully published: {}", publishResponse);
        } else {
            log.debug("Publish failed: {}", publishResponse);
        }

        transfer(flowFile, session, flowFilePublishInput, publishResponse);
    }

    private void transfer(FlowFile flowFile, ProcessSession session, PublishInput publishInputFromContent, PublishResponse publishResponse) {
        PublishOutput publishOutput;
        Relationship destinationIfException = publishResponse.isSuccess() ? PUBLISH_SUCCESS : PUBLISH_FAILURE;
        try {
            publishOutput = new PublishOutput(publishInputFromContent, publishResponse);
        } catch (RuntimeException e) {
            transferWithException(destinationIfException, session, flowFile, publishResponse, "Exception while building PublishOutput from PublishResponse", e);
            return;
        }
        String flowFileContent;
        try {
            flowFileContent = objectMapper.writeValueAsString(publishOutput);
        } catch (JsonProcessingException e) {
            transferWithException(destinationIfException, session, flowFile, publishOutput, "Exception while writing PublishOutput as json", e);
            return;
        }
        FlowFile outputFlowFile = stringToNewFlowFile(flowFileContent, session, flowFile);
        Relationship destination = publishResponse.isSuccess() ? PUBLISH_SUCCESS : mapFailureCodeToRelationship(publishOutput.getFailureCode());
        log.debug("Transferring to {}: {}", destination, flowFileContent);
        session.transfer(outputFlowFile, destination);
    }

    private Relationship mapFailureCodeToRelationship(String failureCode) {
        if (isNullOrEmpty(failureCode)) {
            return PUBLISH_FAILURE;
        }

        switch (failureCode) {
            case "E003":
                return PUBLISH_FAILURE_PUBSET_CREATION;
            case "E004":
                return PUBLISH_FAILURE;
            case "E005":
                return PUBLISH_FAILURE_ARC_TIMEOUT;
            case "E006":
                return PUBLISH_FAILURE_ARC_INTERNAL_SERVER_ERROR;
            case "E011":
                return PUBLISH_FAILURE_PUBSET_HAS_DRAFT_RECORDS;
            case "E012":
                return PUBLISH_FAILURE_CHECK_PUBLISH_STATUS_ARC_TIMEOUT;
            default:
                return PUBLISH_FAILURE;
        }
    }

    private void transferWithException(Relationship relationship, ProcessSession session, FlowFile flowFile, Object newFlowFileContent, String exceptionDescription, Exception e) {
        FlowFile output = newFlowFileContent != null ? stringToNewFlowFile(newFlowFileContent.toString(), session, flowFile) : flowFile;
        session.putAttribute(output, ATTR_EXCEPTION_MESSAGE, exceptionDescription + ": " + (!isNullOrEmpty(e.getMessage()) ? e.getMessage() : e.toString()));
        session.putAttribute(output, ATTR_EXCEPTION_STACKTRACE, getStackTrace(e));
        session.transfer(output, relationship);
    }

    private PublishInput extractPublishConfigFromContent(ProcessSession session, FlowFile flowFile) {
        String flowFileContent = "<content unavailable>";
        try {
            flowFileContent = loadFlowFileToString(flowFile, session);
            PublishInput publishInput = objectMapper.readValue(flowFileContent, PublishInput.class);
            log.debug("PublishInput read from the incoming flow file's content: {}", publishInput);
            return publishInput;
        } catch (Exception e) {
            throw new RuntimeException("PublishInput can't be read from the incoming flow file's content: " + e + ", content: " + flowFileContent, e);
        }
    }

    private PublishInput extractPublishConfigFromProcessorProperties(ProcessContext context, FlowFile flowFile) {
        Map<String, String> attributes = flowFile.getAttributes();
        PublishInput publishInput = new PublishInput(
                null,
                resolveNifiProperty(context, SITE_DESCRIPTOR, attributes),
                resolveNifiProperty(context, STAGE_DESCRIPTOR, attributes),
                new ArcConfig(
                        Long.parseLong(resolveNifiProperty(context, ARC_TIMEOUT_DESCRIPTOR, attributes)),
                        Long.parseLong(resolveNifiProperty(context, PUBLISH_STATUS_CHECK_RETRY_DELAY_DESCRIPTOR, attributes)),
                        Integer.parseInt(resolveNifiProperty(context, PUBLISH_STATUS_CHECK_RETRY_COUNT_DESCRIPTOR, attributes)),
                        Long.parseLong(resolveNifiProperty(context, HTTP_REQUEST_RETRY_DELAY_DESCRIPTOR, attributes)),
                        Integer.parseInt(resolveNifiProperty(context, HTTP_REQUEST_RETRY_COUNT_DESCRIPTOR, attributes)),
                        Long.parseLong(resolveNifiProperty(context, STAGING_TO_LIVE_DELAY_DESCRIPTOR, attributes))
                ),
                resolveNifiProperty(context, USERNAME_DESCRIPTOR, attributes),
                Boolean.parseBoolean(resolveNifiProperty(context, UNPUBLISH_DESCRIPTOR, attributes))
        );
        log.debug("PublishInput derived from ArcPublishProcessor's properties resolved against incoming flow file's attributes: {}", publishInput);
        return publishInput;
    }

    private PublishInput merge(PublishInput flowFilePublishInput, PublishInput processorPublishInput) {
        PublishInput result = flowFilePublishInput.merge(processorPublishInput);
        log.debug("Merged PublishInput: {}", result);
        validateMergedPublishInput(result);
        return result;
    }

    private PublishResponse publish(String arcServer, String arcEnvironment, PublishInput config) {
        try {
            return ucaWriteService.get().publishToArc(
                    config.getUuids().stream().collect(toMap(identity(), key -> "")),
                    config.getSite(),
                    config.getStage(),
                    config.getUnpublish(),
                    config.getUsername(),
                    createServerConfig(arcServer, arcEnvironment, config.getArcConfig())
            );
        } catch (RuntimeException e) {
            log.info("Unexpected exception during publishing", e);
            throw new ProcessException("Unexpected exception during publishing: " + e, e);
        }
    }

    private ServerConfig createServerConfig(String arcServer, String arcEnvironment, ArcConfig arcConfig) {
        return new ServerConfig(
                arcServer,
                arcEnvironment,
                arcConfig.getHttpRequestRetryCount(),
                arcConfig.getHttpRequestRetryDelay(),
                arcConfig.getPublishStatusCheckRetryCount(),
                arcConfig.getPublishStatusCheckRetryDelay(),
                arcConfig.getArcTimeout(),
                arcConfig.getStagingToLiveDelay()
        );
    }

    private void validateMergedPublishInput(PublishInput input) {
        checkDefined(input.getUuids(), "uuids");
        checkNotEmpty(input.getUuids().isEmpty(), "uuids");
        checkNotEmpty(input.getStage(), "stage");
        checkNotEmpty(input.getSite(), "site");
        checkNotEmpty(input.getUsername(), "username");
        checkDefined(input.getArcConfig(), "arcConfig");
        checkDefined(input.getArcConfig().getArcTimeout(), "arcConfig.arcTimeout");
        checkDefined(input.getArcConfig().getStagingToLiveDelay(), "arcConfig.authoringToStagingDelay");
        checkDefined(input.getArcConfig().getHttpRequestRetryCount(), "arcConfig.httpRequestRetryCount");
        checkDefined(input.getArcConfig().getHttpRequestRetryDelay(), "arcConfig.httpRequestRetryDelay");
        checkDefined(input.getArcConfig().getPublishStatusCheckRetryCount(), "arcConfig.publishStatusCheckRetryCount");
        checkDefined(input.getArcConfig().getPublishStatusCheckRetryDelay(), "arcConfig.publishStatusCheckRetryDelay");
    }

    private void checkNotEmpty(String s, String name) {
        checkDefined(s, name);
        checkNotEmpty(s.isEmpty(), name);
    }

    private void checkNotEmpty(boolean isEmpty, String name) {
        checkArgument(!isEmpty, "merged publish input has %s empty", name);
    }

    private void checkDefined(Object obj, String name) {
        checkArgument(obj != null, "merged publish input has %s undefined", name);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return Sets.newHashSet(
                INPUT_INVALID,
                PUBLISH_SUCCESS,
                PUBLISH_FAILURE_PUBSET_CREATION,
                PUBLISH_FAILURE,
                PUBLISH_FAILURE_ARC_TIMEOUT,
                PUBLISH_FAILURE_ARC_INTERNAL_SERVER_ERROR,
                PUBLISH_FAILURE_PUBSET_HAS_DRAFT_RECORDS,
                PUBLISH_FAILURE_CHECK_PUBLISH_STATUS_ARC_TIMEOUT
        );
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return ImmutableList.of(
                ARC_SERVER_DESCRIPTOR,
                ARC_ENVIRONMENT_DESCRIPTOR,
                SITE_DESCRIPTOR,
                STAGE_DESCRIPTOR,
                USERNAME_DESCRIPTOR,
                UNPUBLISH_DESCRIPTOR,
                ARC_TIMEOUT_DESCRIPTOR,
                PUBLISH_STATUS_CHECK_RETRY_DELAY_DESCRIPTOR,
                PUBLISH_STATUS_CHECK_RETRY_COUNT_DESCRIPTOR,
                HTTP_REQUEST_RETRY_DELAY_DESCRIPTOR,
                HTTP_REQUEST_RETRY_COUNT_DESCRIPTOR,
                STAGING_TO_LIVE_DELAY_DESCRIPTOR
        );
    }

    @Override
    protected Function<Map<String, String>, List<Module>> getModulesCreator() {
        return properties -> ImmutableList.of(new ArcPublishModule());
    }
}
