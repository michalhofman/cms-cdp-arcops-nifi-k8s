package com.viacom.arcops.nifi.publish;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedMap;
import com.viacom.arcops.uca.PublishResponse;
import com.viacom.arcops.uca.PublishResponse.BaseResponse;
import com.viacom.arcops.uca.PublishResponse.CreatePublishRequestResponse;
import com.viacom.arcops.uca.PublishResponse.PublishToArcStage;
import lombok.Getter;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

import static java.time.Instant.now;
import static java.util.stream.Collectors.toMap;

@Getter
public class PublishOutput extends PublishInput {

    private final Instant timestamp = now();
    private final boolean success;
    private final String failureSummary;
    private final String failureDescription;
    private final String failureCode;
    private final HttpResponse createPublishSetResponse;
    private final ImmutableSortedMap<String, HttpResponse> stageToHttpResponseMap;

    @JsonCreator
    @SuppressWarnings("unused")
    public PublishOutput(
            @JsonProperty("uuids") ImmutableList<String> uuids,
            @JsonProperty("site") String site,
            @JsonProperty("stage") String stage,
            @JsonProperty("arcConfig") ArcConfig arcConfig,
            @JsonProperty("username") String username,
            @JsonProperty("unpublish") Boolean unpublish,
            @JsonProperty("success") boolean success,
            @JsonProperty("failureSummary") String failureSummary,
            @JsonProperty("failureDescription") String failureDescription,
            @JsonProperty("failureCode") String failureCode,
            @JsonProperty("createPublishSetResponse") HttpResponse createPublishSetResponse,
            @JsonProperty("stageToHttpResponseMap") ImmutableSortedMap<String, HttpResponse> stageToHttpResponseMap
    ) {
        super(uuids, site, stage, arcConfig, username, unpublish);
        this.success = success;
        this.failureSummary = failureSummary;
        this.failureDescription = failureDescription;
        this.failureCode = failureCode;
        this.createPublishSetResponse = createPublishSetResponse;
        this.stageToHttpResponseMap = stageToHttpResponseMap;
    }

    public PublishOutput(PublishInput config, PublishResponse publishResponse) {
        super(config.getUuids(), config.getSite(), config.getStage(), config.getArcConfig(), config.getUsername(), config.getUnpublish());

        success = publishResponse.isSuccess();
        if (success) {
            failureSummary = null;
            failureDescription = null;
            failureCode = null;
        } else {
            failureSummary = publishResponse.getFailureSummary();
            failureDescription = publishResponse.getFailureDescription();
            failureCode = publishResponse.getFailureCode();
        }
        createPublishSetResponse = publishResponse.getPublishSetResponse() != null
                ? createResponse(publishResponse.getPublishSetResponse().getUuid().orElse(null), publishResponse.getPublishSetResponse())
                : null;

        if (publishResponse.getPublishToArcStageResponses() != null) {
            Map<String, HttpResponse> map = publishResponse.getPublishToArcStageResponses().stream().collect(toMap(
                    stageResponse -> stageResponse.getStage().getStageName(),
                    this::extract,
                    (response1, response2) -> {
                        throw new IllegalArgumentException("Duplicate stage in " + publishResponse);
                    },
                    LinkedHashMap::new
            ));
            stageToHttpResponseMap = ImmutableSortedMap.copyOf(map);
        } else {
            stageToHttpResponseMap = null;
        }
    }

    /**
     * Returns {@link HttpResponse} created from the most recent {@link BaseResponse} present in the given
     * <code>stage</code>. The most recent <code>BaseResponse</code> is
     * {@link PublishToArcStage#checkPublishStatusResponse stage.checkPublishStatusResponse}
     * if it is present, otherwise it is
     * {@link PublishToArcStage#createPublishRequestResponse stage.createPublishRequestResponse}.
     *
     * @param stage <code>PublishToArcStage</code> to extract <code>HttpResponse</code> from
     * @return HttpResponse created from the most recent {@link BaseResponse} present in the given <code>stage</code>
     */
    private HttpResponse extract(PublishToArcStage stage) {
        Optional<String> uuid;
        BaseResponse response;
        if (stage.getCheckPublishStatusResponse().isPresent()) {
            uuid = stage.getCreatePublishRequestResponse().flatMap(CreatePublishRequestResponse::getUuid);
            response = stage.getCheckPublishStatusResponse().get();
        } else if (stage.getCreatePublishRequestResponse().isPresent()) {
            uuid = Optional.empty();
            response = stage.getCreatePublishRequestResponse().get();
        } else {
            throw new IllegalArgumentException("No response present for " + stage.getStage().getStageName());
        }
        return createResponse(uuid.orElse(null), response);
    }

    private HttpResponse createResponse(String uuid, BaseResponse response) {
        return new HttpResponse(
                response.getHttpStatus(),
                response.isSuccess(),
                uuid,
                response.getFailureCode().orElse(null),
                response.getFailureDescription().orElse(null)
        );
    }
}
