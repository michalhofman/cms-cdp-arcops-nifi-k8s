package com.viacom.arcops.nifi.publish;

import com.google.common.collect.ImmutableList;
import com.viacom.arcops.uca.*;
import uk.co.jemos.podam.api.PodamFactoryImpl;

import java.util.*;

import static java.util.Collections.emptyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class PublishGen {

    private static final String[] STAGES = new String[]{"authoring", "staging"};
    private static final Random RANDOM = new Random();
    private static final PodamFactoryImpl PODAM_FACTORY = new PodamFactoryImpl();

    static PublishInput publishInput() {
        return PODAM_FACTORY.manufacturePojoWithFullData(PublishInput.class).withStage(stage());
    }

    static PublishResponse publishFailureBecauseOfPubsetCreationFailure(boolean isArcError) {
        PublishResponse.CreatePublishSetResponse createPublishSetFailure = isArcError ? createPublishSetArcFailure() : createPublishSetNonArcFailure();
        String failureDescription = createPublishSetFailure.getFailureDescription().orElse(null);

        PublishResponse publishResponseMock = mock(PublishResponse.class);

        when(publishResponseMock.isSuccess()).thenReturn(false);
        when(publishResponseMock.getPublishSetResponse()).thenReturn(createPublishSetFailure);
        when(publishResponseMock.getPublishToArcStageResponses()).thenReturn(emptyList());
        when(publishResponseMock.getFailureCode()).thenReturn(UcaErrorCodes.PUBSET_CREATION_FAILED);
        when(publishResponseMock.getFailureDescription()).thenReturn(failureDescription);
        when(publishResponseMock.getFailureSummary()).thenReturn("createPublishSet failed " + string());

        return publishResponseMock;
    }

    /**
     * Returns a {@link PublishResponse} which corresponds to the value returned by
     * {@link com.viacom.arcops.uca.UcaWriteServiceImpl#publishToArc(Map, String, String, boolean, String, ServerConfig) publishToArc}
     * method when publish set is successfully created but first stage fails because of publish request failure.
     *
     * @return unsuccessful publish because of first stage (i.e. publishing to staging) publish request failure
     */
    static PublishResponse publishFailureBecauseOfFirstStagePublishRequestFailure(boolean isArcError) {
        PublishResponse.CreatePublishSetResponse createPublishSetSuccess = createPublishSetSuccess();
        PublishResponse.PublishToArcStage stageFailure = stageFailureBecauseOfPublishRequestFailure(Stage.AUTHORING, isArcError);
        String failureDescription = stageFailure.getCreatePublishRequestResponse().orElseThrow(IllegalStateException::new).getFailureDescription().orElse(null);

        PublishResponse publishResponseMock = mock(PublishResponse.class);

        when(publishResponseMock.isSuccess()).thenReturn(false);
        when(publishResponseMock.getPublishSetResponse()).thenReturn(createPublishSetSuccess);
        when(publishResponseMock.getPublishToArcStageResponses()).thenReturn(ImmutableList.of(stageFailure));
        when(publishResponseMock.getFailureCode()).thenReturn(UcaErrorCodes.PUBSET_CREATION_FAILED);
        when(publishResponseMock.getFailureDescription()).thenReturn(failureDescription);
        when(publishResponseMock.getFailureSummary()).thenReturn("createPublishRequest failed " + string());

        return publishResponseMock;
    }

    /**
     * Returns a {@link PublishResponse} which corresponds to the value returned by
     * {@link com.viacom.arcops.uca.UcaWriteServiceImpl#publishToArc(Map, String, String, boolean, String, ServerConfig) publishToArc}
     * method when publish set is successfully created but first stage fails because of {@link #checkPublishStatusDraftRecordFailure(String, String) draft record failure}.
     *
     * @return unsuccessful publish because of first stage (i.e. publishing to staging) draft record failure
     */
    static PublishResponse publishFailureBecauseOfCheckPublishStatusDraftRecordFailure() {
        PublishResponse.CreatePublishSetResponse createPublishSetSuccess = createPublishSetSuccess();
        PublishResponse.PublishToArcStage stageFailure = stageFailureBecauseOfCheckPublishStatusDraftRecordFailure(createPublishSetSuccess.getUuid().orElseThrow(IllegalStateException::new));
        String failureDescription = stageFailure.getCheckPublishStatusResponse().orElseThrow(IllegalStateException::new).getFailureDescription().orElse(null);
        String failureCode = stageFailure.getCheckPublishStatusResponse().orElseThrow(IllegalStateException::new).getFailureCode().orElse(null);

        PublishResponse publishResponseMock = mock(PublishResponse.class);

        when(publishResponseMock.isSuccess()).thenReturn(false);
        when(publishResponseMock.getPublishSetResponse()).thenReturn(createPublishSetSuccess);
        when(publishResponseMock.getPublishToArcStageResponses()).thenReturn(ImmutableList.of(stageFailure));
        when(publishResponseMock.getFailureCode()).thenReturn(failureCode);
        when(publishResponseMock.getFailureDescription()).thenReturn(failureDescription);
        when(publishResponseMock.getFailureSummary()).thenReturn("checkPublishStatus failed " + failureDescription);

        return publishResponseMock;
    }

    /**
     * Returns a {@link PublishResponse} which corresponds to the value returned by
     * {@link com.viacom.arcops.uca.UcaWriteServiceImpl#publishToArc(Map, String, String, boolean, String, ServerConfig) publishToArc}
     * method when publish set is successfully created but the second stage fails because of check publish request failure other than draft record failure.
     *
     * @return unsuccessful publish because of the second stage (i.e. publishing to live) check publish status failure
     */
    static PublishResponse publishFailureBecauseOfCheckPublishStatusFailure() {
        PublishResponse.CreatePublishSetResponse createPublishSetSuccess = createPublishSetSuccess();
        PublishResponse.PublishToArcStage stageSuccess = stageSuccess(Stage.AUTHORING);
        PublishResponse.PublishToArcStage stageFailure = stageFailureBecauseOfCheckPublishStatusFailure(Stage.STAGING);

        PublishResponse publishResponseMock = mock(PublishResponse.class);

        when(publishResponseMock.isSuccess()).thenReturn(false);
        when(publishResponseMock.getPublishSetResponse()).thenReturn(createPublishSetSuccess);
        when(publishResponseMock.getPublishToArcStageResponses()).thenReturn(ImmutableList.of(stageSuccess, stageFailure));
        when(publishResponseMock.getFailureCode()).thenReturn(UcaErrorCodes.PUBLISH_FAILED);
        when(publishResponseMock.getFailureDescription()).thenReturn(null);
        when(publishResponseMock.getFailureSummary()).thenReturn("checkPublishStatus failed");

        return publishResponseMock;
    }

    static PublishResponse publishToLiveSuccess() {
        PublishResponse.CreatePublishSetResponse createPublishSetSuccess = createPublishSetSuccess();
        PublishResponse.PublishToArcStage toStagingSuccess = stageSuccess(Stage.AUTHORING);
        PublishResponse.PublishToArcStage toLiveSuccess = stageSuccess(Stage.STAGING);

        PublishResponse publishResponseMock = mock(PublishResponse.class);

        when(publishResponseMock.isSuccess()).thenReturn(true);
        when(publishResponseMock.getPublishSetResponse()).thenReturn(createPublishSetSuccess);
        when(publishResponseMock.getPublishToArcStageResponses()).thenReturn(ImmutableList.of(toStagingSuccess, toLiveSuccess));
        when(publishResponseMock.getFailureCode()).thenReturn(null);
        when(publishResponseMock.getFailureDescription()).thenReturn(null);
        when(publishResponseMock.getFailureSummary()).thenReturn(null);

        return publishResponseMock;
    }

    /**
     * Returns a {@link PublishResponse.PublishToArcStage PublishToArcStage} which corresponds to the value returned by
     * {@link com.viacom.arcops.uca.UcaWriteServiceImpl#publishToArcStage(String, String, Stage, String, ServerConfig, boolean) publishToArcStage}
     * method in case of publish success.
     *
     * @return stage success
     */
    private static PublishResponse.PublishToArcStage stageSuccess(Stage stage) {
        PublishResponse.CreatePublishRequestResponse publishRequestSuccess = publishRequestSuccess();
        PublishResponse.CheckPublishStatusResponse checkPublishSuccess = checkPublishSuccess();

        PublishResponse.PublishToArcStage stageFailure = mock(PublishResponse.PublishToArcStage.class);
        when(stageFailure.getStage()).thenReturn(stage);
        when(stageFailure.getCreatePublishRequestResponse()).thenReturn(Optional.of(publishRequestSuccess));
        when(stageFailure.getCheckPublishStatusResponse()).thenReturn(Optional.of(checkPublishSuccess));
        return stageFailure;
    }

    /**
     * Returns a {@link PublishResponse.PublishToArcStage PublishToArcStage} which corresponds to the value returned by
     * {@link com.viacom.arcops.uca.UcaWriteServiceImpl#publishToArcStage(String, String, Stage, String, ServerConfig, boolean) publishToArcStage}
     * method in case that
     * {@link com.viacom.arcops.uca.UcaWriteServiceImpl#createPublishRequest(String, String, String, boolean, String, ServerConfig) createPublishRequest}
     * method (called inside <code>publishToArcStage</code> method) returns a
     * {@link PublishResponse.CreatePublishRequestResponse CreatePublishRequestResponse} whose
     * {@link PublishResponse.CreatePublishRequestResponse#success success} field is false.
     *
     * @return stage failure because of unsuccessful publish request
     */
    private static PublishResponse.PublishToArcStage stageFailureBecauseOfPublishRequestFailure(Stage stage, boolean isArcError) {
        PublishResponse.CreatePublishRequestResponse reason = isArcError ? publishRequestArcFailure() : publishRequestNonArcFailure();

        PublishResponse.PublishToArcStage stageFailure = mock(PublishResponse.PublishToArcStage.class);
        when(stageFailure.getStage()).thenReturn(stage);
        when(stageFailure.getCreatePublishRequestResponse()).thenReturn(Optional.of(reason));
        when(stageFailure.getCheckPublishStatusResponse()).thenReturn(Optional.empty());
        return stageFailure;
    }

    /**
     * Returns a {@link PublishResponse.PublishToArcStage PublishToArcStage} which corresponds to the value returned by
     * {@link com.viacom.arcops.uca.UcaWriteServiceImpl#publishToArcStage(String, String, Stage, String, ServerConfig, boolean) publishToArcStage}
     * method in case that
     * {@link com.viacom.arcops.uca.UcaWriteServiceImpl#checkPublishStatus(String, String, String, String, ServerConfig) checkPublishStatus}
     * method (called inside <code>publishToArcStage</code> method) returns a
     * {@link PublishResponse.CheckPublishStatusResponse CheckPublishStatusResponse} indicating draft record failure.
     *
     * @return unsuccessful stage because of draft record failure
     */
    private static PublishResponse.PublishToArcStage stageFailureBecauseOfCheckPublishStatusDraftRecordFailure(String publishSetUuid) {
        PublishResponse.CreatePublishRequestResponse publishRequestSuccess = publishRequestSuccess();
        PublishResponse.CheckPublishStatusResponse reason = checkPublishStatusDraftRecordFailure(
                publishSetUuid,
                publishRequestSuccess.getUuid().orElseThrow(IllegalStateException::new)
        );

        PublishResponse.PublishToArcStage stageFailure = mock(PublishResponse.PublishToArcStage.class);
        when(stageFailure.getStage()).thenReturn(Stage.AUTHORING);
        when(stageFailure.getCreatePublishRequestResponse()).thenReturn(Optional.of(publishRequestSuccess));
        when(stageFailure.getCheckPublishStatusResponse()).thenReturn(Optional.of(reason));
        return stageFailure;
    }

    /**
     * Returns a {@link PublishResponse.PublishToArcStage PublishToArcStage} which corresponds to the value returned by
     * {@link com.viacom.arcops.uca.UcaWriteServiceImpl#publishToArcStage(String, String, Stage, String, ServerConfig, boolean) publishToArcStage}
     * method in case that
     * {@link com.viacom.arcops.uca.UcaWriteServiceImpl#checkPublishStatus(String, String, String, String, ServerConfig) checkPublishStatus}
     * method (called inside <code>publishToArcStage</code> method) returns a
     * {@link PublishResponse.CheckPublishStatusResponse CheckPublishStatusResponse} indicating failure (i.e.
     * with {@link PublishResponse.CheckPublishStatusResponse#failure failure} field equal to true).
     *
     * @return unsuccessful stage because of failure other than draft record failure
     */
    private static PublishResponse.PublishToArcStage stageFailureBecauseOfCheckPublishStatusFailure(Stage stage) {
        PublishResponse.CreatePublishRequestResponse publishRequestSuccess = publishRequestSuccess();
        PublishResponse.CheckPublishStatusResponse reason = checkPublishStatusFailure(publishRequestSuccess.getUuid().orElseThrow(IllegalStateException::new));

        PublishResponse.PublishToArcStage stageFailure = mock(PublishResponse.PublishToArcStage.class);
        when(stageFailure.getStage()).thenReturn(stage);
        when(stageFailure.getCreatePublishRequestResponse()).thenReturn(Optional.of(publishRequestSuccess));
        when(stageFailure.getCheckPublishStatusResponse()).thenReturn(Optional.of(reason));
        return stageFailure;
    }

    /**
     * Returns a {@link PublishResponse.CreatePublishSetResponse CreatePublishSetResponse} which correspond to the
     * value returned by
     * {@link com.viacom.arcops.uca.UcaWriteServiceImpl#createPublishSet(Set, String, String, String, ServerConfig) createPublishSet}
     * method in case of 200 http status code.
     *
     * @return publish set creation success
     */
    private static PublishResponse.CreatePublishSetResponse createPublishSetSuccess() {
        PublishResponse.CreatePublishSetResponse success = mock(PublishResponse.CreatePublishSetResponse.class);
        return mockBaseRequestSuccess(success);
    }

    /**
     * Returns a {@link PublishResponse.CreatePublishRequestResponse CreatePublishRequestResponse} which correspond to the
     * value returned by
     * {@link com.viacom.arcops.uca.UcaWriteServiceImpl#createPublishRequest(String, String, String, boolean, String, ServerConfig)} createPublishRequest}
     * method in case of 200 http status code.
     *
     * @return publish request success
     */
    private static PublishResponse.CreatePublishRequestResponse publishRequestSuccess() {
        PublishResponse.CreatePublishRequestResponse success = mock(PublishResponse.CreatePublishRequestResponse.class);
        return mockBaseRequestSuccess(success);
    }

    /**
     * Returns a {@link PublishResponse.CreatePublishRequestResponse CreatePublishRequestResponse} which correspond to the
     * value returned by
     * {@link com.viacom.arcops.uca.UcaWriteServiceImpl#createPublishRequest(String, String, String, boolean, String, ServerConfig)} createPublishRequest}
     * method in case of 200 http status code.
     *
     * @return publish request success
     */
    private static PublishResponse.CheckPublishStatusResponse checkPublishSuccess() {
        PublishResponse.CheckPublishStatusResponse success = mock(PublishResponse.CheckPublishStatusResponse.class);
        return mockBaseRequestSuccess(success);
    }

    private static <T extends PublishResponse.BaseResponse> T mockBaseRequestSuccess(T success) {
        when(success.getHttpStatus()).thenReturn(httpOkStatus());
        when(success.isSuccess()).thenReturn(true);
        when(success.getUuid()).thenReturn(Optional.of(uuid()));
        when(success.getFailureCode()).thenReturn(Optional.empty());
        when(success.getFailureDescription()).thenReturn(Optional.empty());
        return success;
    }

    /**
     * Returns a {@link PublishResponse.CreatePublishSetResponse CreatePublishSetResponse} which correspond to the
     * value returned by
     * {@link com.viacom.arcops.uca.UcaWriteServiceImpl#createPublishSet(Set, String, String, String, ServerConfig) createPublishSet}
     * method in case of Arc error (i.e. when {@link ArcResponse#isArcError() isArcError} returns true).
     *
     * @return publish set creation Arc failure
     */
    private static PublishResponse.CreatePublishSetResponse createPublishSetArcFailure() {
        PublishResponse.CreatePublishSetResponse failure = mock(PublishResponse.CreatePublishSetResponse.class);
        return mockBaseRequestFailure(failure, true, null, UcaErrorCodes.PUBSET_CREATION_FAILED, "createPublishSet failed " + string());
    }

    /**
     * Returns a {@link PublishResponse.CreatePublishSetResponse CreatePublishSetResponse} which correspond to the
     * value returned by
     * {@link com.viacom.arcops.uca.UcaWriteServiceImpl#createPublishSet(Set, String, String, String, ServerConfig) createPublishSet}
     * method in case of http status different than 200 but without Arc error (normally, if present, Arc error is included in response body).
     *
     * @return publish set creation non-Arc failure
     */
    private static PublishResponse.CreatePublishSetResponse createPublishSetNonArcFailure() {
        PublishResponse.CreatePublishSetResponse failure = mock(PublishResponse.CreatePublishSetResponse.class);
        return mockBaseRequestFailure(failure, true, null, null, null);
    }

    /**
     * Returns a {@link PublishResponse.CreatePublishRequestResponse CreatePublishRequestResponse} which correspond to the
     * value returned by
     * {@link com.viacom.arcops.uca.UcaWriteServiceImpl#createPublishRequest(String, String, String, boolean, String, ServerConfig) createPublishRequest}
     * method in case of Arc error (i.e. when {@link ArcResponse#isArcError() isArcError} returns true).
     *
     * @return publish request Arc failure
     */
    private static PublishResponse.CreatePublishRequestResponse publishRequestArcFailure() {
        PublishResponse.CreatePublishRequestResponse failure = mock(PublishResponse.CreatePublishRequestResponse.class);
        return mockBaseRequestFailure(failure, true, null, UcaErrorCodes.PUBLISH_FAILED, "error summary: " + string());
    }

    /**
     * Returns a {@link PublishResponse.CreatePublishRequestResponse CreatePublishRequestResponse} which correspond to the
     * value returned by
     * {@link com.viacom.arcops.uca.UcaWriteServiceImpl#createPublishRequest(String, String, String, boolean, String, ServerConfig) createPublishRequest}
     * method in case of http status different than 200 but without Arc error (normally, if present, Arc error is included in response body).
     *
     * @return publish request non-Arc failure
     */
    private static PublishResponse.CreatePublishRequestResponse publishRequestNonArcFailure() {
        PublishResponse.CreatePublishRequestResponse failure = mock(PublishResponse.CreatePublishRequestResponse.class);
        return mockBaseRequestFailure(failure, true, null, null, null);
    }

    /**
     * Returns a {@link PublishResponse.CheckPublishStatusResponse CheckPublishStatusResponse} which correspond to the
     * value returned by
     * {@link com.viacom.arcops.uca.UcaWriteServiceImpl#checkPublishStatus(String, String, String, String, ServerConfig) checkPublishStatus}
     * method in case of draft record error.
     *
     * @return check publish status draft record failure
     */
    private static PublishResponse.CheckPublishStatusResponse checkPublishStatusDraftRecordFailure(String publishSetUuid, String publishRequestUuid) {
        PublishResponse.CheckPublishStatusResponse failure = mock(PublishResponse.CheckPublishStatusResponse.class);
        return mockBaseRequestFailure(failure, false, publishRequestUuid, UcaErrorCodes.PUBSET_HAS_DRAFT_RECORDS, "Publish Request '" + publishRequestUuid + "' could not be executed. Reason: Publish Set '" + publishSetUuid + "' contains DRAFT records");
    }

    /**
     * Returns a {@link PublishResponse.CheckPublishStatusResponse CheckPublishStatusResponse} which correspond to the
     * value returned by
     * {@link com.viacom.arcops.uca.UcaWriteServiceImpl#checkPublishStatus(String, String, String, String, ServerConfig) checkPublishStatus}
     * method in case of check status response indicates failure but not because of draft records.
     *
     * @return check publish status failure
     */
    private static PublishResponse.CheckPublishStatusResponse checkPublishStatusFailure(String publishRequestUuid) {
        PublishResponse.CheckPublishStatusResponse failure = mock(PublishResponse.CheckPublishStatusResponse.class);
        return mockBaseRequestFailure(failure, false, publishRequestUuid, null, null);
    }

    private static <T extends PublishResponse.BaseResponse> T mockBaseRequestFailure(T failure, boolean isHttpError, String uuid, String failureCode, String failureDescription) {
        when(failure.getHttpStatus()).thenReturn(isHttpError ? httpClientErrorStatus() : 200);
        when(failure.isSuccess()).thenReturn(false);
        when(failure.getUuid()).thenReturn(Optional.ofNullable(uuid));
        when(failure.getFailureCode()).thenReturn(Optional.ofNullable(failureCode));
        when(failure.getFailureDescription()).thenReturn(Optional.ofNullable(failureDescription));
        return failure;
    }

    private static String stage() {
        return STAGES[RANDOM.nextInt(STAGES.length)];
    }

    private static String string() {
        return PODAM_FACTORY.manufacturePojo(String.class);
    }

    private static int httpOkStatus() {
        return RANDOM.nextInt(100) + 200;
    }

    private static int httpClientErrorStatus() {
        return RANDOM.nextInt(100) + 400;
    }

    private static String uuid() {
        return UUID.randomUUID().toString();
    }
}
