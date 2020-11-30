package com.viacom.arcops.nifi.publish;

import com.viacom.arcops.uca.PublishResponse;
import org.assertj.core.api.AbstractAssert;
import org.assertj.core.api.Assertions;

import java.util.Map;

import static com.viacom.arcops.uca.Stage.AUTHORING;
import static com.viacom.arcops.uca.Stage.STAGING;

public class PublishOutputAssert extends AbstractAssert<PublishOutputAssert, PublishOutput> {

    public PublishOutputAssert(PublishOutput actual) {
        super(actual, PublishOutputAssert.class);
    }

    public static PublishOutputAssert assertThat(PublishOutput actual) {
        return new PublishOutputAssert(actual);
    }

    public PublishOutputAssert containsPublishInput(PublishInput publishInput) {
        Assertions.assertThat(actual).isEqualToComparingOnlyGivenFields(publishInput, "uuids", "site", "stage", "arcConfig", "username");

        return this;
    }

    public PublishOutputAssert containsPublishResponse(PublishResponse publishResponse) {
        Assertions.assertThat(actual.isSuccess()).isEqualTo(publishResponse.isSuccess());
        Assertions.assertThat(actual.getFailureSummary()).isEqualTo(publishResponse.getFailureSummary());
        Assertions.assertThat(actual.getFailureDescription()).isEqualTo(publishResponse.getFailureDescription());
        Assertions.assertThat(actual.getFailureCode()).isEqualTo(publishResponse.getFailureCode());
        HttpResponseAssert.assertThat(actual.getCreatePublishSetResponse()).isEqualToBaseResponse(publishResponse.getPublishSetResponse());
        Assertions.assertThat(actual.getStageToHttpResponseMap()).hasSize(publishResponse.getPublishToArcStageResponses().size());
        int stageCount = actual.getStageToHttpResponseMap().size();
        Assertions.assertThat(stageCount).isLessThan(3);
        if (stageCount > 0) {
            Map.Entry<String, HttpResponse> toStagingEntry = actual.getStageToHttpResponseMap().entrySet().asList().get(0);
            Assertions.assertThat(toStagingEntry.getKey()).isEqualTo(AUTHORING.getStageName());
            HttpResponseAssert.assertThat(toStagingEntry.getValue()).wasCreatedFromStage(publishResponse.getPublishToArcStageResponses().get(0));
        }
        if (stageCount > 1) {
            Map.Entry<String, HttpResponse> toLiveEntry = actual.getStageToHttpResponseMap().entrySet().asList().get(1);
            Assertions.assertThat(toLiveEntry.getKey()).isEqualTo(STAGING.getStageName());
            HttpResponseAssert.assertThat(toLiveEntry.getValue()).wasCreatedFromStage(publishResponse.getPublishToArcStageResponses().get(1));
        }

        return this;
    }
}
