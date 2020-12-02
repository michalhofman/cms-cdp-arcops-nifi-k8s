package com.viacom.arcops.nifi.publish;

import com.viacom.arcops.uca.PublishResponse.BaseResponse;
import com.viacom.arcops.uca.PublishResponse.PublishToArcStage;
import org.assertj.core.api.AbstractAssert;
import org.assertj.core.api.Assertions;

public class HttpResponseAssert extends AbstractAssert<HttpResponseAssert, HttpResponse> {

    private HttpResponseAssert(HttpResponse actual) {
        super(actual, HttpResponseAssert.class);
    }

    public static HttpResponseAssert assertThat(HttpResponse actual) {
        return new HttpResponseAssert(actual);
    }

    HttpResponseAssert isEqualToBaseResponse(BaseResponse expected) {
        assertEqualToBaseResponseIgnoringUuid(expected);
        Assertions.assertThat(actual.getUuid()).isEqualTo(expected.getUuid().orElse(null));

        return this;
    }

    HttpResponseAssert wasCreatedFromStage(PublishToArcStage stage) {
        if (stage.getCheckPublishStatusResponse().isPresent()) {
            assertEqualToBaseResponseIgnoringUuid(stage.getCheckPublishStatusResponse().orElseThrow(IllegalStateException::new));
            // In case that checkPublishStatusResponse is present, uuid is taken from createPublishRequestResponse (not from checkPublishStatusResponse like every other field):
            Assertions.assertThat(actual.getUuid()).isEqualTo(stage.getCreatePublishRequestResponse().orElseThrow(IllegalStateException::new).getUuid().orElseThrow(IllegalStateException::new));
        } else {
            assertEqualToBaseResponseIgnoringUuid(stage.getCreatePublishRequestResponse().orElseThrow(IllegalStateException::new));
            Assertions.assertThat(actual.getUuid()).isNull();
        }

        return this;
    }

    private void assertEqualToBaseResponseIgnoringUuid(BaseResponse expected) {
        Assertions.assertThat(actual.getHttpStatus()).isEqualTo(expected.getHttpStatus());
        Assertions.assertThat(actual.isSuccess()).isEqualTo(expected.isSuccess());
        Assertions.assertThat(actual.getFailureCode()).isEqualTo(expected.getFailureCode().orElse(null));
        Assertions.assertThat(actual.getFailureDescription()).isEqualTo(expected.getFailureDescription().orElse(null));
    }
}
