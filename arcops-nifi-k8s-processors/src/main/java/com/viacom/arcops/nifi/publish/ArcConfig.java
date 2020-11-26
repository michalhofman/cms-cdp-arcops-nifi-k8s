package com.viacom.arcops.nifi.publish;

import lombok.Builder;
import lombok.Value;

@Value
@Builder(toBuilder = true)
public class ArcConfig {

    Long arcTimeout;
    Long publishStatusCheckRetryDelay;
    Integer publishStatusCheckRetryCount;
    Long httpRequestRetryDelay;
    Integer httpRequestRetryCount;
    Long stagingToLiveDelay;

    ArcConfig merge(ArcConfig other) {
        if (other == null) {
            return this;
        }
        ArcConfigBuilder builder = toBuilder();
        if (arcTimeout == null) {
            builder.arcTimeout(other.arcTimeout);
        }
        if (publishStatusCheckRetryDelay == null) {
            builder.publishStatusCheckRetryDelay(other.publishStatusCheckRetryDelay);
        }
        if (publishStatusCheckRetryCount == null) {
            builder.publishStatusCheckRetryCount(other.publishStatusCheckRetryCount);
        }
        if (httpRequestRetryDelay == null) {
            builder.httpRequestRetryDelay(other.httpRequestRetryDelay);
        }
        if (httpRequestRetryCount == null) {
            builder.httpRequestRetryCount(other.httpRequestRetryCount);
        }
        if (stagingToLiveDelay == null) {
            builder.stagingToLiveDelay(other.stagingToLiveDelay);
        }
        return builder.build();
    }
}
