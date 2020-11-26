package com.viacom.arcops.nifi.publish;

import lombok.Value;

@Value
class HttpResponse {
    int httpStatus;
    boolean success;
    String uuid;
    String failureCode;
    String failureDescription;
}
