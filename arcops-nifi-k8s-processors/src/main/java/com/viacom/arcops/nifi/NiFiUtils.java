package com.viacom.arcops.nifi;

import org.apache.commons.io.IOUtils;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessSession;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicReference;

@SuppressWarnings("WeakerAccess")
public class NiFiUtils {

    public static String loadFlowFileToString(final FlowFile inputFlowFile, final ProcessSession session) {
        return loadFlowFileToString(inputFlowFile, session, StandardCharsets.UTF_8);
    }

    public static String loadFlowFileToString(final FlowFile inputFlowFile, final ProcessSession session, final Charset encoding) {
        final AtomicReference<String> content = new AtomicReference<>();
        session.read(inputFlowFile, in -> content.set(IOUtils.toString(in, encoding)));
        return content.get();
    }
}
