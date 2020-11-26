package com.viacom.arcops.nifi;

import com.google.common.io.CharStreams;
import org.apache.commons.io.IOUtils;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.StringReader;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Objects;
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

    public static FlowFile stringToNewFlowFile(String s, ProcessSession session, FlowFile flowFile) {
        return session.write(flowFile, (out) -> {
            stringToOutputStream(s, out);
        });
    }

    public static String resolveNifiProperty(ProcessContext context, PropertyDescriptor propertyDescriptor, Map<String, String> flowFileAttributes) {
        PropertyValue value = context.getProperty(propertyDescriptor);
        if (propertyDescriptor.isExpressionLanguageSupported()) {
            value = value.evaluateAttributeExpressions(flowFileAttributes);
        }

        return value.getValue();
    }

    private static void stringToOutputStream(String text, OutputStream outputStream) throws IOException {
        Objects.requireNonNull(text, "non-null text required");
        Objects.requireNonNull(outputStream, "non-null outputStream required");
        OutputStreamWriter to = new OutputStreamWriter(outputStream, StandardCharsets.UTF_8);
        CharStreams.copy(new StringReader(text), to);
        to.flush();
    }
}
