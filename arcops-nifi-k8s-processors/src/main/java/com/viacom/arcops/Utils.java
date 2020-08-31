package com.viacom.arcops;

import org.apache.commons.io.IOUtils;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessSession;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

public class Utils {

    public static Properties loadProperties(String classpath) {
        try {
            Properties props = new Properties();
            props.load(Utils.class.getResourceAsStream(classpath));
            return props;
        } catch (IOException e) {
            throw new IllegalArgumentException(e);
        }
    }

    public static String loadFlowFileToString(final FlowFile inputFlowFile, final ProcessSession session) {
        final AtomicReference<String> content = new AtomicReference<>();
        session.read(inputFlowFile, in -> content.set(IOUtils.toString(in, StandardCharsets.UTF_8)));
        return content.get();
    }

}
