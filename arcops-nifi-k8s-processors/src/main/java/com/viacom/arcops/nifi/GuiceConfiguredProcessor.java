package com.viacom.arcops.nifi;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

public abstract class GuiceConfiguredProcessor extends AbstractProcessor {

    private Injector testInjector;

    @SuppressWarnings({"WeakerAccess", "unused"})
    public GuiceConfiguredProcessor() {
    }

    GuiceConfiguredProcessor(Injector testInjector) {
        this.testInjector = testInjector;
    }

    public Injector initializeInjector(ProcessContext context) {
        return testInjector == null ? Guice.createInjector(new DatabaseNifiModule(context)) : testInjector;
    }

    protected abstract Function<Map<String, String>, List<Module>> getModulesCreator();

}
