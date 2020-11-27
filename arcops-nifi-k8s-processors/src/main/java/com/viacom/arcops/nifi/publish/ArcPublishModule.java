package com.viacom.arcops.nifi.publish;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.viacom.arcops.uca.ArcOperations;
import com.viacom.arcops.uca.ArcServer;
import com.viacom.arcops.uca.UcaWriteService;
import com.viacom.arcops.uca.UcaWriteServiceImpl;

public class ArcPublishModule extends AbstractModule {

    @Override
    protected void configure() {
        bind(ArcOperations.class).to(ArcServer.class);
    }

    @Provides
    @Singleton
    UcaWriteService ucaWriteService(ArcOperations arcOperations) {
        return new UcaWriteServiceImpl(arcOperations);
    }
}
