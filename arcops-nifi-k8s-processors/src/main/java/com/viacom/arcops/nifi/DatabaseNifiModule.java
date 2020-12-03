package com.viacom.arcops.nifi;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.processor.ProcessContext;

import java.util.Properties;

import static com.viacom.arcops.nifi.NiFiProperties.DBCP_SERVICE;

class DatabaseNifiModule extends AbstractModule {

    private final ProcessContext context;
    private final Properties properties;

    DatabaseNifiModule(ProcessContext context) {
        this.context = context;
        this.properties = getProperties(context);
    }

    @Override
    protected void configure() {
//        Properties defaultProperties = Utils.loadProperties("dbconnection.properties");
//        defaultProperties.putAll(properties); // override properties from file with the one set on Nifi
//        Names.bindProperties(binder(), defaultProperties);
    }

    @Provides
    @Singleton
    DBCPService getDataSource() {
        return context.getProperty(DBCP_SERVICE).asControllerService(DBCPService.class);
    }

    private static Properties getProperties(ProcessContext context) {
        Properties properties = new Properties();
        context.getProperties().forEach((k, v) ->
                properties.put(k.getName(), StringUtils.isNotBlank(v) ? v : k.getDefaultValue())
        );
        return properties;
    }
}
