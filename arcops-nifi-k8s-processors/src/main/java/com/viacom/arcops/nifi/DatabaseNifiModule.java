package com.viacom.arcops.nifi;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.name.Names;
import com.viacom.arcops.Utils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.processor.ProcessContext;

import javax.sql.DataSource;
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

        Properties defaultProperties = Utils.loadProperties("/vevo.properties");
        defaultProperties.putAll(properties); // override properties from file with the one set on Nifi
        Names.bindProperties(binder(), defaultProperties);
    }

    @Provides
    @Singleton
    DataSource getDataSource() {
        PropertyValue vevoStoreProperty = context.getProperty(DBCP_SERVICE);
        Validate.isTrue(vevoStoreProperty.isSet(), "Database service property is not set. Check processor configuration");
        return new DBCPServiceAdapter(vevoStoreProperty.asControllerService(DBCPService.class));
    }

    private static Properties getProperties(ProcessContext context) {
        Properties properties = new Properties();
        context.getProperties().forEach((k, v) ->
                properties.put(k.getName(), StringUtils.isNotBlank(v) ? v : k.getDefaultValue())
        );

        return properties;
    }
}
