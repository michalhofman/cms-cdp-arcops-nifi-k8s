package com.viacom.arcops.nifi;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.processor.ProcessContext;
import org.springframework.jdbc.core.JdbcTemplate;

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

    @Provides
    @Singleton
    JdbcTemplate getJdbcTemplate(DBCPService dbcpService) {
        DataSource dataSource = new DBCPServiceAdapter(dbcpService);
        return new JdbcTemplate(dataSource);
    }

}
