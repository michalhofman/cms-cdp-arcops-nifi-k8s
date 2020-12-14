package com.viacom.arcops.nifi;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import org.apache.nifi.dbcp.DBCPService;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;

public class TestModule extends AbstractModule {
    @Override
    protected void configure() {
    }

    @Singleton
    @Provides
    DBCPService getService() {
        return NiFiTestUtils.h2dbcpService();
    }

    @Singleton
    @Provides
    JdbcTemplate getJdbcTemplate(DBCPService dbcpService) {
        DataSource dataSource = new DBCPServiceAdapter(dbcpService);
        return new JdbcTemplate(dataSource);
    }
}
