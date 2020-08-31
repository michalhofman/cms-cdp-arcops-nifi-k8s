package com.viacom.arcops.nifi;

import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.nifi.dbcp.DBCPConnectionPool;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.util.TestRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

import static com.viacom.arcops.nifi.NiFiProperties.DBCP_SERVICE;
import static org.mockito.Mockito.mock;

class NiFiTestUtils {
    private static final Logger LOG = LoggerFactory.getLogger(NiFiTestUtils.class);

    static void addDbcpService(TestRunner runner) {
        try {
            Properties prop = new Properties();
            prop.load(ClassLoader.getSystemResourceAsStream("dbconnection.properties"));

            String jdbcUrl = String.format("jdbc:mysql://%s:%s/%s",
                    prop.getProperty("serverName"),
                    prop.getProperty("portNumber"),
                    prop.getProperty("dbName")
            );
            LOG.info(jdbcUrl);

            DBCPConnectionPool service = new DBCPConnectionPool();
            String dbscpServiceIdentifier = "test-db-pool-service";
            runner.addControllerService(dbscpServiceIdentifier, service);
            runner.setProperty(service, DBCPConnectionPool.DATABASE_URL, jdbcUrl);
            runner.setProperty(service, DBCPConnectionPool.DB_USER, prop.getProperty("userName"));
            runner.setProperty(service, DBCPConnectionPool.DB_PASSWORD, prop.getProperty("password"));
            runner.setProperty(service, DBCPConnectionPool.DB_DRIVERNAME, "com.mysql.jdbc.Driver");
            runner.enableControllerService(service);
            runner.assertValid(service);
            runner.setProperty(DBCP_SERVICE, dbscpServiceIdentifier);

            runner.assertValid(runner.getProcessContext().getControllerServiceLookup().getControllerService(dbscpServiceIdentifier));

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    static Injector prepareInjector() {
        return Guice.createInjector(new TestModule());
    }

    static final String H2_TEST_DB_POOL_SERVICE = "test-h2-db-pool-service";

    public static DBCPService addH2DbcpService(TestRunner runner, String initCommand) {
        try {
            DBCPConnectionPool service = new DBCPConnectionPool();
            String dbscpServiceIdentifier = H2_TEST_DB_POOL_SERVICE;
            runner.addControllerService(dbscpServiceIdentifier, service);
            String url = "jdbc:h2:mem:test" + (initCommand != null ? initCommand : "");
            runner.setProperty(service, DBCPConnectionPool.DATABASE_URL, url);
            runner.setProperty(service, DBCPConnectionPool.DB_DRIVERNAME, "org.h2.Driver");
            runner.enableControllerService(service);
            runner.assertValid(service);
            runner.setProperty(DBCP_SERVICE, dbscpServiceIdentifier);
            runner.assertValid(runner.getProcessContext().getControllerServiceLookup().getControllerService(dbscpServiceIdentifier));
            return service;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    static DBCPService addH2DbcpService(TestRunner runner) {
        return addH2DbcpService(runner, null);
    }
}
