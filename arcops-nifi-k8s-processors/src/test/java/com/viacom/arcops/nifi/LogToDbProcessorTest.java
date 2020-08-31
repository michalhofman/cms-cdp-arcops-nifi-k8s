package com.viacom.arcops.nifi;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import static com.viacom.arcops.nifi.LogToDbProcessor.FLOW_FILE_PARAM;
import static com.viacom.arcops.nifi.LogToDbProcessor.QUERY;
import static com.viacom.arcops.nifi.NiFiProperties.FAILURE;
import static com.viacom.arcops.nifi.NiFiProperties.SUCCESS;
import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
class LogToDbProcessorTest {

    private TestRunner testRunner;

    @BeforeEach
    void setUp() {
        testRunner = prepareTestRunnerFor(new LogToDbProcessor());
    }

    @Test
    void shouldStoreUTF8FlowFileContentsAsBlob() throws SQLException {
        Connection connection = prepareTables();

        testRunner.setProperty(QUERY, "insert into log_test (id, message) values (?, ?)");
        testRunner.setProperty(FLOW_FILE_PARAM, "SPparam2:Blob");

        String message = "Realität getroffen haben und überprüfen für s 2016 RO🌑 AM 🌕CK asj 2017 Sony Music Entertainment Türkiye Tic. A.Ş. ASJ" +
                "® Ⓟ ℠ ™ASJ ①②③④⑤⑥⑦⑧⑨⑩⑪⑫⑬⑭⑮⑯ ┠┡┢┣┤┥┦┧┨┩┪┫┬┭┮┯ ⠀⠁⠂⠃⠄⠅⠆⠇⠈⠉⠊⠋⠌⠍⠎⠏ ⭐⭑⭒⭓⭔⭕⭖⭗⭘⭙ ⰀⰁⰂⰃⰄⰅⰆⰇⰈⰉⰊⰋⰌⰍⰎⰏ";

        testRunner.enqueue(message, new HashMap<String, String>() {{
            put("SPparam1", "10");
        }});

        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(SUCCESS);

        List<Pair<Integer, String>> rows = DbUtils.dbQuery(connection, "test", "SELECT id, message from log_test where id = 10").call(r -> {
            List<Pair<Integer, String>> results = new ArrayList<>();
            while (r.next()) {
                int id = r.getInt(1);
                Blob blob = r.getBlob(2);
                results.add(Pair.of(id, new String(blob.getBytes(1, (int) blob.length()), StandardCharsets.UTF_8)));
            }
            return results;
        });

        assertThat(rows).hasSize(1);
        assertThat(rows.get(0).getLeft()).isEqualTo(10);
        assertThat(rows.get(0).getRight()).isEqualTo(message);
    }

    @Test
    void shouldUseFlowFileAttributeAndEvaluatedProcessorsPropertyAsSQLParameters() throws SQLException {
        Connection connection = prepareTables();
        final String messageId = "b2adc985-4460-4cd6-a29a-3901f1bd6802";

        testRunner.setProperty(QUERY, "insert into log_test (str_col, id) values (?, ?)");
        testRunner.setProperty("SPparam1", "${messageId}");


        testRunner.enqueue("msg", new HashMap<String, String>() {{
            put("SPparam2", "11");
            put("messageId", messageId);
        }});

        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(SUCCESS);

        List<Pair<Integer, String>> rows = DbUtils.dbQuery(
                connection,
                "test",
                "SELECT id, str_col from log_test where id = 11"
        ).call(r -> {
            List<Pair<Integer, String>> results = new ArrayList<>();
            while (r.next()) {
                results.add(Pair.of(r.getInt(1), r.getString(2)));
            }
            return results;
        });

        assertThat(rows).hasSize(1);
        assertThat(rows.get(0).getLeft()).isEqualTo(11);
        assertThat(rows.get(0).getRight()).isEqualTo(messageId);
    }

    @Test
    void shouldEvaluateQueryAndBindSqlParametersFromPropertiesAndAttributes() throws SQLException {
        Connection connection = prepareTables();
        final String messageId = "b2adc985-4460-4cd6-a29a-3901f1bd6802";

        testRunner.setProperty(QUERY, "insert into log_test (id, int_col, str_col, message) values (${id}, ?, ?, ?)");
        testRunner.setProperty("SPparam1", "999");
        testRunner.setProperty("SPparam2", "${messageId}");
        testRunner.setProperty(FLOW_FILE_PARAM, "SPparam3 : blob");

        testRunner.enqueue("msg", new HashMap<String, String>() {{
            put("id", "12");
            put("messageId", messageId);
        }});

        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(SUCCESS);

        List<List<String>> rows = DbUtils.dbQuery(connection, "test", "SELECT id, int_col, str_col, message from log_test where id = 12").call(r -> {
            List<List<String>> results = new ArrayList<>();
            while (r.next()) {
                Blob blob = r.getBlob(4);
                results.add(Arrays.asList(
                        String.valueOf(r.getInt(1)),
                        String.valueOf(r.getInt(2)),
                        r.getString(3),
                        new String(blob.getBytes(1, (int) blob.length()), StandardCharsets.UTF_8)
                        ));
            }
            return results;
        });

        assertThat(rows).hasSize(1);

        assertThat(rows.get(0).get(0)).isEqualTo("12");
        assertThat(rows.get(0).get(1)).isEqualTo("999");
        assertThat(rows.get(0).get(2)).isEqualTo(messageId);
        assertThat(rows.get(0).get(3)).isEqualTo("msg");
    }

    @Test
    void shouldHandleFailureOnWrongQuery() {

        testRunner.setProperty("DatabaseQuery", "select 1 fro dual");
        testRunner.enqueue("some data");

        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(FAILURE);
    }

    @Test
    void shouldFailOnLessThenExpectedNumberOfDBParameters() throws SQLException {
        prepareTables();

        testRunner.setProperty(QUERY, "insert into log_test (id, str_col) values (?, ?)");
        testRunner.setProperty("SPparam1", "30");

        testRunner.enqueue("msg", new HashMap<>());

        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(FAILURE);
        MockFlowFile ff = testRunner.getFlowFilesForRelationship(FAILURE).get(0);
        assertThat(ff.getAttribute(NiFiProperties.ATTR_EXCEPTION_MESSAGE)).isEqualTo("IllegalArgumentException: Expected number of parameters in SQL statement is: '2', but was: '1'");
        assertThat(ff.getAttributes()).containsKey(NiFiProperties.ATTR_EXCEPTION_STACKTRACE);
    }

    @Test
    void shouldFailOnMissingDBParameter() throws SQLException {
        prepareTables();

        testRunner.setProperty(QUERY, "insert into log_test (id, int_col, str_col) values (?, ?, ?)");
        testRunner.setProperty("SPparam1", "33");

        testRunner.enqueue("msg", new HashMap<String, String>() {{
            put("SPparam3", "some_str");
            put("SPparam4", "11");
        }});

        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(FAILURE);
        MockFlowFile ff = testRunner.getFlowFilesForRelationship(FAILURE).get(0);
        assertThat(ff.getAttribute(NiFiProperties.ATTR_EXCEPTION_MESSAGE)).isEqualTo("IllegalArgumentException: Missing parameters [SPparam2]");
        assertThat(ff.getAttributes()).containsKey(NiFiProperties.ATTR_EXCEPTION_STACKTRACE);
    }

    private Connection prepareTables() throws SQLException {
        DBCPService dbcpService = testRunner.getControllerService(NiFiTestUtils.H2_TEST_DB_POOL_SERVICE, DBCPService.class);
        dbcpService.getConnection().prepareCall("DROP TABLE if EXISTS log_test").execute();
        dbcpService.getConnection().prepareCall("CREATE TABLE log_test (id INT NOT NULL, int_col INT NULL, str_col VARCHAR(1000) NULL, message BLOB)").execute();
        return dbcpService.getConnection();
    }

    private static TestRunner prepareTestRunnerFor(AbstractProcessor processor) {
        TestRunner testRunner = TestRunners.newTestRunner(processor);
        NiFiTestUtils.addH2DbcpService(testRunner);
        return testRunner;
    }
}
