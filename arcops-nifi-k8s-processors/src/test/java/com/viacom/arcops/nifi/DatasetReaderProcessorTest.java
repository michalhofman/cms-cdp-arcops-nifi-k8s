package com.viacom.arcops.nifi;

import com.google.inject.Injector;
import lombok.extern.slf4j.Slf4j;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

import static com.viacom.arcops.nifi.DatasetReaderProcessor.BODY_COLUMN;
import static com.viacom.arcops.nifi.DatasetReaderProcessor.QUERY;
import static com.viacom.arcops.nifi.NiFiProperties.FAILURE;
import static com.viacom.arcops.nifi.NiFiProperties.SUCCESS;
import static com.viacom.arcops.nifi.NiFiTestUtils.*;
import static org.assertj.core.api.Assertions.*;
@Slf4j
public class DatasetReaderProcessorTest {
    private TestRunner runner;
    private DBCPService dbcpService;

    @BeforeEach
    void setup() {
        final Injector injector = prepareInjector();
        DatasetReaderProcessor processor = new DatasetReaderProcessor(injector);
        HashMap<String, String> processorParameters = new HashMap<String, String>() {{
            put(QUERY.getName(), "select param1, param2, param3 from any_table");
            put(BODY_COLUMN.getName(), "param3");
        }};
        runner = prepareTestRunnerFor(processor, processorParameters);
        dbcpService = injector.getInstance(DBCPService.class);
        dbcpService = addH2DbcpService(dbcpService,runner,null);
        // TODO: assert flow file content
    }

    @Test
    public void shouldRetrieveMessages() throws SQLException {
        final int rowCount = 4;
        fillUpTable(rowCount);

        runner.run();

        runner.assertValid();
        runner.assertAllFlowFilesTransferred(SUCCESS);

        List<MockFlowFile> failures = runner.getFlowFilesForRelationship(FAILURE);
        assertThat(failures.size()).isZero();

        List<MockFlowFile> success = runner.getFlowFilesForRelationship(SUCCESS);
        List<String> paramsFromFlowFiles = success.stream().map(mockFlowFile -> mockFlowFile.getAttribute("param1")).collect(Collectors.toList());
        assertThat(paramsFromFlowFiles).containsAll(Arrays.asList("1","2","3","4"));
    }

    private void fillUpTable(int rowCount) throws SQLException {
        dbcpService.getConnection().prepareCall("DROP TABLE if EXISTS any_table").execute();
        dbcpService.getConnection().prepareCall("CREATE TABLE any_table (param1 INT NOT NULL, param2 DOUBLE NULL, param3 VARCHAR(1000) NULL)").execute();
        int row = 0;
        while (++row <= rowCount) {
            String param1 = String.valueOf(row);
            String param2 = param1 + "." + param1;
            String param3 = "value" + param1;
            String insert = "INSERT INTO any_table(param1, param2, param3) VALUES (" + param1 + ", " + param2 + ", " + "'" + param3 + "')";
            log.info(insert);
            dbcpService.getConnection().prepareCall(insert).execute();
        }

    }

}
