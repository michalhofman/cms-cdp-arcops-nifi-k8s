package com.viacom.arcops.nifi;

import com.google.inject.Injector;
import lombok.extern.slf4j.Slf4j;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

import static com.viacom.arcops.nifi.DatasetReaderProcessor.BODY_COLUMN;
import static com.viacom.arcops.nifi.DatasetReaderProcessor.QUERY;
import static com.viacom.arcops.nifi.NiFiProperties.FAILURE;
import static com.viacom.arcops.nifi.NiFiProperties.SUCCESS;
import static com.viacom.arcops.nifi.NiFiTestUtils.*;
import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
class DatasetReaderProcessorTest {
    private TestRunner runner;
    private DBCPService dbcpService;
    DatasetReaderProcessor processor;
    private final Injector injector = prepareInjector();

    @BeforeEach
    void setup() {
        processor = new DatasetReaderProcessor(injector);
        HashMap<String, String> processorParameters = new HashMap<String, String>() {{
            put(QUERY.getName(), "select param1, param2, param3 from any_table");
            put(BODY_COLUMN.getName(), "param3");
        }};
        runner = prepareTestRunnerFor(processor, processorParameters);
        dbcpService = injector.getInstance(DBCPService.class);
        dbcpService = addH2DbcpService(dbcpService, runner, null);
    }

    @Test
    void shouldRetrieveMessages() throws SQLException {
        final int rowCount = 4;
        fillUpTable(rowCount);

        runner.run();

        runner.assertValid();
        runner.assertAllFlowFilesTransferred(SUCCESS);

        List<MockFlowFile> failures = runner.getFlowFilesForRelationship(FAILURE);
        assertThat(failures.size()).isZero();

        List<MockFlowFile> success = runner.getFlowFilesForRelationship(SUCCESS);
        List<String> paramsFromFlowFiles = success.stream().map(mockFlowFile -> mockFlowFile.getAttribute("param1")).collect(Collectors.toList());
        assertThat(paramsFromFlowFiles).containsAll(Arrays.asList("1", "2", "3", "4"));
    }

    @Test
    void shouldProcessQuery() {
        String query = "#[propertyName]";
        HashMap<String, String> propertiesMap = new HashMap<String, String>() {{
            put("#[propertyName]", "key.value");
        }};

        String expectedQuery = processor.processDatasetQuery(query, propertiesMap);
        assertThat(expectedQuery).isEqualTo("key.value");
    }

    @Test
    void shouldProcessQueryWithDuplication() {
        String query = "#[propertyName] and #[propertyName]";
        HashMap<String, String> propertiesMap = new HashMap<String, String>() {{
            put("#[propertyName]", "key.value");
        }};

        String expectedQuery = processor.processDatasetQuery(query, propertiesMap);
        assertThat(expectedQuery).isEqualTo("key.value and key.value");
    }

    @Test
    void shouldProcessQueryWith2Params() {
        String query = "#12 and #21";
        HashMap<String, String> propertiesMap = new HashMap<String, String>() {
            {
                put("#12", "key.value");
                put("#21", "key.value2");
            }
        };

        String expectedQuery = processor.processDatasetQuery(query, propertiesMap);
        assertThat(expectedQuery).isEqualTo("key.value and key.value2");
    }

    @Test
    void shouldProcessComplicatedQuery() {
        String query = "Select * from table where property = propertyName";
        HashMap<String, String> propertiesMap = new HashMap<String, String>() {{
            put("propertyName", "key.value");
        }};

        String expectedQuery = processor.processDatasetQuery(query, propertiesMap);
        assertThat(expectedQuery).isEqualTo("Select * from table where property = key.value");
    }

    @Test
    void shouldNotProcessQuery() {
        String query = "queryWithoutPattern";
        String expectedQuery = processor.processDatasetQuery(query, Collections.emptyMap());
        runner.run();
        assertThat(expectedQuery).isEqualTo("queryWithoutPattern");
    }

    @Test
    void shouldExtractProperProperties(){
        runner.setProperty("Key1","value1");
        runner.setProperty("#[Key2]","value2");
        runner.setProperty("#dd","value2");
        runner.setProperty("#21","value2");
        runner.setProperty("#22","value2");
        Map<String, String> matchingProperties = processor.getPropertiesWhichMatchesWithPattern(runner.getProcessContext(), "#\\d+");
        assertThat(matchingProperties).containsOnlyKeys("#21","#22");
    }

    @Test
    void shouldNotExtractAnyProperties(){
        runner.setProperty("Key1","value1");
        runner.setProperty("#[Key2]","value2");
        runner.setProperty("#dd","value2");
        Map<String, String> matchingProperties = processor.getPropertiesWhichMatchesWithPattern(runner.getProcessContext(), "#\\d+");
        assertThat(matchingProperties).isEmpty();
    }

    @Test
    void integrationTestWithSqlPlaceholdersFeature() throws SQLException {
        runner.setProperty("#[Key2]","'low'");
        runner.setProperty(QUERY.getName(),"Select * from any_table where param4 = #[Key2]");
        fillUpTable(4, "low");
        runner.run();
        runner.assertAllFlowFilesTransferred(SUCCESS);
        List<MockFlowFile> successedFlowFiles = runner.getFlowFilesForRelationship(SUCCESS);
        assertThat(successedFlowFiles.size()).isEqualTo(4);
        boolean allFlowFilesHaveProperParam4 = successedFlowFiles.stream().allMatch(mockFlowFile -> "low".equals(mockFlowFile.getAttribute("param4")));
        assertThat(allFlowFilesHaveProperParam4).isTrue();
    }


    @Test
    void shouldNotExtractAnyPropertiesSecondTest(){
        Map<String, String> matchingProperties = processor.getPropertiesWhichMatchesWithPattern(runner.getProcessContext(), "#\\d+");
        assertThat(matchingProperties).isEmpty();
    }

    private void fillUpTable(int rowCount) throws SQLException {
        fillUpTable(rowCount,"");
    }

    private void fillUpTable(int rowCount, String param4) throws SQLException {
        try (Connection connection = dbcpService.getConnection()) {
            connection.prepareCall("DROP TABLE if EXISTS any_table").execute();
            connection.prepareCall("CREATE TABLE any_table (param1 INT NOT NULL, param2 DOUBLE NULL, param3 VARCHAR(1000) NULL, param4 VARCHAR(1000))").execute();
            int row = 0;
            insertRowsIntoTable( param4, connection, row, rowCount);
            int notValidRowNumber = new Random(rowCount).nextInt();
            insertRowsIntoTable("Not" + param4, connection, row, notValidRowNumber);
        }
    }

    private void insertRowsIntoTable(String param4, Connection connection, int row, int rowNumber) throws SQLException {
        int rowCount = row + rowNumber;
        while (++row <= rowCount){
            String param1 = String.valueOf(row);
            String param2 = param1 + "." + param1;
            String param3 = "value" + param1;
            String insert = "INSERT INTO any_table(param1, param2, param3, param4) VALUES (" + param1 + ", " + param2 + ", " + "'" + param3 + "'"+", " +"'" + param4 + "'"+")";
            log.info(insert);
            connection.prepareCall(insert).execute();
        }
    }

}
