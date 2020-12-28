package com.viacom.arcops.nifi;

import com.google.common.collect.Sets;
import com.google.inject.Injector;
import com.google.inject.Module;
import lombok.extern.slf4j.Slf4j;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.util.StringUtils;
import org.springframework.jdbc.core.JdbcTemplate;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.viacom.arcops.nifi.NiFiProperties.*;
import static com.viacom.arcops.nifi.NiFiUtils.stringToNewFlowFile;
import static org.apache.commons.lang3.exception.ExceptionUtils.getMessage;
import static org.apache.commons.lang3.exception.ExceptionUtils.getStackTrace;

@EventDriven
@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)
@Tags({"DB", "dataset", "sql", "select", "flowfile", "rows"})
@CapabilityDescription("DatasetReaderProcessor is used to execute stored procedure or a SELECT query providing a dataset. " +
        "Each row is then pushed as a flowfile with designated column serving as its body whereas the others are set as a flowfile attribues." +
        "Query can have placeholders which will be replaced with user property values." +
        "Within query each name of such a property need to follow pattern: #[propertyName]." +
        "For example to run query like: select * from #[propertyName] there must be provided property named: propertyName.")
@WritesAttributes({
        @WritesAttribute(attribute = ATTR_EXCEPTION_MESSAGE, description = "stack trace taken from exception occurred during processing"),
        @WritesAttribute(attribute = ATTR_EXCEPTION_STACKTRACE, description = "error message taken from exception occurred during processing")
})
@Slf4j
public class DatasetReaderProcessor extends GuiceConfiguredProcessor {

    static final PropertyDescriptor BODY_COLUMN = new PropertyDescriptor.Builder()
            .name("BodyColumn")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();


    static final PropertyDescriptor QUERY = new PropertyDescriptor.Builder()
            .name("DatabaseQuery")
            .description("Database SELECT or stored procedure call returning a dataset")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .description("Dynamic DB query parameter")
                .name(propertyDescriptorName)
                .addValidator(new StandardValidators.StringLengthValidator(0, 1000))
                .dynamic(true)
                .defaultValue(StringUtils.EMPTY)
                .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
                .build();
    }

    private JdbcTemplate jdbcTemplate;
    private String columnForFlowFileBody;
    private String datasetQuery;

    @SuppressWarnings("unused")
    DatasetReaderProcessor(Injector injector) {
        super(injector);
    }

    @SuppressWarnings("unused")
    public DatasetReaderProcessor() {
    }

    @OnScheduled
    @SuppressWarnings("unused")
    public void setup(final ProcessContext processContext) {
        jdbcTemplate = initializeInjector(processContext).getInstance(JdbcTemplate.class);
        datasetQuery = processContext.getProperty(QUERY).getValue();
        columnForFlowFileBody = processContext.getProperty(BODY_COLUMN).getValue();
    }

    @Override
    public void onTrigger(ProcessContext processContext, ProcessSession processSession) throws ProcessException {
        try {
            Map<String, String> properties = getDynamicProperties(processContext);
            datasetQuery = processDatasetQuery(datasetQuery, properties);
            log.info("Reading dataset: {}", datasetQuery);

            processFlowFilesQuery(processSession);
        } catch (Exception ex) {
            log.error("Exception in DatasetReaderProcessor: {}", ex.getMessage());
            FlowFile flowFileOnError = processSession.create();
            flowFileOnError = processSession.putAttribute(flowFileOnError, ATTR_EXCEPTION_MESSAGE, getMessage(ex));
            flowFileOnError = processSession.putAttribute(flowFileOnError, ATTR_EXCEPTION_STACKTRACE, getStackTrace(ex));
            processSession.transfer(flowFileOnError, FAILURE);
        }
    }

    Map<String, String> getDynamicProperties(ProcessContext processContext) {
        return processContext.getProperties().entrySet().stream()
                .filter(p->p.getKey().isDynamic())
                .collect(Collectors.toMap(property -> property.getKey().getName(), Map.Entry::getValue));
    }

    String processDatasetQuery(String query, Map<String, String> properties) {
        if (properties.isEmpty()) {
            return query;
        }

        for (Map.Entry<String, String> property : properties.entrySet()) {
            String propertyKey = property.getKey();
            String propertyValue = property.getValue().replace("'", "''");
            propertyValue = "'" + propertyValue + "'";
            query = query.replace("#["+propertyKey+"]", propertyValue);
        }

        return query;
    }

    private void processFlowFilesQuery(ProcessSession processSession) {
        jdbcTemplate.query(datasetQuery, resultSet -> {
            ResultSetMetaData metaData = resultSet.getMetaData();
            List<String> attributeColumns = getAttributeColumnLabels(metaData);
            Optional<String> bodyColumn = getBodyColumnLabel(metaData);
            FlowFile flowFile = processSession.create();
            if (bodyColumn.isPresent()) {
                flowFile = stringToNewFlowFile(resultSet.getString(bodyColumn.get()), processSession, flowFile);
            }
            for (String column : attributeColumns) {
                processSession.putAttribute(flowFile, column.toLowerCase(), resultSet.getString(column));
            }
            log.trace("Sending flowfile: {}", attributeColumns);
            processSession.transfer(flowFile, SUCCESS);
        });
    }

    private List<String> getAttributeColumnLabels(ResultSetMetaData resultSetMetaData) throws SQLException {
        List<String> columns = new ArrayList<>();
        int columnId = resultSetMetaData.getColumnCount() + 1;
        while (--columnId > 0) {
            if (!resultSetMetaData.getColumnLabel(columnId).equals(columnForFlowFileBody)) {
                columns.add(resultSetMetaData.getColumnLabel(columnId));
            }
        }
        return columns;
    }

    private Optional<String> getBodyColumnLabel(ResultSetMetaData resultSetMetaData) throws SQLException {
        int columnId = resultSetMetaData.getColumnCount() + 1;
        while (--columnId > 0) {
            if (resultSetMetaData.getColumnLabel(columnId).equals(columnForFlowFileBody)) {
                return Optional.of(resultSetMetaData.getColumnLabel(columnId));
            }
        }
        return Optional.empty();
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return Arrays.asList(DBCP_SERVICE, QUERY, BODY_COLUMN);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return Sets.newHashSet(SUCCESS, FAILURE);
    }

    @Override
    protected Function<Map<String, String>, List<Module>> getModulesCreator() {
        return null;
    }
}
