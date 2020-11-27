package com.viacom.arcops.nifi;


import com.google.common.collect.Sets;
import org.apache.nifi.annotation.behavior.*;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.*;

import static com.viacom.arcops.nifi.NiFiProperties.*;
import static org.apache.commons.lang3.exception.ExceptionUtils.getMessage;
import static org.apache.commons.lang3.exception.ExceptionUtils.getStackTrace;

@EventDriven
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"Route", "SQL", "filter", "enhance", "attribute"})
@CapabilityDescription("RouteOnSQLCondition is used to route flow file to matched/unmatched outputs based on attribute value expression. " +
        "SQL query is used to retrieve additional attributes that can be used in expression. " +
        "All those attributes (columns) from database will be added to output flow file for next processors to use")
@WritesAttributes({
        @WritesAttribute(attribute = ATTR_EXCEPTION_MESSAGE, description = "stack trace taken from exception occurred during processing"),
        @WritesAttribute(attribute = ATTR_EXCEPTION_STACKTRACE, description = "error message taken from exception occurred during processing")
})
public class RouteOnSQLCondition extends AbstractProcessor {

    private final static Logger LOG = LoggerFactory.getLogger(RouteOnSQLCondition.class);

    static final PropertyDescriptor QUERY = new PropertyDescriptor.Builder()
            .name("DatabaseQuery")
            .description("SQL query, ie: `{ select someflag from dbtable where id = ${idAttr}}`. " +
                    "All the columns will be mapped to attributes with lowercase names. " +
                    "Query from example will populate attribute 'someflag' and add it to flow file" +
                    "If query returns no rows - output will be routed to failure." +
                    "If query returns more than one row, only first will be parsed.")
            .required(true)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    static final PropertyDescriptor EXPRESSION = new PropertyDescriptor.Builder()
            .name("RouteExpression")
            .description("Boolean routing expression. You can use all attributes from flow file and attributes created by DatabaseQuery result")
            .required(true)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    static final PropertyDescriptor EMPTY_RESULTS_TO_UNMATCHED = new PropertyDescriptor.Builder()
            .name("Route to 'unmatched' on empty result set")
            .description("If set to 'true', empty result set from query redirects flow file to 'unmatched' output. When 'false' or not set, empty result sets redirects to 'failure'")
            .required(false)
            .defaultValue("false")
            .expressionLanguageSupported(false)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();

    static final PropertyDescriptor DONT_ADD_ATTRIBUTES = new PropertyDescriptor.Builder()
            .name("Don't add new attributes to flow file")
            .description("If set to 'true', columns from DataBaseQuery won't be added to output flow file as attributes. They still can be used in 'RouteExpression")
            .required(false)
            .defaultValue("false")
            .expressionLanguageSupported(false)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();

    static final Relationship MATCHED = new Relationship.Builder()
            .name("matched")
            .build();

    static final Relationship UNMATCHED = new Relationship.Builder()
            .name("unmatched")
            .build();


    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();

        if (flowFile == null) {
            return;
        }

        final Map<String, String> attributes = Collections.unmodifiableMap(flowFile.getAttributes());
        final String query = context.getProperty(QUERY).evaluateAttributeExpressions(attributes).getValue();

        try {
            Map<String, String> dbAttributes = executeQuery(context, query);

            if (dbAttributes.isEmpty()) {
                if (getBoolean(context, EMPTY_RESULTS_TO_UNMATCHED)) {
                    session.transfer(flowFile, UNMATCHED);
                    return;
                } else {
                    throw new IllegalArgumentException(String.format("Query '%s' returned 0 rows", query));
                }
            }

            if (!getBoolean(context, DONT_ADD_ATTRIBUTES)) {
                session.putAllAttributes(flowFile, dbAttributes);
            }

            Map<String, String> allAttributes = new HashMap<>(attributes);
            allAttributes.putAll(dbAttributes);
            boolean matches = context.getProperty(EXPRESSION).evaluateAttributeExpressions(allAttributes).asBoolean();

            session.transfer(flowFile, matches ? MATCHED : UNMATCHED);

        } catch (Exception ex) {
            flowFile = session.putAttribute(flowFile, ATTR_EXCEPTION_MESSAGE, getMessage(ex));
            flowFile = session.putAttribute(flowFile, ATTR_EXCEPTION_STACKTRACE, getStackTrace(ex));
            session.transfer(session.penalize(flowFile), FAILURE);
        }
    }

    private boolean getBoolean(ProcessContext context, PropertyDescriptor propertyDescriptor) {
        Boolean b = context.getProperty(propertyDescriptor).asBoolean();
        return b != null && b;
    }

    private static Map<String, String> executeQuery(ProcessContext context, String query) throws SQLException {
        try (Connection connection = context.getProperty(DBCP_SERVICE).asControllerService(DBCPService.class).getConnection();
             CallableStatement statement = connection.prepareCall(query)) {

            LOG.debug("Executing statement {}", query);
            ResultSet resultSet = statement.executeQuery();
            if (resultSet.first()) {
                return parseResultSet(resultSet);
            } else {
                return Collections.emptyMap();
            }
        }
    }

    private static Map<String, String> parseResultSet(ResultSet resultSet) throws SQLException {
        Map<String, String> attributes = new HashMap<>();
        ResultSetMetaData metaData = resultSet.getMetaData();

        int columnCount = metaData.getColumnCount();
        for (int i = 1; i < columnCount + 1; i++) {
            String columnName = metaData.getColumnLabel(i);
            String columnValue = resultSet.getString(i);
            attributes.put(columnName.toLowerCase(), columnValue);
        }

        return attributes;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return Arrays.asList(DBCP_SERVICE, QUERY, EXPRESSION, EMPTY_RESULTS_TO_UNMATCHED, DONT_ADD_ATTRIBUTES);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return Sets.newHashSet(MATCHED, UNMATCHED, FAILURE);
    }

}
