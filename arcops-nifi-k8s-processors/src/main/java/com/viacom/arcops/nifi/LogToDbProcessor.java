package com.viacom.arcops.nifi;

import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.*;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.rowset.serial.SerialBlob;
import java.nio.charset.StandardCharsets;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.viacom.arcops.nifi.NiFiProperties.*;
import static com.viacom.arcops.nifi.NiFiUtils.loadFlowFileToString;
import static org.apache.commons.lang3.exception.ExceptionUtils.getMessage;
import static org.apache.commons.lang3.exception.ExceptionUtils.getStackTrace;


@EventDriven
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"DB", "logging", "logs", "log"})
@CapabilityDescription("LogToDbProcessor is used to execute stored procedure on database")
@WritesAttributes({
        @WritesAttribute(attribute = ATTR_EXCEPTION_MESSAGE, description = "stack trace taken from exception occurred during processing"),
        @WritesAttribute(attribute = ATTR_EXCEPTION_STACKTRACE, description = "error message taken from exception occurred during processing")
})
@ReadsAttributes({
        @ReadsAttribute(attribute = "parameters")
})
public class LogToDbProcessor extends AbstractProcessor {

    private final static Logger LOG = LoggerFactory.getLogger(LogToDbProcessor.class);

    static final PropertyDescriptor QUERY = new PropertyDescriptor.Builder()
            .name("DatabaseQuery")
            .description("DB command accepted by configured database, ie: `{ call usp_put_to_log(?,?)}`")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    static final PropertyDescriptor FLOW_FILE_PARAM = new PropertyDescriptor.Builder()
            .name("FlowFileParam")
            .required(false)
            .addValidator(StandardValidators.createRegexMatchingValidator(SpParam.PATTERN))
            .build();

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .description("Dynamic DB query parameter that should be named like SPparam1")
                .name(propertyDescriptorName)
                .addValidator(new StandardValidators.StringLengthValidator(0, 1000))
                .dynamic(true)
                .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
                .build();
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();

        if (flowFile == null) {
            return;
        }

        final Map<String, String> attributes = Collections.unmodifiableMap(flowFile.getAttributes());

        final Set<SpParam> procParams = flowFile.getAttributes().entrySet().stream()
                .filter(e -> SpParam.PATTERN.matcher(e.getKey()).matches())
                .map(e -> SpParam.from(e.getKey(), e.getValue()))
                .collect(Collectors.toSet());

        String flowFileParam = context.getProperty(FLOW_FILE_PARAM).getValue();
        if (StringUtils.isNotBlank(flowFileParam)) {
            procParams.add(SpParam.from(flowFileParam, NiFiUtils.loadFlowFileToString(flowFile, session)));
        }

        Set<SpParam> paramsFromProperties = context.getProperties().entrySet().stream()
                .filter(e -> SpParam.PATTERN.matcher(e.getKey().getName()).matches())
                .map(e -> SpParam.from(e.getKey().getName(), context.newPropertyValue(e.getValue()).evaluateAttributeExpressions(attributes).getValue()))
                .collect(Collectors.toSet());

        procParams.addAll(paramsFromProperties);

        String query = context.getProperty(QUERY).evaluateAttributeExpressions(attributes).getValue();
        LOG.debug("Preparing query: {}", query);

        try {
            executeQuery(context, query, procParams);
            session.transfer(flowFile, SUCCESS);
        } catch (Exception ex) {
            LOG.error("Exception in LogToDbProcessor", ex);
            flowFile = session.putAttribute(flowFile, ATTR_EXCEPTION_MESSAGE, getMessage(ex));
            flowFile = session.putAttribute(flowFile, ATTR_EXCEPTION_STACKTRACE, getStackTrace(ex));
            session.transfer(session.penalize(flowFile), FAILURE);
        }
    }

    private static void executeQuery(ProcessContext context, String query, Set<SpParam> params) throws SQLException {
        try (Connection connection = context.getProperty(DBCP_SERVICE).asControllerService(DBCPService.class).getConnection();
             CallableStatement statement = connection.prepareCall(query)) {

            int statementParamCount = statement.getParameterMetaData().getParameterCount();
            LOG.debug("Expected number of input parameters: {}", statementParamCount);

            if (statementParamCount > params.size()) {
                throw new IllegalArgumentException("Expected number of parameters in SQL statement is: '" + statementParamCount + "', but was: '" + params.size() + "'");
            }

            Map<Integer, SpParam> paramsMap = params.stream().collect(Collectors.toMap(o -> o.number, o -> o));

            List<String> missingParameters = IntStream.range(1, statementParamCount + 1)
                    .filter(v -> !paramsMap.containsKey(v)).boxed()
                    .map(no -> SpParam.PREFIX + no)
                    .collect(Collectors.toList());

            if (missingParameters.size() > 0) {
                throw new IllegalArgumentException("Missing parameters " + missingParameters);
            }

            for (int i = 1; i <= statementParamCount; i++) {
                SpParam spParam = paramsMap.get(i);
                statement.setObject(i, spParam.getValue());
            }

            LOG.debug("Executing statement {}: {}", query, statement.getParameterMetaData().toString());
            statement.execute();
            LOG.debug("Statement executed");
        }
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return Arrays.asList(DBCP_SERVICE, QUERY, FLOW_FILE_PARAM);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return Sets.newHashSet(SUCCESS, FAILURE);
    }

    private static class SpParam {
        private static final String PREFIX = "SPparam";
        private static final Pattern PATTERN = Pattern.compile(PREFIX + "(?<number>\\d+)\\s*(:\\s*(?<type>[a-zA-Z]+))?");

        final private int number;
        final private String destType;
        final private String value;

        private SpParam(int number, String destType, String value) {
            this.number = number;
            this.destType = destType;
            this.value = value;
        }

        static SpParam from(String spParamExpr, String value) {
            spParamExpr = StringUtils.trim(spParamExpr);

            Matcher matcher = PATTERN.matcher(spParamExpr);
            if (matcher.matches()) {
                return new SpParam(Integer.parseInt(matcher.group("number")), matcher.group("type"), value);
            } else {
                throw new IllegalArgumentException(String.format("'%s' is not valid sp param expression", spParamExpr));
            }
        }

        Object getValue() {
            if (StringUtils.equalsAnyIgnoreCase("blob", destType)) {
                return toBlob(value);
            } else {
                return value;
            }
        }

        private static Blob toBlob(String string) {
            try {
                return new SerialBlob(string != null ? string.getBytes(StandardCharsets.UTF_8) : new byte[0]);
            } catch (SQLException e) {
                throw new IllegalArgumentException(e);
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            SpParam spParam = (SpParam) o;

            return number == spParam.number;
        }

        @Override
        public int hashCode() {
            return number;
        }
    }
}
