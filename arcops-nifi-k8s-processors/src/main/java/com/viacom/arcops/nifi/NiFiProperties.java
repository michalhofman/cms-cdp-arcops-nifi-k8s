package com.viacom.arcops.nifi;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;

public class NiFiProperties {

    public static final String ATTR_EXCEPTION_MESSAGE = "exception.message";
    public static final String ATTR_EXCEPTION_STACKTRACE = "exception.stacktrace";

    static final PropertyDescriptor DBCP_SERVICE = new PropertyDescriptor.Builder()
            .name("Database Controller Service")
            .description("The Controller Service to obtain connection to database")
            .required(true)
            .identifiesControllerService(DBCPService.class)
            .build();

    static final Relationship SUCCESS = new Relationship.Builder()
            .name("success")
            .build();

    static final Relationship FAILURE = new Relationship.Builder()
            .name("failure")
            .build();

}
