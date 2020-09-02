package com.viacom.arcops.nifi;

import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;

import java.util.List;
import java.util.Set;

import static com.viacom.arcops.nifi.NiFiProperties.SUCCESS;
import static java.util.Collections.emptyList;
import static java.util.Collections.singleton;
import static org.apache.nifi.annotation.behavior.InputRequirement.Requirement.INPUT_REQUIRED;

@EventDriven
@InputRequirement(INPUT_REQUIRED)
@Tags({"penalty", "penalize"})
@CapabilityDescription("PenalizingProcessor penalizes every flow file")
public class PenalizingProcessor extends AbstractProcessor {

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();

        if (flowFile == null) {
            return;
        }

        session.transfer(session.penalize(flowFile), SUCCESS);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return emptyList();
    }

    @Override
    public Set<Relationship> getRelationships() {
        return singleton(SUCCESS);
    }
}
