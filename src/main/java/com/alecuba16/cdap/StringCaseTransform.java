package com.alecuba16.cdap;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.*;
import javax.annotation.Nullable;
import java.util.*;
import java.util.regex.Pattern;

/**
 * Transformation Plugin
 * A Transform plugin is used to convert one input record into zero or more output records. It can be used in both
 * batch and real-time data pipelines.

 * The only method that needs to be implemented is: transform()

 * Methods
 * initialize(): Used to perform any initialization step that might be required during the runtime of the Transform.
 * It is guaranteed that this method will be invoked before the transform method.
 *
 * transform(): This method contains the logic that will be applied on each incoming data object. An emitter can be
 * used to pass the results to the subsequent stage.
 *
 * destroy(): Used to perform any cleanup before the plugin shuts down.
 */
/**
 * Transform that can transforms specific fields to lowercase or uppercase.
 */
@Plugin(type = Transform.PLUGIN_TYPE)
@Name(StringCaseTransform.NAME)
@Description("Transforms configured fields to lowercase or uppercase.")
public class StringCaseTransform extends Transform<StructuredRecord, StructuredRecord> {
    public static final String NAME = "StringCase";
    private final Conf config;
    private Set<String> upperFields;
    private Set<String> lowerFields;

    /**
     * Config properties for the plugin.
     */
    public static class Conf extends PluginConfig {
        public static final String UPPER_FIELDS = "upperFields";
        public static final String LOWER_FIELDS = "lowerFields";
        private static final Pattern SPLIT_ON = Pattern.compile("\\s*,\\s*");

        // nullable means this property is optional
        @Nullable
        @Name(UPPER_FIELDS)
        @Description("A comma separated list of fields to uppercase. Each field must be of type String.")
        private String upperFields;

        @Nullable
        @Name(LOWER_FIELDS)
        @Description("A comma separated list of fields to lowercase. Each field must be of type String.")
        private String lowerFields;

        private Set<String> getUpperFields() {
            return parseToSet(upperFields);
        }

        private Set<String> getLowerFields() {
            return parseToSet(lowerFields);
        }

        private Set<String> parseToSet(String str) {
            Set<String> set = new HashSet<>();
            if (str == null || str.isEmpty()) {
                return set;
            }
            for (String element : SPLIT_ON.split(str)) {
                set.add(element);
            }
            return set;
        }
    }

    public StringCaseTransform(Conf config) {
        this.config = config;
    }

    // configurePipeline is called only once, when the pipeline is deployed. Static validation should be done here.
    @Override
    public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
        StageConfigurer stageConfigurer = pipelineConfigurer.getStageConfigurer();
        // the output schema is always the same as the input schema
        Schema inputSchema = stageConfigurer.getInputSchema();

        // if schema is null, that means it is either not known until runtime, or it is variable
        if (inputSchema != null) {
            // if the input schema is constant and known at configure time, check that all configured fields are strings
            for (String fieldName : config.getUpperFields()) {
                validateFieldIsString(inputSchema, fieldName);
            }
            for (String fieldName : config.getLowerFields()) {
                validateFieldIsString(inputSchema, fieldName);
            }
        }

        stageConfigurer.setOutputSchema(inputSchema);
    }

    // initialize is called once at the start of each pipeline run
    @Override
    public void initialize(TransformContext context) throws Exception {
        upperFields = config.getUpperFields();
        lowerFields = config.getLowerFields();
    }

    // transform is called once for each record that goes into this stage
    @Override
    public void transform(StructuredRecord record, Emitter<StructuredRecord> emitter) throws Exception {
        /*
         * Field metrics, uncomment the following line and the one inside the for loop if you want to
         * have a counter with the number of fields that you have changed at the CDAP's metrics microservice
         * https://cdap.atlassian.net/wiki/spaces/DOCS/pages/477692194/Metrics+Microservices
         */
        // int fieldsChanged = 0;

        StructuredRecord.Builder builder = StructuredRecord.builder(record.getSchema());
        for (Schema.Field field : record.getSchema().getFields()) {
            String fieldName = field.getName();
            if (upperFields.contains(fieldName)) {
                builder.set(fieldName, record.get(fieldName).toString().toUpperCase());
            } else if (lowerFields.contains(fieldName)) {
                builder.set(fieldName, record.get(fieldName).toString().toLowerCase());
            } else {
                builder.set(fieldName, record.get(fieldName));
            }
            // Field metrics, uncomment if you want to enable this metric.
            // fieldsChanged += 1;
        }

        // Uncomment to publish this metric
        // getContext().getMetrics().count("fieldsChanged", fieldsChanged);

        emitter.emit(builder.build());
    }

    private void validateFieldIsString(Schema schema, String fieldName) {
        Schema.Field inputField = schema.getField(fieldName);
        if (inputField == null) {
            throw new IllegalArgumentException(
                    String.format("Field '%s' does not exist in input schema %s.", fieldName, schema));
        }
        Schema fieldSchema = inputField.getSchema();
        Schema.Type fieldType = fieldSchema.isNullable() ? fieldSchema.getNonNullable().getType() : fieldSchema.getType();
        if (fieldType != Schema.Type.STRING) {
            throw new IllegalArgumentException(
                    String.format("Field '%s' is of illegal type %s. Must be of type %s.",
                            fieldName, fieldType, Schema.Type.STRING));
        }
    }
}
