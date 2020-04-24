package io.cdap.hydrator.plugin;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.Transform;
import io.cdap.cdap.etl.api.TransformContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.soulwing.rot13.Rot13;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.List;

@Plugin(type = Transform.PLUGIN_TYPE)
@Name("SimpleMask") // <- NOTE: The name of the plugin should match the name of the docs and widget json files.
@Description("Mask text data using an insecure reversible Rot13 cipher")
public class SimpleMask extends Transform<StructuredRecord, StructuredRecord> {

    private static final Logger LOG = LoggerFactory.getLogger(ExampleTransformPlugin.class);

    // Usually, you will need a private variable to store the config that was passed to your class
    private final SimpleMask.Config config;
    private Schema outputSchema;
    private Schema input;

    public SimpleMask(SimpleMask.Config config) {
        this.config = config;
    }

    @Override
    public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
        super.configurePipeline(pipelineConfigurer);
        // It's usually a good idea to validate the configuration at this point. It will stop the pipeline from being
        // published if this throws an error.
        Schema inputSchema = pipelineConfigurer.getStageConfigurer().getInputSchema();
        config.validate(inputSchema);
        try {
            pipelineConfigurer.getStageConfigurer().setOutputSchema(Schema.parseJson(config.schema));
        } catch (IOException e) {
            throw new IllegalArgumentException("Output schema cannot be parsed.", e);
        }
    }

    @Override
    public void initialize(TransformContext context) throws Exception {
        super.initialize(context);
        outputSchema = Schema.parseJson(config.schema);
    }

    @Override
    public void transform(final StructuredRecord input, final Emitter<StructuredRecord> emitter) throws Exception {
        // Get all the fields that are in the output schema
        List<Schema.Field> fields = outputSchema.getFields();
        // Create a builder for creating the output record
        StructuredRecord.Builder builder = StructuredRecord.builder(outputSchema);
        // Add all the values to the builder
        for (Schema.Field field : input.getSchema().getFields()) {

            String fieldName = field.getName();
            if (fieldName.equals(config.scrambleText)) {
                String inputValue = input.get(fieldName).toString();
                String outputValue = Rot13.rotate(inputValue);
                builder.set(fieldName, outputValue);
            } else {
                builder.set(fieldName, input.get(fieldName));
            }
        }
        // If you wanted to make additional changes to the output record, this might be a good place to do it.

        // Finally, build and emit the record.
        emitter.emit(builder.build());
    }

    public static class Config extends PluginConfig {

        @Name("scrambleText")
        @Description("Field containing text to be scrambled")
        @Macro
        @Nullable
        private final String scrambleText;

        @Name("schema")
        @Description("Specifies the schema of the records outputted from this plugin")
        private final String schema;

        public Config(@Nullable final String scrambleText, final String schema) {
            this.scrambleText = scrambleText;
            this.schema = schema;
        }

        private void validate(Schema inputSchema) throws IllegalArgumentException {
            // It's usually a good idea to check the schema. Sometimes users edit
            // the JSON config directly and make mistakes.
            try {
                Schema.parseJson(schema);
            } catch (IOException e) {
                throw new IllegalArgumentException("Output schema cannot be parsed.", e);
            }
            // This method should be used to validate that the configuration is valid.
            if (scrambleText == null || scrambleText.isEmpty()) {
                throw new IllegalArgumentException("myOption is a required field.");
            }
            // You can use the containsMacro() function to determine if you can validate at deploy time or runtime.
            // If your plugin depends on fields from the input schema being present or the right type, use inputSchema
        }
    }
}
