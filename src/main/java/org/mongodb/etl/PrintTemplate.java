package org.mongodb.etl;

import org.bson.json.JsonWriterSettings;
import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;
import uk.dioxic.mgenerate.core.Template;

import java.util.concurrent.Callable;

@Command(name = "print", description = "prints an mgenerate template to the console")
public class PrintTemplate implements Callable<Integer> {

    @Parameters(paramLabel = "TEMPLATE", description = "mgenerate template file", converter = TemplateTypeConverter.class)
    Template template;

    @Override
    public Integer call() {
        System.out.println(template.toJson(JsonWriterSettings.builder().indent(true).build()));
        return 0;
    }
}
