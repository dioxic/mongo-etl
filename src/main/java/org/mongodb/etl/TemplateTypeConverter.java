package org.mongodb.etl;

import picocli.CommandLine;
import uk.dioxic.mgenerate.core.Template;

import java.io.IOException;
import java.nio.file.Paths;

class TemplateTypeConverter implements CommandLine.ITypeConverter<Template> {
    public Template convert(String value) throws IOException {
        return Template.from(Paths.get(value));
    }
}