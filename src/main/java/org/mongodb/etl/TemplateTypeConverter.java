package org.mongodb.etl;

import picocli.CommandLine;
import uk.dioxic.mgenerate.core.Template;

import java.nio.file.Paths;

class TemplateTypeConverter implements CommandLine.ITypeConverter<Template> {
        public Template convert(String value) {
            return Template.from(Paths.get(value));
        }
    }