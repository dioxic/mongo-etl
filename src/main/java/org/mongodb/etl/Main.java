package org.mongodb.etl;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import uk.dioxic.mgenerate.core.Template;

import java.nio.file.Paths;
import java.util.concurrent.Callable;

@Command
public class Main implements Callable<Integer> {

    public static void main(String[] args) {
        CommandLine cl = new CommandLine(new Main());
        cl.registerConverter(Template.class, s -> Template.from(Paths.get(s)));
        cl.addSubcommand("load", new TemplateLoader());
        System.exit(cl.execute(args));
    }

    @Override
    public Integer call() {
        System.out.println(this);
        return 0;
    }

}
