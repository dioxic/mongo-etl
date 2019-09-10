package org.mongodb.etl;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import uk.dioxic.mgenerate.core.Template;
import uk.dioxic.mgenerate.core.operator.OperatorFactory;

import java.nio.file.Paths;
import java.util.concurrent.Callable;

@Command(name = "mongoetl")
public class Main implements Callable<Integer> {

    static {
        // add mgenerate operators
        OperatorFactory.addBuilders("org.mongodb.etl.operator");
    }

    @Option(names = {"-h", "--help"}, usageHelp = true, description = "display a help message")
    private boolean helpRequested = false;

    public static void main(String[] args) {
        CommandLine cl = new CommandLine(new Main());
        cl.registerConverter(Template.class, s -> Template.from(Paths.get(s)));
        cl.addSubcommand("load", new LoadTemplate());
        cl.addSubcommand("print", new PrintTemplate());
        System.exit(cl.execute(args));
    }

    @Override
    public Integer call() {
        System.out.println(this);
        return 0;
    }

}
