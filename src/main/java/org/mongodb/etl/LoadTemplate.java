package org.mongodb.etl;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.model.InsertManyOptions;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;
import com.mongodb.reactivestreams.client.MongoCollection;
import org.bson.RawBsonDocument;
import org.bson.UuidRepresentation;
import org.bson.codecs.UuidCodecProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import uk.dioxic.mgenerate.core.Template;
import uk.dioxic.mgenerate.core.codec.MgenDocumentCodec;
import uk.dioxic.mgenerate.core.codec.TemplateCodec;

import java.util.UUID;
import java.util.concurrent.Callable;

import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;
import static picocli.CommandLine.Command;
import static picocli.CommandLine.Option;

@Command(name = "load", description = "loads random documents into a mongo collection based on an mgenerate template")
public class LoadTemplate implements Callable<Integer> {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Option(names = {"-h", "--help"}, usageHelp = true, description = "display a help message")
    private boolean helpRequested = false;

    @Option(names = {"--drop"})
    private boolean drop = false;

    @Option(names = {"-d", "--database"}, description = "mongo database", defaultValue = "test")
    private String database;

    @Option(names = {"-c", "--collection"}, description = "mongo collection", required = true)
    private String collection;

    @Option(names = {"-u", "--uri"}, description = "mongoUri", defaultValue = "mongodb://localhost:27017")
    private String mongoUri;

    @Option(names = {"-n", "--number"}, description = "number of documents to generate (default: ${DEFAULT-VALUE})", defaultValue = "5")
    private Integer number;

    @Option(names = {"-t", "--template"}, description = "template file", required = true, converter = TemplateTypeConverter.class)
    private Template template;

    @Option(names = {"-b", "--batchSize"}, description = "insert batch size")
    private int batchSize = 1000;

    @Option(names = {"--concurrency"}, description = "reactive flapmap concurrency (default: ${DEFAULT-VALUE})", defaultValue = "3")
    private int concurrency;

    private InsertManyOptions options;

    LoadTemplate() {
        options = new InsertManyOptions();
        options.ordered(false);
//        OperatorFactory.addBuilder(Imsi);
    }

    @Override
    public Integer call() {
        MongoClientSettings mcs = MongoClientSettings.builder()
                .codecRegistry(MgenDocumentCodec.getCodecRegistry())
                .applyConnectionString(new ConnectionString(mongoUri))
                .build();

        MongoCollection<Template> mc = MongoClients.create(mcs)
                .getDatabase(database)
                .getCollection(collection, Template.class);

        if (drop) {
            logger.info("dropping collection {}", collection);
            Mono.from(mc.drop()).block();
        }

        Scheduler s = Schedulers.newElastic("executorPool");

        long start = System.currentTimeMillis();

        Flux.range(0, number)
                .publishOn(s)
                .map(i -> template)
                //.map(Template::toRawBson)
                .buffer(batchSize)
                .flatMap(batch -> mc.insertMany(batch, options), concurrency)
                .blockLast();

        double time = (double) (System.currentTimeMillis() - start) / 1000;
        double speed = number / time;

        System.out.println("populated " + number + " documents in " + Math.round(time) + "s (" + Math.round(speed) + " doc/s)");

        s.dispose();
        return 0;
    }
}