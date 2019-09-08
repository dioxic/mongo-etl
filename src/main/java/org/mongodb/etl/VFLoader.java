package org.mongodb.etl;

import com.mongodb.client.model.Indexes;
import com.mongodb.client.model.InsertManyOptions;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;
import com.mongodb.reactivestreams.client.MongoCollection;
import org.bson.Document;
import org.bson.RawBsonDocument;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class VFLoader {

    private MongoClient client;
    private int batchSize = 1000;
    private MongoCollection<Document> tgt;
    private InsertManyOptions options;

    public static void main(String[] args) {
        if (args.length != 3) {
            throw new IllegalArgumentException("expected 3 arguments: <mongouri> <namespace> <# documents>");
        }

        new VFLoader(args[0], args[1]).populate(Integer.parseInt(args[2]));
    }

    public VFLoader(String mongoUri, String ns) {
        this(MongoClients.create(mongoUri), ns);
    }

    public VFLoader(String ns) {
        this(MongoClients.create(), ns);
    }

    public VFLoader(MongoClient client, String ns) {

        String[] srcSplit = ns.split("\\.");

        if (srcSplit.length != 2) {
            throw new IllegalArgumentException("src namespace is not valid (" + ns + ")");
        }

        tgt = client.getDatabase(srcSplit[0]).getCollection(srcSplit[1], Document.class);

        options = new InsertManyOptions();
        options.ordered(false);

    }

    public void populate(int docCount) {

        System.out.println("dropping src collection");
        Mono.from(tgt.drop()).block();
        Mono.from(tgt.createIndex(Indexes.ascending("TIMS.id"))).block();

        long start = System.currentTimeMillis();

        VFDataOCOMOCGenerator ocomocGenerator = new VFDataOCOMOCGenerator(20000);

        Flux<Document> docFlux = Flux.generate(
                () -> 1,
                (state, sink) -> {
                    sink.next(ocomocGenerator.createCSP(state));
                    if (state == docCount)
                        sink.complete();
                    return state+1;
                }
        );

        docFlux
            .buffer(batchSize)
            .flatMap(batch -> tgt.insertMany(batch, options), 3)
            .blockLast();

        double time = (System.currentTimeMillis() - start) /1000;
        double speed = docCount / time;

        System.out.println("populated " + docCount + " documents in " + Math.round(time) + "s (" + Math.round(speed) + " doc/s)");

    }

}
