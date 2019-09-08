package org.mongodb.etl;

import com.mongodb.client.model.InsertManyOptions;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;
import com.mongodb.reactivestreams.client.MongoCollection;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.RawBsonDocument;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.function.Function;

public class Runner {

    private MongoClient client;
    private int batchSize = 1000;
    private MongoCollection<BsonDocument> src;
    private MongoCollection<BsonDocument> tgt;
    private InsertManyOptions options;

    static RawBsonDocument d;

    public static void main(String[] args) {
        if (args.length != 3) {
            throw new IllegalArgumentException("expected 2 arguments: <src namespace> <tgt namespace>");
        }

        new Runner(args[0], args[1], args[2]).run();
    }

    public Runner(String mongoUri, String srcNs, String tgtNs) {

        String[] srcSplit = srcNs.split("\\.");
        String[] tgtSplit = tgtNs.split("\\.");

        if (srcSplit.length != 2) {
            throw new IllegalArgumentException("src namespace is not valid (" + srcNs + ")");
        }
        if (tgtSplit.length != 2) {
            throw new IllegalArgumentException("tgt namespace is not valid (" + tgtNs + ")");
        }

        client = MongoClients.create(mongoUri);

        src = client.getDatabase(srcSplit[0]).getCollection(srcSplit[1], BsonDocument.class);
        tgt = client.getDatabase(tgtSplit[0]).getCollection(tgtSplit[1], BsonDocument.class);

        options = new InsertManyOptions();
        options.ordered(false);

    }

    public void populateTestData(int docCount) {

        System.out.println("dropping src collection");
        Mono.from(src.drop()).block();

        long start = System.currentTimeMillis();

        Flux<BsonDocument> docFlux = Flux.generate(
                () -> 1,
                (state, sink) -> {
                    BsonDocument doc = new BsonDocument();
                    doc.put("text", new BsonString("{ f1: \"some string\", f2: " + state + ", f3: \"another string\" }"));
                    sink.next(doc);
                    if (state == docCount)
                        sink.complete();
                    return state+1;
                }
        );

        docFlux
            .buffer(batchSize)
            .flatMap(batch -> src.insertMany(batch, options), 3)
            .blockLast();

        double time = (System.currentTimeMillis() - start) /1000;
        double speed = docCount / time;

        System.out.println("populated " + docCount + " documents in " + Math.round(time) + "s (" + Math.round(speed) + " doc/s)");

    }


    public void run() {
        System.out.println("dropping tgt collection");
        Mono.from(tgt.drop()).block();

        long start = System.currentTimeMillis();
        long docCount = Mono.from(src.estimatedDocumentCount()).block();

        Flux.from(src.find().batchSize(batchSize))  // extract
                .flatMap(transform, 8)          // transform in parallel
                .buffer(batchSize)                         // batch docs together
                .flatMap(batch -> tgt.insertMany(batch, options), 4)  // load docs in parallel
//                .doOnNext(System.out::println)
                .doOnComplete(() -> System.out.println("complete!"))
                .blockLast();

        double time = (System.currentTimeMillis() - start) /1000;
        double speed = docCount / time;

        System.out.println("transformed " + docCount + " documents in " + Math.round(time) + "s (" + Math.round(speed) + " doc/s)");

    }

    /**
     * Takes an incoming document and parses the JSON in the "text" field into a BsonDocument which is then returned.
     */
    Function<BsonDocument, Mono<BsonDocument>> transform = doc -> {
        BsonDocument newDoc = BsonDocument.parse(doc.getString("text").getValue());
        return Mono.just(newDoc);
    };


}
