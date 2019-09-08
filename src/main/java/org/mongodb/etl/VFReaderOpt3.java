package org.mongodb.etl;

import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;
import com.mongodb.reactivestreams.client.MongoCollection;
import org.bson.RawBsonDocument;
import reactor.core.publisher.Flux;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static com.mongodb.client.model.Aggregates.match;
import static com.mongodb.client.model.Aggregates.project;
import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Projections.*;

public class VFReaderOpt3 {

    private MongoCollection<RawBsonDocument> src;
    private Random rnd = new Random();

    public static void main(String[] args) {

        if (args.length != 3) {
            throw new IllegalArgumentException("expected 2 arguments: <mongo uri> <namespace> <# docs>");
        }

        new VFReaderOpt3(args[0], args[1]).run(Integer.parseInt(args[2]));
    }

    public VFReaderOpt3(String mongoUri, String srcNs) {
        this(MongoClients.create(mongoUri), srcNs);
    }

    public VFReaderOpt3(String srcNs) {
        this(MongoClients.create(), srcNs);
    }

    public VFReaderOpt3(MongoClient client, String srcNs) {

        String[] srcSplit = srcNs.split("\\.");

        if (srcSplit.length != 2) {
            throw new IllegalArgumentException("src namespace is not valid (" + srcNs + ")");
        }
        src = client.getDatabase(srcSplit[0]).getCollection(srcSplit[1], RawBsonDocument.class);
    }

    public void run(int docNum) {
        long start = System.currentTimeMillis();

        Flux<List<String>> docFlux = Flux.generate(
                () -> 1,
                (state, sink) -> {
                    sink.next(getAllIMSIPrefixes(state));
                    if (state == docNum)
                        sink.complete();
                    return state+1;
                }
        );

        long docCount = docFlux
//                .doOnNext(imsi -> System.out.println("searching for " + imsi))
//                .flatMap(imsi -> src.find(in("TIMS.id", imsi)), 4)
                .flatMap(imsi -> src.aggregate(Arrays.asList(
                        match(in("TIMS.id", imsi)),
                        project(
                                fields(
                                        include("CSP_ID", "NBIOT_MESSAGE_SIZE", "THRESHOLDS"),
                                        computed("TIMS", eq("$filter", and(eq("input", "$TIMS"), eq("as", "tim"), in("cond", Arrays.asList("$$tim.id", imsi)))))
                                )
                        )
                )), 2)
//                .map(doc -> doc.get("_id"))
//                .doOnNext(System.out::println)
                .count()
                .block();

        double time = (double)(System.currentTimeMillis() - start) /1000;
        double speed = docCount / time;

        System.out.println("read " + docCount + " documents in " + Math.round(time) + "s (" + Math.round(speed) + " doc/s)");

    }

    private int createIMSIPrefix() {
        int[] prefixArray = {94721, 3009, 98419, 68754, 51032, 91416, 79307, 21803, 90367, 14782};
        return prefixArray[rnd.nextInt(prefixArray.length)];
    }

    private List<String> getAllIMSIPrefixes(int state) {
        int[] prefixArray = {94721, 3009, 98419, 68754, 51032, 91416, 79307, 21803, 90367, 14782};
        List<String> imsiList = new ArrayList<>(prefixArray.length);

        for (int i: prefixArray) {
            imsiList.add(String.format("%05d%010d", i, state));
        }

        return imsiList;
    }

}
