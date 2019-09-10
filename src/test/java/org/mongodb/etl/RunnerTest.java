package org.mongodb.etl;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

public class RunnerTest {
    Logger logger = LoggerFactory.getLogger(getClass());

    String uri = "mongodb://localhost:27017";

    @Test
    void run() {
        System.out.println("test starting");

//        Runner runner = new Runner(uri, "etl.src", "etl.tgt");
//
//        runner.populateTestData(1000000);
//
//        runner.run();
    }

}
