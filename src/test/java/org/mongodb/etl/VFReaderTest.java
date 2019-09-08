package org.mongodb.etl;

import org.junit.jupiter.api.Test;

public class VFReaderTest {

    @Test
    public void testReader() {
        System.out.println("loading test data");
        VFLoader loader = new VFLoader("test.ocomoc");
        loader.populate(1000);

        System.out.println("reading test data");
        VFReaderOpt3 reader = new VFReaderOpt3("test.ocomoc");
        reader.run(1000);
    }

}
