package org.mongodb.etl;

import org.bson.Document;
import org.bson.json.JsonWriterSettings;
import org.junit.jupiter.api.Test;
import uk.dioxic.mgenerate.core.util.BsonUtil;

import java.net.URISyntaxException;
import java.nio.file.Paths;

public class TimGeneratorTest {

    @Test
    void printTimDefinition() throws URISyntaxException {
        Document definition = BsonUtil.parseFile(Paths.get(getClass().getClassLoader().getResource("tim-definition.json").toURI()));
        JsonWriterSettings jws = JsonWriterSettings.builder().indent(true).build();
        System.out.println(BsonUtil.toJson(definition));
    }
}
