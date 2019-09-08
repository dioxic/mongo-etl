package org.mongodb.etl;

import org.bson.Document;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.IntStream;

public class UEPConstants {

    private ThreadLocal<Random> random = ThreadLocal.withInitial(Random::new);
    private Map<String, List<Object>> weightedConstants;
    private static UEPConstants instance;

    public static UEPConstants load(String filePath) {
        if (instance == null) {
            instance = load(Paths.get(filePath));
        }
        return instance;
    }

    public static UEPConstants load(Path filePath) {
        if (instance == null) {
            try {
                instance = new UEPConstants(Document.parse(Files.readString(filePath)));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        return instance;
    }

    public static UEPConstants instance() {
        if (instance == null) {
            try {
                instance = load(Paths.get(UEPConstants.class.getClassLoader()
                        .getResource("uep-constants.json").toURI()));
            } catch (URISyntaxException e) {
                throw new RuntimeException(e);
            }
        }
        return instance;
    }

    private UEPConstants(Document config) {
        weightedConstants = new HashMap<>(config.size());
        config.forEach((key, value) -> weightedConstants.put(key, parseEntry(value)));
    }

    public Object getRandom(String key) {
        List<Object> weightedArray = weightedConstants.get(key);
        if (weightedArray == null) {
            throw new IllegalArgumentException("key " + key + " not found in constant map");
        }
        return weightedArray.get(random.get().nextInt(weightedArray.size()));
    }

    public int getRandomInt(int upperBound) {
        return random.get().nextInt(upperBound);
    }

    public int getRandomInt(int lowerBound, int upperBound) {
        return lowerBound + getRandomInt(upperBound-lowerBound);
    }

    public long getRandomLong() {
        return random.get().nextLong();
    }

    public boolean getRandomBoolean() {
        return random.get().nextBoolean();
    }

    public long createImsiFromSequence(long seq) {
        List<Object> prefixList = weightedConstants.get("imsi_prefix");
        Integer imsiPrefix = (Integer) prefixList.get((int) (seq % prefixList.size()));
        return (imsiPrefix * 10000000000L) + seq;
    }

    private List<Object> parseEntry(Object v) {
        if (!(v instanceof List)) {
            throw new IllegalStateException("expected an array found: " + v);
        }
        List<?> items = (List<?>) v;
        List<Object> weightedItems = new ArrayList<>(items.size() * 2);

        items.forEach(item -> {
            if (item instanceof Document) {
                Document doc = (Document) item;
                Object value = doc.get("value");
                Integer weight = doc.getInteger("weight");
                if (value == null || weight == null) {
                    throw new IllegalArgumentException("weighting array not valid for item: " + doc.toJson());
                }
                IntStream.range(0, weight).forEach(i -> weightedItems.add(value));
            } else {
                weightedItems.add(item);
            }
        });

        return weightedItems;
    }

    @Override
    public String toString() {
        return weightedConstants.toString();
    }
}
