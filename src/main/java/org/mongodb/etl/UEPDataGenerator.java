package org.mongodb.etl;

import org.bson.Document;
import sun.java2d.marlin.DPathConsumer2D;

public class UEPDataGenerator {

    private UEPConstants constants = UEPConstants.instance();

    public Document createTim(long seq) {
        long imsi = constants.createImsiFromSequence(seq);

        Document tim = new Document();
        tim.put("imsi", imsi);
        tim.put("customer_id", constants.getRandomLong() );
        tim.put("csp_id", constants.getRandomLong() );
        tim.put("state", constants.getRandom("state") );
        tim.put("sim_identifier", imsi );
        tim.put("vf_test", constants.getRandom("vf_test") );
        tim.put("last_updated", null );
        tim.put("device_group", null );
        tim.put("minThresholds", null );
        tim.put("usage", null );

        return tim;
    }

    public Document createThreshold() {
        return null;
    }
}
