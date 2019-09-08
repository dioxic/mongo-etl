package org.mongodb.etl;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URISyntaxException;

import static org.assertj.core.api.Assertions.*;

public class UEPConstantsTest {

    @Test
    void loadTest() {
        try {
            UEPConstants constants = UEPConstants.instance();
            System.out.println(constants);
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test
    void testRandomGet() {
        try {
            UEPConstants constants = UEPConstants.instance();

            String type = constants.getRandom("threshold_types").toString();
            assertThat(type).isIn("min", "compound", "standard");

            Object vfTest = constants.getRandom("vf_test");
            assertThat(vfTest).isIn(true, false);

            System.out.println(type);
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void createImsiFromSequence() {
        UEPConstants constants = UEPConstants.instance();

        long imsi = constants.createImsiFromSequence(1);

        assertThat(imsi).isGreaterThan(1000);

        System.out.println(imsi);

    }
}
