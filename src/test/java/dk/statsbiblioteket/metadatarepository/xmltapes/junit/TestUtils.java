package dk.statsbiblioteket.metadatarepository.xmltapes.junit;

import java.io.File;

public class TestUtils {
    public static File mkdir(File store, String dirname) {
        File dir = new File(store, dirname);
        dir.mkdirs();
        return dir;
    }
}
