package dk.statsbiblioteket.metadatarepository.xmltapes.junit;

import java.io.File;
import java.net.URI;

public class TestUtils {
    public static File mkdir(URI store, String dirname) {
        File dir = new File(new File(store), dirname);
        dir.mkdirs();
        return dir;
    }
}
