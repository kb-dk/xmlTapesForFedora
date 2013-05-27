package dk.statsbiblioteket.metadatarepository.xmltapes;

import dk.statsbiblioteket.metadatarepository.xmltapes.redis.RedisIndex;
import org.akubraproject.BlobStore;
import org.akubraproject.tck.TCKTestSuite;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * Created with IntelliJ IDEA.
 * User: abr
 * Date: 5/21/13
 * Time: 2:53 PM
 * To change this template use File | Settings | File Templates.
 */
public class XmlTapesTestSuite extends TCKTestSuite {




    public XmlTapesTestSuite() throws IOException, URISyntaxException {
        super(getPrivateStore(), getPrivateStoreId(), false, false);

    }

    private static URI getStoreLocation() throws URISyntaxException {
        URI archiveFolder = new File(Thread.currentThread().getContextClassLoader().getResource("archive/empty").toURI()).getParentFile().toURI();
        return archiveFolder;
    }

    private static URI getPrivateStoreId() throws URISyntaxException {
        URI name = URI.create("test:storageForTapes");
        return name;
    }

    public static BlobStore getPrivateStore() throws URISyntaxException, IOException {
        clean();

        XmlTapesBlobStore store = new XmlTapesBlobStore(getPrivateStoreId());

        store.setArchive(new ZipArchive(getStoreLocation(),1024*1024));
        store.getArchive().setIndex(new RedisIndex("localhost",6379));
        store.getArchive().init();
        return store;
    }


    public static void clean() throws IOException, URISyntaxException {
        File archiveFolder = new File(getStoreLocation());
        FileUtils.cleanDirectory(archiveFolder);
        FileUtils.touch(new File(archiveFolder, "empty"));

    }



    @Override
    protected URI getInvalidId() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    protected URI[] getAliases(URI uri) {
        return null;
    }


}
