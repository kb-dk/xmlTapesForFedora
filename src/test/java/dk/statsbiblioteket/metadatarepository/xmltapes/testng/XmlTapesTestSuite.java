package dk.statsbiblioteket.metadatarepository.xmltapes.testng;

import dk.statsbiblioteket.metadatarepository.xmltapes.akubra.XmlTapesBlobStore;
import dk.statsbiblioteket.metadatarepository.xmltapes.cache.CacheForDeferringTaper;
import dk.statsbiblioteket.metadatarepository.xmltapes.redis.RedisIndex;
import dk.statsbiblioteket.metadatarepository.xmltapes.taper.DeferringTaper;
import dk.statsbiblioteket.metadatarepository.xmltapes.tarfiles.TapeArchiveImpl;
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


    public static final String REDIS_HOST = "localhost";
    public static final int REDIS_PORT = 6379;
    public static final int REDIS_DATABASE = 4;
    private static CacheForDeferringTaper archive;

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

        long tapeSize = 1024L * 1024;


        URI storeLocation = getStoreLocation();
        File tapingDir = new File(new File(storeLocation), "tapingDir");
        tapingDir.mkdirs();
        File cachingDir = new File(new File(storeLocation), "cachingDir");
        cachingDir.mkdirs();
        File tempDir = new File(new File(storeLocation), "tempDir");
        tempDir.mkdirs();


        CacheForDeferringTaper temp = new CacheForDeferringTaper(cachingDir, tempDir);
        TapeArchiveImpl tapeArchive = new TapeArchiveImpl(storeLocation, tapeSize, ".tar", "tape", "tempTape");
        DeferringTaper taper = new DeferringTaper(tapingDir);
        taper.setDelay(10);
        taper.setTapeDelay(1000);

        temp.setDelegate(taper);
        taper.setDelegate(tapeArchive);
        taper.setParent(temp);
        archive = temp;

        store.setArchive(archive);
        tapeArchive.setIndex(new RedisIndex(REDIS_HOST, REDIS_PORT, REDIS_DATABASE));
        tapeArchive.rebuild();
        archive.init();
        return store;
    }


    public static void clean() throws IOException, URISyntaxException {

        if (archive != null){
            archive.close();
        }

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


 /*   @Test( dependsOnGroups={ "post" })
    public void testFinal(){

        System.out.println("Final test");
        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }

    }*/


}
