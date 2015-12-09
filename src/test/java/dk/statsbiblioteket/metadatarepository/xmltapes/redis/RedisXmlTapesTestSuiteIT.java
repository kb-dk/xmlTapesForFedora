package dk.statsbiblioteket.metadatarepository.xmltapes.redis;

import dk.statsbiblioteket.metadatarepository.xmltapes.abstracts.AbstractXmlTapesTestSuiteIT;
import dk.statsbiblioteket.metadatarepository.xmltapes.akubra.XmlTapesBlobStore;
import dk.statsbiblioteket.metadatarepository.xmltapes.cacheStore.CacheStore;
import dk.statsbiblioteket.metadatarepository.xmltapes.common.TapeArchive;
import dk.statsbiblioteket.metadatarepository.xmltapes.common.index.Index;
import dk.statsbiblioteket.metadatarepository.xmltapes.TestUtils;
import dk.statsbiblioteket.metadatarepository.xmltapes.postgres.PostgresTestSettings;
import dk.statsbiblioteket.metadatarepository.xmltapes.redis.RedisIndex;
import dk.statsbiblioteket.metadatarepository.xmltapes.redis.RedisTestSettings;
import dk.statsbiblioteket.metadatarepository.xmltapes.tapingStore.Taper;
import dk.statsbiblioteket.metadatarepository.xmltapes.tapingStore.TapingStore;
import dk.statsbiblioteket.metadatarepository.xmltapes.tarfiles.TapeArchiveImpl;
import org.akubraproject.BlobStore;
import org.akubraproject.tck.TCKTestSuite;
import org.apache.commons.io.FileUtils;
import org.testng.annotations.AfterSuite;
import redis.clients.jedis.JedisPoolConfig;

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
public class RedisXmlTapesTestSuiteIT extends AbstractXmlTapesTestSuiteIT {

    public RedisXmlTapesTestSuiteIT() throws IOException, URISyntaxException {
        super(RedisTestSettings.getIndex(),getStoreLocation());
    }

    private static File getStoreLocation() throws URISyntaxException {
        File archiveFolder = new File(Thread.currentThread().getContextClassLoader().getResource("archive/empty").toURI()).getParentFile();
        archiveFolder = new File(archiveFolder,RedisXmlTapesTestSuiteIT.class.getSimpleName());
        archiveFolder.mkdirs();
        return archiveFolder;
    }
}
