package dk.statsbiblioteket.metadatarepository.xmltapes.postgres;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import dk.statsbiblioteket.metadatarepository.xmltapes.TestNamesListener;
import dk.statsbiblioteket.metadatarepository.xmltapes.abstracts.AbstractXmlTapesTestSuiteIT;
import org.akubraproject.BlobStore;
import org.akubraproject.tck.TCKTestSuite;
import org.apache.commons.io.FileUtils;
import org.testng.annotations.AfterSuite;

import dk.statsbiblioteket.metadatarepository.xmltapes.TestUtils;
import dk.statsbiblioteket.metadatarepository.xmltapes.akubra.XmlTapesBlobStore;
import dk.statsbiblioteket.metadatarepository.xmltapes.cacheStore.CacheStore;
import dk.statsbiblioteket.metadatarepository.xmltapes.common.TapeArchive;
import dk.statsbiblioteket.metadatarepository.xmltapes.common.index.Index;
import dk.statsbiblioteket.metadatarepository.xmltapes.postgres.PostgresTestSettings;
import dk.statsbiblioteket.metadatarepository.xmltapes.tapingStore.Taper;
import dk.statsbiblioteket.metadatarepository.xmltapes.tapingStore.TapingStore;
import dk.statsbiblioteket.metadatarepository.xmltapes.tarfiles.TapeArchiveImpl;
import org.testng.annotations.Listeners;

public class XmlTapesTestSuiteIT extends AbstractXmlTapesTestSuiteIT {


    public XmlTapesTestSuiteIT() throws IOException, URISyntaxException {
        super(PostgresTestSettings.getIndex());
    }
}
