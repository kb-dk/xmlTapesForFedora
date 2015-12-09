package dk.statsbiblioteket.metadatarepository.xmltapes.postgres;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;

import dk.statsbiblioteket.metadatarepository.xmltapes.abstracts.AbstractXmlTapesTestSuiteIT;

public class PostgresXmlTapesTestSuiteIT extends AbstractXmlTapesTestSuiteIT {


    public PostgresXmlTapesTestSuiteIT() throws IOException, URISyntaxException {
        super(PostgresTestSettings.getIndex(),getStoreLocation());
    }

    private static File getStoreLocation() throws URISyntaxException {
        File archiveFolder = new File(Thread.currentThread().getContextClassLoader().getResource("archive/empty").toURI()).getParentFile();
        archiveFolder = new File(archiveFolder,PostgresXmlTapesTestSuiteIT.class.getSimpleName());
        archiveFolder.mkdirs();
        return archiveFolder;
    }
}
