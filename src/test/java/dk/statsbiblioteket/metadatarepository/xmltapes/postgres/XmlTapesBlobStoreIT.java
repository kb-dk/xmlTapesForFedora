package dk.statsbiblioteket.metadatarepository.xmltapes.postgres;

import dk.statsbiblioteket.metadatarepository.xmltapes.TestNamesListener;
import dk.statsbiblioteket.metadatarepository.xmltapes.common.index.Index;
import dk.statsbiblioteket.metadatarepository.xmltapes.abstracts.AbstractXmlTapesBlobStoreIT;
import dk.statsbiblioteket.metadatarepository.xmltapes.postgres.PostgresTestSettings;
import org.testng.annotations.Listeners;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertTrue;

public class XmlTapesBlobStoreIT extends AbstractXmlTapesBlobStoreIT {

    @Override
    protected Index getIndex() {
        return PostgresTestSettings.getIndex();
    }

}
