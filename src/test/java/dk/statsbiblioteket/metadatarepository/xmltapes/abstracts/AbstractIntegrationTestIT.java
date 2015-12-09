package dk.statsbiblioteket.metadatarepository.xmltapes.abstracts;

import dk.statsbiblioteket.metadatarepository.xmltapes.IntegrationTestImmediateReporter;
import dk.statsbiblioteket.metadatarepository.xmltapes.TestUtils;
import dk.statsbiblioteket.metadatarepository.xmltapes.akubra.XmlTapesBlobStore;
import dk.statsbiblioteket.metadatarepository.xmltapes.cacheStore.CacheStore;
import dk.statsbiblioteket.metadatarepository.xmltapes.common.AkubraCompatibleArchive;
import dk.statsbiblioteket.metadatarepository.xmltapes.common.index.Index;
import dk.statsbiblioteket.metadatarepository.xmltapes.tapingStore.Taper;
import dk.statsbiblioteket.metadatarepository.xmltapes.tapingStore.TapingStore;
import dk.statsbiblioteket.metadatarepository.xmltapes.tarfiles.TapeArchiveImpl;
import org.akubraproject.Blob;
import org.akubraproject.BlobStore;
import org.akubraproject.BlobStoreConnection;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Listeners;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Date;
import java.util.Iterator;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

@Listeners(IntegrationTestImmediateReporter.class)
public abstract class AbstractIntegrationTestIT {

    private AkubraCompatibleArchive archive;
    private Index index;
    private BlobStore blobStore;

    @BeforeMethod
    public void startArchive() throws Exception {
        clean();
        blobStore = openArchive();
    }

    @AfterMethod
    public void clean() throws IOException, URISyntaxException {
        if (archive != null) {
            archive.close();
        }
        if(index != null) {
            index.clear();
        }

        File archiveFolder = getPrivateStoreId();
        FileUtils.cleanDirectory(archiveFolder);
        FileUtils.touch(new File(archiveFolder, "empty"));
    }

    private  File getPrivateStoreId() throws URISyntaxException {
        File archiveFolder = new File(Thread.currentThread().getContextClassLoader().getResource("archive/empty").toURI()).getParentFile();
        archiveFolder = new File(archiveFolder,getClass().getSimpleName());
        archiveFolder.mkdirs();
        return archiveFolder;
    }


    public BlobStore openArchive() throws URISyntaxException, IOException {
        File store = getPrivateStoreId();
        long tapeSize = 1024L * 1024*10;
        //Create the blobstore
        XmlTapesBlobStore xmlTapesBlobStore = new XmlTapesBlobStore(URI.create("test:tapestorage"));
        //create the cacheStore
        File cachingDir = TestUtils.mkdir(store, "cachingDir");
        File tempDir = TestUtils.mkdir(store, "tempDir");
        CacheStore cacheStore = new CacheStore(cachingDir, tempDir);
        //create the tapingStore
        File tapingDir = TestUtils.mkdir(store, "tapingDir");
        TapingStore tapingStore = new TapingStore(tapingDir);
        tapingStore.setCache(cacheStore);
        tapingStore.setDelay(100);
        cacheStore.setDelegate(tapingStore);

        //create the TapeArchive
        TapeArchiveImpl tapeArchive = new TapeArchiveImpl(store, tapeSize, ".tar", "tape", "tempTape");
        tapeArchive.setRebuild(true);
        //RedisIndex redis = new RedisIndex(REDIS_HOST, REDIS_PORT, REDIS_DATABASE, new JedisPoolConfig());
        index = getIndex();
        tapeArchive.setIndex(index);
        tapingStore.setDelegate(tapeArchive);
        Taper taper = new Taper(tapingStore, cacheStore, tapeArchive);
        taper.setTapeDelay(1000);
        tapingStore.setTask(taper);
        archive = cacheStore;
        archive.init();
        xmlTapesBlobStore.setArchive(archive);
        return xmlTapesBlobStore;
    }

    protected abstract Index getIndex();


    public BlobStoreConnection openConnection() throws Exception {
        return blobStore.openConnection(null, null);
    }



    @Test
    public void testPutAndGetBlob() throws Exception {
        BlobStoreConnection c = openConnection();
        final URI blob11 = new URI("Blob1");
        try {
            InputStream resourceAsStream = Thread.currentThread().getContextClassLoader().getResourceAsStream("Blob1");
            byte[] contents = IOUtils.toString(resourceAsStream).getBytes();
            //reopen
            resourceAsStream = Thread.currentThread().getContextClassLoader().getResourceAsStream("Blob1");
            create(c, blob11, contents);
            byte[] storedContents = retrieve(c, blob11);
            assertThat(storedContents, is(contents));
        } finally {
            delete(c, blob11);
            c.close();
        }
    }


    @Test
    public void test3Blobls() throws Exception {
        BlobStoreConnection c = openConnection();
        try {
            final URI blob1 = new URI("Blob1");
            final URI blob2 = new URI("Blob2");
            final URI blob3 = new URI("Blob3");
            create(c, blob1, "11111".getBytes());
            create(c, blob2, "22222".getBytes());
            create(c, blob3, "33333".getBytes());
            delete(c, blob1);
            delete(c, blob2);
            delete(c, blob3);
            Iterator<URI> blobs = c.listBlobIds("");
            assertThat(blobs.hasNext(), is(false));
        } finally {
            c.close();
        }
    }


    @Test
    public void test3BloblsConnections() throws Exception {
        BlobStoreConnection c = openConnection();
        try {
            final URI blob1 = new URI("Blob1");
            final URI blob2 = new URI("Blob2");
            final URI blob3 = new URI("Blob3");
            create(c, blob1, "11111".getBytes());
            create(c, blob2, "22222".getBytes());
            create(c, blob3, "33333".getBytes());
            c.close();
            c = openConnection();
            assertThat(retrieve(c, blob1), is("11111".getBytes()));
            assertThat(retrieve(c, blob2), is("22222".getBytes()));
            assertThat(retrieve(c, blob3), is("33333".getBytes()));
            c.close();
            c = openConnection();
            delete(c, blob1);
            delete(c, blob2);
            delete(c, blob3);
            Iterator<URI> blobs = c.listBlobIds("");
            assertThat(blobs.hasNext(), is(false));
            try {
                retrieve(c, blob1);
                Assert.fail();
            } catch (Exception e) {
            }
        } finally {
            c.close();
        }
    }


    @Test
    public void test3BloblsConnectionsSleep() throws Exception {
        BlobStoreConnection c = openConnection();
        try {
            final URI blob1 = new URI("Blob1");
            final URI blob2 = new URI("Blob2");
            final URI blob3 = new URI("Blob3");
            create(c, blob1, "11111".getBytes());
            create(c, blob2, "22222".getBytes());
            create(c, blob3, "33333".getBytes());
            c = closeOpen(c);
            assertThat(retrieve(c, blob1), is("11111".getBytes()));
            assertThat(retrieve(c, blob2), is("22222".getBytes()));
            assertThat(retrieve(c, blob3), is("33333".getBytes()));
            c = closeOpen(c);
            delete(c, blob1);
            delete(c, blob2);
            delete(c, blob3);
            c = closeOpen(c);
            Iterator<URI> blobs = c.listBlobIds("");
            assertThat(blobs.hasNext(), is(false));
            try {
                retrieve(c, blob1);
                Assert.fail();
            } catch (Exception e) {
            }
        } finally {
            c.close();
        }
    }

    @Test
    public void test3BloblsConnectionsSleepRecreate() throws Exception {
        BlobStoreConnection c = openConnection();
        try {
            final URI blob1 = new URI("Blob1");
            final URI blob2 = new URI("Blob2");
            final URI blob3 = new URI("Blob3");
            create(c, blob1, "11111".getBytes());
            create(c, blob2, "22222".getBytes());
            create(c, blob3, "33333".getBytes());
            c = closeOpen(c);
            assertThat(retrieve(c, blob1), is("11111".getBytes()));
            assertThat(retrieve(c, blob2), is("22222".getBytes()));
            assertThat(retrieve(c, blob3), is("33333".getBytes()));
            c = closeOpen(c);
            delete(c, blob1);
            delete(c, blob2);
            delete(c, blob3);
            c = closeOpen(c);
            Iterator<URI> blobs = c.listBlobIds("");
            assertThat(blobs.hasNext(), is(false));
            create(c, blob1, "11111".getBytes());
            create(c, blob2, "22222".getBytes());
            create(c, blob3, "33333".getBytes());
            c = closeOpen(c);
            update(c, blob1, "111111".getBytes());
            update(c, blob2, "222222".getBytes());
            update(c, blob3, "333333".getBytes());
            c = closeOpen(c);
            assertThat(retrieve(c, blob1), is("111111".getBytes()));
            assertThat(retrieve(c, blob2), is("222222".getBytes()));
            assertThat(retrieve(c, blob3), is("333333".getBytes()));
            c = closeOpen(c);
            delete(c, blob1);
            delete(c, blob2);
            delete(c, blob3);
            c = closeOpen(c);
            try {
                retrieve(c, blob1);
                Assert.fail();
            } catch (Exception e) {
            }
        } finally {
            c.close();
        }
    }


    @Test
    public void test2BloblsConnectionsSleepRecreate() throws Exception {
        BlobStoreConnection c1 = openConnection();
        BlobStoreConnection c2 = openConnection();
        try {
            final URI blob1 = new URI("Blob1");
            final URI blob2 = new URI("Blob2");
            create(c1, blob1, "11111".getBytes());
            create(c2, blob2, "22222".getBytes());
            assertThat(retrieve(c2, blob1), is("11111".getBytes()));
            assertThat(retrieve(c1, blob2), is("22222".getBytes()));
            c1 = closeOpen(c1);
            assertThat(retrieve(c2, blob1), is("11111".getBytes()));
            assertThat(retrieve(c1, blob2), is("22222".getBytes()));
            assertThat(retrieve(c1, blob1), is("11111".getBytes()));
            assertThat(retrieve(c2, blob2), is("22222".getBytes()));
            c2 = closeOpen(c2);
            delete(c2, blob1);
            delete(c2, blob2);
            Iterator<URI> blobs = c1.listBlobIds("");
            assertThat(blobs.hasNext(), is(false));
            c1 = closeOpen(c1);
            blobs = c1.listBlobIds("");
            assertThat(blobs.hasNext(), is(false));
            create(c2, blob1, "11111".getBytes());
            create(c2, blob2, "22222".getBytes());
            c2 = closeOpen(c2);
            update(c1, blob1, "111111".getBytes());
            update(c1, blob2, "222222".getBytes());
            c1 = closeOpen(c1);
            assertThat(retrieve(c1, blob1), is("111111".getBytes()));
            assertThat(retrieve(c2, blob2), is("222222".getBytes()));
            c2 = closeOpen(c2);
            delete(c1, blob1);
            delete(c2, blob2);
            c1 = closeOpen(c1);
            try {
                retrieve(c1, blob1);
                Assert.fail();
            } catch (Exception e) {
            }
        } finally {
            c1.close();
            c2.close();
        }
    }

    @Test
    public void testThreads() throws Exception {
        final BlobStoreConnection c = openConnection();
        Runnable runner = new Runnable() {
            @Override
            public void run() {
                for (int i = 0; i < 10; i++) {
                    URI blobId = URI.create("uuid:" + UUID.randomUUID().toString());
                    byte[] contents = new byte[1024 * 1024];
                    new Random(new Date().getTime()).nextBytes(contents);
                    try {
                        create(c, blobId, contents);
                        byte[] storedContents = retrieve(c, blobId);
                        assertThat(storedContents, is(contents));
                        delete(c, blobId);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        };
        ExecutorService pool = Executors.newFixedThreadPool(4);
        Future<?> thread1 = pool.submit(runner);
        Future<?> thread2 = pool.submit(runner);
        Future<?> thread3 = pool.submit(runner);
        Future<?> thread4 = pool.submit(runner);
        pool.shutdown();
        pool.awaitTermination(1, TimeUnit.HOURS);
        assertThat(thread1.get(),is(nullValue()));
        assertThat(thread2.get(), is(nullValue()));
        assertThat(thread3.get(), is(nullValue()));
        assertThat(thread4.get(), is(nullValue()));
        Iterator<URI> blobs = c.listBlobIds("");
        boolean some = false;
        while (blobs.hasNext()) {
            some = true;
            URI next = blobs.next();
            assertThat(retrieve(c,next),is(notNullValue()));
            System.out.println(next);
        }
        assertThat(some, is(false));
        c.close();

    }

    @Test
    public void testRebuild() throws Exception {
        final BlobStoreConnection c = openConnection();
        Runnable runner = new Runnable() {
            @Override
            public void run() {
                for (int i = 0; i < 5; i++) {
                    URI blobId = URI.create("uuid:" + UUID.randomUUID().toString());
                    byte[] contents = new byte[1024 * 1024];
                    new Random(new Date().getTime()).nextBytes(contents);
                    try {
                        create(c, blobId, contents);
                        byte[] storedContents = retrieve(c, blobId);
                        assertThat(storedContents, is(contents));
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        };
        ExecutorService pool = Executors.newFixedThreadPool(4);
        Future<?> thread1 = pool.submit(runner);
        Future<?> thread2 = pool.submit(runner);
        Future<?> thread3 = pool.submit(runner);
        Future<?> thread4 = pool.submit(runner);
        pool.shutdown();
        pool.awaitTermination(1, TimeUnit.HOURS);
        assertThat(thread1.get(), is(nullValue()));
        assertThat(thread2.get(), is(nullValue()));
        assertThat(thread3.get(), is(nullValue()));
        assertThat(thread4.get(), is(nullValue()));
        c.close();

        //Here the archive is closed. Openarchive opens a new archive, which triggers a rebuild
        archive.close();
        index.clear();
        blobStore = openArchive();

        BlobStoreConnection c2 = openConnection();
        Iterator<URI> blobs = c2.listBlobIds("");
        int i = 0;
        while (blobs.hasNext()) {
            URI next = blobs.next();
            assertThat(retrieve(c2, next), is(notNullValue()));
            delete(c2, next);
            i++;
        }
        assertThat(i,is(20));//we found 20 records
        assertThat(c2.listBlobIds("").hasNext(), is(false)); //they are all now deleted
        c2.close();


        archive.close();
        index.clear();
        blobStore = openArchive();

        BlobStoreConnection c3 = openConnection();
        assertThat(c3.listBlobIds("").hasNext(), is(false));
        c3.close();
    }


    private BlobStoreConnection closeOpen(BlobStoreConnection c) throws Exception {
        c.sync();
        c.close();
        Thread.sleep(600);
        assertThat(c.isClosed(), is(true));
        c = openConnection();
        return c;
    }


    public static URI create(BlobStoreConnection con, URI key, byte[] contents) throws IOException {
        Blob blob = con.getBlob(key, null);
        OutputStream outputstream = blob.openOutputStream(contents.length, false);
        try {
            IOUtils.copyLarge(new ByteArrayInputStream(contents), outputstream);
        } finally {
            IOUtils.closeQuietly(outputstream);
        }
        if (!blob.exists()) {
            throw new RuntimeException("Blob does not exist after creation");
        }
        if (blob.getSize() != contents.length) {
            throw new RuntimeException("Blob have wrong size");
        }
        return blob.getId();
    }

    public static byte[] retrieve(BlobStoreConnection con, URI key) throws IOException {
        Blob blob = con.getBlob(key, null);
        final InputStream input = blob.openInputStream();
        try {
            return IOUtils.toByteArray(input);
        } finally {
            IOUtils.closeQuietly(input);
        }
    }

    public static URI update(BlobStoreConnection con, URI key, byte[] contents) throws IOException {
        Blob blob = con.getBlob(key, null);
        if (!blob.exists()) {
            throw new RuntimeException("Blob does not exist");
        }
        OutputStream outputstream = blob.openOutputStream(contents.length, true);
        try {
            IOUtils.copyLarge(new ByteArrayInputStream(contents), outputstream);
        } finally {
            IOUtils.closeQuietly(outputstream);
        }
        if (!blob.exists()) {
            throw new RuntimeException("Blob does not exist after creation");
        }
        if (blob.getSize() != contents.length) {
            throw new RuntimeException("Blob have wrong size");
        }
        return blob.getId();
    }

    public static URI delete(BlobStoreConnection con, URI key) throws IOException {
        Blob blob = con.getBlob(key, null);
        blob.delete();
        if (blob.exists()) {
            throw new RuntimeException("Blob exists after delete");
        }
        return blob.getId();
    }
}
