package dk.statsbiblioteket.metadatarepository.xmltapes.common;

import dk.statsbiblioteket.metadatarepository.xmltapes.TapeUtils;
import dk.statsbiblioteket.metadatarepository.xmltapes.common.index.Entry;
import dk.statsbiblioteket.metadatarepository.xmltapes.common.index.Index;
import org.kamranzafar.jtar.TarEntry;
import org.kamranzafar.jtar.TarHeader;
import org.kamranzafar.jtar.TarOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.LinkedList;

/**
 * This is a special outputstream. This class ensures that bytes written are buffered in memory, and only written to
 * the archive when the stream is closed. This is because the entry needs to know the length before writing.
 * When created, will lock the store for writing. Will unlock the store when closed. This is to ensure a that
 * the sequence of tapes is not disturbed.
 * When the stream is closed and have written the record, the index is updated with the new location.
 */
public class TapeOutputStream extends TarOutputStream {

    private static final Logger log = LoggerFactory.getLogger(TapeOutputStream.class);


    private static final int CAPACITY = 1 * 1024 * 1024;
    private Entry entry;
    private final URI id;
    private final Index index;
    private StoreLock writeLock;

    boolean closing = false;
    boolean closed = false;

    private LinkedList<ByteBuffer> buffer;

    private ByteBuffer lastBuffer;

    /**
     * Create a new TapeOutputStream
     * @param delegate the outputstream to delegate the writing to
     * @param entry the index entry
     * @param id the fedora ID
     * @param index the index instance
     * @param writeLock the writelock object

     */
    public TapeOutputStream(OutputStream delegate, Entry entry, URI id, Index index, StoreLock writeLock)  {
        super(delegate);

        log.trace("Opening an outputstream for the id {} and trying to acquire lock",id);

        this.entry = entry;
        this.id = id;
        this.index = index;
        this.writeLock = writeLock;
        buffer = new LinkedList<ByteBuffer>();
        buffer.add(ByteBuffer.allocate(CAPACITY));
        lastBuffer = buffer.getLast();

        this.writeLock.lock(Thread.currentThread());

        log.trace("Store write lock acquired for id {}",id);

    }




    protected void checkWriting(int len) throws IOException {
        if (closed){
            throw new IOException("Stream closed");
        }
        if (closing){
            return;
        }

        if (len > lastBuffer.remaining()){ //close this buffer and allocate a new one fitting the size min CAPACITY

            int size = (len > CAPACITY ? len : CAPACITY);
            ByteBuffer byteBuffer = ByteBuffer.allocate(size);
            buffer.add(byteBuffer);
            lastBuffer = byteBuffer;
            log.debug("Allocating a new buffer of size {} for record {}",size,id);
        }
    }

    @Override
    public  void write(int b) throws IOException {
        checkWriting(1);
        if (closing){
            super.write(b);
            return;
        }
        lastBuffer.put((byte) b);
    }


    @Override
    public  void write(byte[] b) throws IOException {
        write(b, 0, b.length);
    }

    /**
     * Write a set of  bytes to the stream.
     * If the stream is open, put the bytes in the buffer, if nessesary allocate more buffers
     * If the stream is closing, write the bytes to super
     * if the stream is closed, throw IOException
     * @param b the byte to write
     * @throws IOException
     */
    @Override
    public synchronized void write(byte[] b, int off, int len) throws IOException {
        checkWriting(len);
        if (closing){
            super.write(b, off, len);
            return;
        }
        lastBuffer.put(b,off,len);


    }

    @Override
    public synchronized void close() throws IOException {
        log.trace("Closing the record {}",id);

        closing = true;//From now on, writes go the the delegate, not the buffer
        long timestamp = System.currentTimeMillis();
        long size = calcSizeOfBuffers();

        TarHeader tarHeader = TarHeader.createHeader(TapeUtils.toFilename(id),size,timestamp/1000,false);
        TarEntry entry = new TarEntry(tarHeader);
        putNextEntry(entry);
        for (ByteBuffer byteBuffer : buffer) {
            if (byteBuffer.hasArray()){
                write(byteBuffer.array(), 0, byteBuffer.position());
            } else {
                byte[] temp = new byte[4*1024];

                int remaining = byteBuffer.limit(byteBuffer.position()).rewind().remaining();
                while (remaining > 0){
                    int length = (remaining > temp.length ? remaining : temp.length);
                    byteBuffer.get(temp,0,length);
                    write(temp,0,length);
                    remaining = byteBuffer.remaining();
                }

            }
        }
        closeCurrentEntry();
        out.close();
        index.addLocation(id, this.entry,timestamp); //Update the index to the newly written entry
        closed = true; //Now we cannot write anymore
        writeLock.unlock(Thread.currentThread()); //unlock the storage system, we are done
    }

    /**
     * Calc the size of the buffers
     * @return
     */
    private long calcSizeOfBuffers() {
        long size = 0;
        for (ByteBuffer byteBuffer : buffer) {
            size += byteBuffer.position();
        }
        return size;
    }


    /**
     * Close the stream and mark this entry as a "delete" record.
     */
    public synchronized void delete() throws IOException {
        closing = true;//From now on, writes go the the delegate, not the buffer
        TarHeader tarHeader = TarHeader.createHeader(TapeUtils.toDeleteFilename(id),0,System.currentTimeMillis()/1000,false);
        TarEntry entry = new TarEntry(tarHeader);
        putNextEntry(entry);
        closeCurrentEntry();
        out.close();
        index.remove(id);
        closed = true; //Now we cannot write anymore
        writeLock.unlock(Thread.currentThread()); //unlock the storage system, we are done
    }
}
