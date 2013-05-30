package dk.statsbiblioteket.metadatarepository.xmltapes.common;

import dk.statsbiblioteket.metadatarepository.xmltapes.TapeUtils;
import dk.statsbiblioteket.metadatarepository.xmltapes.common.index.Entry;
import dk.statsbiblioteket.metadatarepository.xmltapes.common.index.Index;
import org.kamranzafar.jtar.TarEntry;
import org.kamranzafar.jtar.TarHeader;
import org.kamranzafar.jtar.TarOutputStream;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * This is a special outputstream. This class ensures that bytes written are buffered in memory, and only written to
 * the archive when the stream is closed. This is because the entry needs to know the length before writing.
 * When created, will lock the store for writing. Will unlock the store when closed. This is to ensure a that
 * the sequence of tapes is not disturbed.
 * When the stream is closed and have written the record, the index is updated with the new location.
 */
public class TapeOutputStream extends TarOutputStream {

    private static final int CAPACITY = 1 * 1024 * 1024;
    private Entry entry;
    private final URI id;
    private final Index index;
    private StoreLock writeLock;

    boolean closing = false;
    boolean closed = false;

    private List<ByteBuffer> buffer;

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

        this.entry = entry;
        this.id = id;
        this.index = index;
        this.writeLock = writeLock;
        buffer = new LinkedList<ByteBuffer>();
        buffer.add(ByteBuffer.allocate(CAPACITY));

        this.writeLock.lock(Thread.currentThread());

    }


    /**
     * Write a single byte the the stream.
     * If the stream is open, put the byte in the buffer
     * If the stream is closing, write the byte to delegate
     * if the stream is closed, throw IOException
     * @param b the byte to write
     * @throws IOException
     */
    @Override
    public synchronized void write(int b) throws IOException {

        if (closed){
            throw new IOException("Stream closed");
        }

        if (closing){
            super.write(b);
            return;
        }

        ByteBuffer lastBuffer = getBuffer();
        lastBuffer.put((byte) b);
    }

    /**
     * Get the latest buffer. If the buffer does not have at least one available, allocate a new buffer.
     * @return
     */
    private ByteBuffer getBuffer() {
        ByteBuffer lastBuffer = buffer.get(buffer.size() - 1);
        if (lastBuffer.remaining() <= 0){
            ByteBuffer byteBuffer = ByteBuffer.allocate(CAPACITY);
            buffer.add(byteBuffer);
            lastBuffer = byteBuffer;
        }
        return lastBuffer;
    }


    @Override
    public synchronized void close() throws IOException {
        closing = true;//From now on, writes go the the delegate, not the buffer
        long timestamp = System.currentTimeMillis();
        long size = calcSizeOfBuffers();

        TarHeader tarHeader = TarHeader.createHeader(TapeUtils.toFilename(id),size,timestamp/1000,false);
        TarEntry entry = new TarEntry(tarHeader);
        putNextEntry(entry);
        for (ByteBuffer byteBuffer : buffer) {
            super.write(byteBuffer.array(),0,byteBuffer.position());
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
