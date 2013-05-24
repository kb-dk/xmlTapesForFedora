package dk.statsbiblioteket.metadatarepository.xmltapes.interfaces;

import org.kamranzafar.jtar.TarEntry;
import org.kamranzafar.jtar.TarHeader;
import org.kamranzafar.jtar.TarOutputStream;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Changes are stored in a set of bytebuffers, and written as a whole when the stream is closed. This is because
 * the entry needs to know the length before writing
 * When created, will lock the store for writing. Will unlock the store when closed. This is to ensure a that
 * the sequence of tapes is not disturbed.
 */
public class TapeOutputStream extends TarOutputStream {

    private static final int CAPACITY = 1 * 1024 * 1024;
    private Entry file;
    private final URI id;
    private final Index index;
    private StoreLock writeLock;

    boolean finished = false;

    private List<ByteBuffer> buffer;

    public TapeOutputStream(OutputStream delegate, Entry file, URI id, Index index, StoreLock writeLock) throws IOException {
        super(delegate);

        this.file = file;
        this.id = id;
        this.index = index;
        this.writeLock = writeLock;
        buffer = new ArrayList<ByteBuffer>(1);
        buffer.add(ByteBuffer.allocate(CAPACITY));

        this.writeLock.lock(this);

    }



    @Override
    public synchronized void write(int b) throws IOException {
        if (finished){
            super.write(b);
            return;
        }
        ByteBuffer lastBuffer = buffer.get(buffer.size() - 1);
        if (lastBuffer.remaining() <= 0){
            ByteBuffer byteBuffer = ByteBuffer.allocate(CAPACITY);
            buffer.add(byteBuffer);
            lastBuffer = byteBuffer;
        }
        lastBuffer.put((byte) b);
    }




    @Override
    public synchronized void close() throws IOException {
        finished = true;
        long size = calcSizeOfBuffers();
        TarHeader tarHeader = TarHeader.createHeader(file.getFilename(),size,System.currentTimeMillis(),false);
        TarEntry entry = new TarEntry(tarHeader);
        putNextEntry(entry);
        for (ByteBuffer byteBuffer : buffer) {
            write(byteBuffer.array(),0,byteBuffer.position());
        }
        closeCurrentEntry();
        out.close();
        index.addLocation(id, file);
        writeLock.unlock(this);
    }

    private long calcSizeOfBuffers() {
        long size = 0;
        for (ByteBuffer byteBuffer : buffer) {
            size += byteBuffer.position();
        }
        return size;
    }
}
