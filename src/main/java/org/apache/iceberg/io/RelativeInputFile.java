package org.apache.iceberg.io;

import java.io.IOException;
import java.util.Objects;

public class RelativeInputFile implements InputFile {
    private final String location;
    private final InputFile file;

    public RelativeInputFile(String location, InputFile file) {
        this.location = location;
        this.file = file;
    }

    @Override
    public long getLength() {
        return file.getLength();
    }

    @Override
    public SeekableInputStream newStream() {
        return new RelativeInputStream( file.newStream());
    }

    @Override
    public String location() {
        return location;
    }

    @Override
    public boolean exists() {
        return file.exists();
    }

    // We need this workaround because iceberg handles HadoopInputFile specially.
    // see https://github.com/trinodb/trino/issues/5201#issuecomment-876089214
    private static class RelativeInputStream extends SeekableInputStream {
        private final SeekableInputStream delegate;

        public RelativeInputStream(SeekableInputStream delegate) {
            this.delegate = Objects.requireNonNull(delegate, "delegate is null");
        }

        @Override
        public int read() throws IOException {
            return delegate.read();
        }

        @Override
        public int read(byte b[]) throws IOException {
            return delegate.read(b);
        }

        @Override
        public int read(byte b[], int off, int len) throws IOException {
            return delegate.read(b, off, len);
        }

        public long skip(long n) throws IOException {
            return delegate.skip(n);
        }

        @Override
        public int available() throws IOException {
            return delegate.available();
        }

        @Override
        public void mark(int readlimit) {
            delegate.mark(readlimit);
        }

        @Override
        public synchronized void reset() throws IOException {
            delegate.reset();
        }

        @Override
        public boolean markSupported() {
            return delegate.markSupported();
        }

        @Override
        public long getPos() throws IOException {
            return delegate.getPos();
        }

        @Override
        public void seek(long newPos) throws IOException {
            delegate.seek(newPos);
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }
    }
}
