package org.apache.iceberg.io;

import java.io.IOException;
import java.util.Objects;

public class RelativeOutputFile implements OutputFile {
    private final String location;
    private final OutputFile file;

    public RelativeOutputFile(String location, OutputFile file) {
        this.location = location;
        this.file = file;
    }

    @Override
    public PositionOutputStream create() {
        return new RelativeOutputStream(file.create());
    }

    @Override
    public PositionOutputStream createOrOverwrite() {
        return new RelativeOutputStream(file.createOrOverwrite());
    }

    @Override
    public String location() {
        return location;
    }

    @Override
    public InputFile toInputFile() {
        return new RelativeInputFile(location, file.toInputFile());
    }

    private static class RelativeOutputStream extends PositionOutputStream {
        private final PositionOutputStream delegate;

        public RelativeOutputStream(PositionOutputStream delegate) {
            this.delegate = Objects.requireNonNull(delegate, "delegate is null");
        }

        @Override
        public long getPos() throws IOException {
            return delegate.getPos();
        }

        @Override
        public void write(int b) throws IOException {
            delegate.write(b);
        }

        @Override
        public void write(byte b[]) throws IOException {
            delegate.write(b);
        }

        @Override
        public void write(byte b[], int off, int len) throws IOException {
            delegate.write(b, off, len);
        }

        @Override
        public void flush() throws IOException {
            delegate.flush();
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }
    }
}
