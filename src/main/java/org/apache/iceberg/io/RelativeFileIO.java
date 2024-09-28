package org.apache.iceberg.io;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.hadoop.HadoopConfigurable;
import org.apache.iceberg.hadoop.SerializableConfiguration;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.base.Strings;
import org.apache.iceberg.relocated.com.google.common.collect.Streams;
import org.apache.iceberg.util.LocationUtil;
import org.apache.iceberg.util.SerializableMap;
import org.apache.iceberg.util.SerializableSupplier;

import java.net.URI;
import java.util.Map;
import java.util.function.Function;

public class RelativeFileIO implements HadoopConfigurable, DelegateFileIO {
    private static final String RELATIVE_IO_IMPL = "relative.io-impl";

    private DelegateFileIO io;
    private String warehouseLocation;
    private SerializableMap<String, String> properties;
    private SerializableSupplier<Configuration> hadoopConf;

    public RelativeFileIO() {
    }

    private String absoluteLocation(String location) {
        if (URI.create(location).isAbsolute()) {
            return location;
        }
        return warehouseLocation + location;
    }

    private String relativeLocation(String location) {
        return StringUtils.removeStart(location, warehouseLocation);
    }

    @Override
    public void initialize(Map<String, String> newProperties) {
        this.properties = SerializableMap.copyOf(newProperties);
        String inputWarehouseLocation = properties.get(CatalogProperties.WAREHOUSE_LOCATION);
        Preconditions.checkArgument(
                !Strings.isNullOrEmpty(inputWarehouseLocation),
                "Cannot initialize HadoopRelativeCatalog because warehousePath must not be null or empty");

        this.warehouseLocation = LocationUtil.stripTrailingSlash(inputWarehouseLocation) + "/";
        String impl = properties.getOrDefault(RELATIVE_IO_IMPL, ResolvingFileIO.class.getName());
        FileIO fileIO = CatalogUtil.loadFileIO(impl, properties, hadoopConf.get());
        Preconditions.checkState(fileIO instanceof DelegateFileIO,
                "FileIO does not implement DelegateFileIO: " + fileIO.getClass().getName());
        this.io = (DelegateFileIO) fileIO;
    }

    @Override
    public Map<String, String> properties() {
        return properties.immutableMap();
    }

    @Override
    public InputFile newInputFile(String path) {
        return new RelativeInputFile(relativeLocation(path), io.newInputFile(absoluteLocation(path)));
    }

    @Override
    public InputFile newInputFile(String path, long length) {
        return new RelativeInputFile(relativeLocation(path), io.newInputFile(absoluteLocation(path), length));
    }

    @Override
    public OutputFile newOutputFile(String path) {
        return new RelativeOutputFile(relativeLocation(path), io.newOutputFile(absoluteLocation(path)));
    }

    @Override
    public void deleteFiles(Iterable<String> iterable) throws BulkDeletionFailureException {
        io.deleteFiles(Streams.stream(iterable).map(this::absoluteLocation)::iterator);
    }

    @Override
    public Iterable<FileInfo> listPrefix(String s) {
        return Streams.stream(io.listPrefix(absoluteLocation(s)))
                      .map(x -> new FileInfo(relativeLocation(x.location()), x.size(), x.createdAtMillis()))::iterator;
    }

    @Override
    public void deletePrefix(String s) {
        io.deletePrefix(absoluteLocation(s));
    }

    @Override
    public void deleteFile(String path) {
        io.deleteFile(absoluteLocation(path));
    }

    @Override
    public void close() {
        if (io != null) {
            io.close();
        }
    }

    @Override
    public Configuration getConf() {
        return hadoopConf.get();
    }

    @Override
    public void setConf(Configuration conf) {
        this.hadoopConf = new SerializableConfiguration(conf)::get;
    }

    @Override
    public void serializeConfWith(Function<Configuration, SerializableSupplier<Configuration>> confSerializer) {
        this.hadoopConf = confSerializer.apply(getConf());
    }
}