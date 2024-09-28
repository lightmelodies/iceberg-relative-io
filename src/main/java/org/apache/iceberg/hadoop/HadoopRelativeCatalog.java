package org.apache.iceberg.hadoop;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.iceberg.*;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.*;
import org.apache.iceberg.io.CloseableGroup;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.RelativeFileIO;
import org.apache.iceberg.relocated.com.google.common.base.Joiner;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.base.Strings;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.util.LocationUtil;
import org.apache.iceberg.util.LockManagers;
import org.apache.iceberg.util.PropertyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.AccessDeniedException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class HadoopRelativeCatalog extends BaseMetastoreCatalog
        implements SupportsNamespaces, Configurable {
    private static final Logger LOG = LoggerFactory.getLogger(HadoopRelativeCatalog.class);

    private static final String TABLE_METADATA_FILE_EXTENSION = ".metadata.json";
    private static final Joiner SLASH = Joiner.on("/");
    private static final PathFilter TABLE_FILTER = path -> path.getName().endsWith(TABLE_METADATA_FILE_EXTENSION);

    private static final String VERIFY_CHECKSUM = "fs.verfiy-checksum";
    private static final String WRITE_CHECKSUM = "fs.write-checksum";
    private static final boolean VERIFY_CHECKSUM_DEFAULT = true;
    private static final boolean WRITE_CHECKSUM_DEFAULT = true;
    private static final String HADOOP_SUPPRESS_PERMISSION_ERROR = "suppress-permission-error";

    private String catalogName;
    private Configuration conf;
    private CloseableGroup closeableGroup;
    private String warehouseLocation;
    private FileSystem fs;
    private FileIO fileIO;
    private LockManager lockManager;
    private boolean suppressPermissionError = false;
    private Map<String, String> catalogProperties;

    public HadoopRelativeCatalog() {
    }

    /**
     * The constructor of the HadoopRelativeCatalog. It uses the passed location as its warehouse directory.
     *
     * @param conf              The Hadoop configuration
     * @param warehouseLocation The location used as warehouse directory
     */
    public HadoopRelativeCatalog(Configuration conf, String warehouseLocation) {
        setConf(conf);
        initialize("hadoop-relative", ImmutableMap.of(CatalogProperties.WAREHOUSE_LOCATION, warehouseLocation));
    }

    @Override
    public void initialize(String name, Map<String, String> properties) {
        this.catalogProperties = ImmutableMap.copyOf(properties);

        String inputWarehouseLocation = properties.get(CatalogProperties.WAREHOUSE_LOCATION);
        Preconditions.checkArgument(
                !Strings.isNullOrEmpty(inputWarehouseLocation),
                "Cannot initialize HadoopRelativeCatalog because warehousePath must not be null or empty");

        this.catalogName = name;
        this.warehouseLocation = LocationUtil.stripTrailingSlash(inputWarehouseLocation) + "/";
        this.fs = Util.getFs(new Path(warehouseLocation), conf);

        boolean verifyChecksum = PropertyUtil.propertyAsBoolean(properties, VERIFY_CHECKSUM, VERIFY_CHECKSUM_DEFAULT);
        boolean writeChecksum = PropertyUtil.propertyAsBoolean(properties, WRITE_CHECKSUM, WRITE_CHECKSUM_DEFAULT);
        fs.setVerifyChecksum(verifyChecksum);
        fs.setWriteChecksum(writeChecksum);

        String fileIOImpl = properties.getOrDefault(CatalogProperties.FILE_IO_IMPL, RelativeFileIO.class.getName());

        this.fileIO = CatalogUtil.loadFileIO(fileIOImpl, properties, conf);

        this.lockManager = LockManagers.from(properties);

        this.closeableGroup = new CloseableGroup();
        closeableGroup.addCloseable(lockManager);
        closeableGroup.addCloseable(metricsReporter());
        closeableGroup.setSuppressCloseFailure(true);

        this.suppressPermissionError = Boolean.parseBoolean(properties.get(HADOOP_SUPPRESS_PERMISSION_ERROR));

    }

    @Override
    public String name() {
        return catalogName;
    }

    private String relativeLocation(String location) {
        return StringUtils.removeStart(location, warehouseLocation);
    }

    private boolean shouldSuppressPermissionError(IOException ioException) {
        if (suppressPermissionError) {
            return ioException instanceof AccessDeniedException
                    || (ioException.getMessage() != null
                    && ioException.getMessage().contains("AuthorizationPermissionMismatch"));
        }
        return false;
    }

    private boolean isTableDir(Path path) {
        Path metadataPath = new Path(path, "metadata");
        // Only the path which contains metadata is the path for table, otherwise it could be still a namespace.
        try {
            return fs.listStatus(metadataPath, TABLE_FILTER).length >= 1;
        } catch (FileNotFoundException e) {
            return false;
        } catch (IOException e) {
            if (shouldSuppressPermissionError(e)) {
                LOG.warn("Unable to list metadata directory {}", metadataPath, e);
                return false;
            } else {
                throw new UncheckedIOException(e);
            }
        }
    }

    private boolean isDirectory(Path path) {
        try {
            return fs.getFileStatus(path).isDirectory();
        } catch (FileNotFoundException e) {
            return false;
        } catch (IOException e) {
            if (shouldSuppressPermissionError(e)) {
                LOG.warn("Unable to list directory {}", path, e);
                return false;
            } else {
                throw new UncheckedIOException(e);
            }
        }
    }

    @Override
    public List<TableIdentifier> listTables(Namespace namespace) {
        Preconditions.checkArgument(
                namespace.levels().length >= 1, "Missing database in table identifier: %s", namespace);

        Path nsPath = new Path(warehouseLocation, SLASH.join(namespace.levels()));
        Set<TableIdentifier> tblIdents = Sets.newHashSet();

        try {
            if (!isDirectory(nsPath)) {
                throw new NoSuchNamespaceException("Namespace does not exist: %s", namespace);
            }
            RemoteIterator<FileStatus> it = fs.listStatusIterator(nsPath);
            while (it.hasNext()) {
                FileStatus status = it.next();
                if (!status.isDirectory()) {
                    // Ignore the path which is not a directory.
                    continue;
                }

                Path path = status.getPath();
                if (isTableDir(path)) {
                    TableIdentifier tblIdent = TableIdentifier.of(namespace, path.getName());
                    tblIdents.add(tblIdent);
                }
            }
        } catch (IOException ioe) {
            throw new RuntimeIOException(ioe, "Failed to list tables under: %s", namespace);
        }

        return Lists.newArrayList(tblIdents);
    }

    @Override
    protected boolean isValidIdentifier(TableIdentifier identifier) {
        return true;
    }

    @Override
    protected TableOperations newTableOps(TableIdentifier identifier) {
        return new HadoopRelativeTableOperations(warehouseLocation,
                new Path(defaultWarehouseLocation(identifier)), fileIO, fs, lockManager);
    }

    @Override
    protected String defaultWarehouseLocation(TableIdentifier tableIdentifier) {
        StringBuilder sb = new StringBuilder();
        for (String level : tableIdentifier.namespace().levels()) {
            sb.append(level).append('/');
        }
        sb.append(tableIdentifier.name());
        return sb.toString();
    }

    @Override
    public boolean dropTable(TableIdentifier identifier, boolean purge) {
        if (!isValidIdentifier(identifier)) {
            throw new NoSuchTableException("Invalid identifier: %s", identifier);
        }

        Path tablePath = new Path(warehouseLocation, defaultWarehouseLocation(identifier));
        TableOperations ops = newTableOps(identifier);
        TableMetadata lastMetadata = ops.current();
        try {
            if (lastMetadata == null) {
                LOG.debug("Not an iceberg table: {}", identifier);
                return false;
            } else {
                if (purge) {
                    // Since the data files and the metadata files may store in different locations,
                    // so it has to call dropTableData to force delete the data file.
                    CatalogUtil.dropTableData(ops.io(), lastMetadata);
                }
                return fs.delete(tablePath, true /* recursive */);
            }
        } catch (IOException e) {
            throw new RuntimeIOException(e, "Failed to delete file: %s", tablePath);
        }
    }

    @Override
    public void renameTable(TableIdentifier from, TableIdentifier to) {
        throw new UnsupportedOperationException("Cannot rename Hadoop tables");
    }

    @Override
    public void createNamespace(Namespace namespace, Map<String, String> meta) {
        Preconditions.checkArgument(
                !namespace.isEmpty(), "Cannot create namespace with invalid name: %s", namespace);
        if (!meta.isEmpty()) {
            throw new UnsupportedOperationException(
                    "Cannot create namespace " + namespace + ": metadata is not supported");
        }

        Path nsPath = new Path(warehouseLocation, SLASH.join(namespace.levels()));

        if (isNamespace(nsPath)) {
            throw new AlreadyExistsException("Namespace already exists: %s", namespace);
        }

        try {
            fs.mkdirs(nsPath);

        } catch (IOException e) {
            throw new RuntimeIOException(e, "Create namespace failed: %s", namespace);
        }
    }

    @Override
    public List<Namespace> listNamespaces(Namespace namespace) {
        Path nsPath =
                namespace.isEmpty()
                        ? new Path(warehouseLocation)
                        : new Path(warehouseLocation, SLASH.join(namespace.levels()));
        if (!isNamespace(nsPath)) {
            throw new NoSuchNamespaceException("Namespace does not exist: %s", namespace);
        }

        try {
            // using the iterator listing allows for paged downloads
            // from HDFS and prefetching from object storage.
            List<Namespace> namespaces = Lists.newArrayList();
            RemoteIterator<FileStatus> it = fs.listStatusIterator(nsPath);
            while (it.hasNext()) {
                Path path = it.next().getPath();
                if (isNamespace(path)) {
                    namespaces.add(append(namespace, path.getName()));
                }
            }
            return namespaces;
        } catch (IOException ioe) {
            throw new RuntimeIOException(ioe, "Failed to list namespace under: %s", namespace);
        }
    }

    private Namespace append(Namespace ns, String name) {
        String[] levels = Arrays.copyOfRange(ns.levels(), 0, ns.levels().length + 1);
        levels[ns.levels().length] = name;
        return Namespace.of(levels);
    }

    @Override
    public boolean dropNamespace(Namespace namespace) {
        Path nsPath = new Path(warehouseLocation, SLASH.join(namespace.levels()));

        if (!isNamespace(nsPath) || namespace.isEmpty()) {
            return false;
        }

        try {
            if (fs.listStatusIterator(nsPath).hasNext()) {
                throw new NamespaceNotEmptyException("Namespace %s is not empty.", namespace);
            }

            return fs.delete(nsPath, false /* recursive */);
        } catch (IOException e) {
            throw new RuntimeIOException(e, "Namespace delete failed: %s", namespace);
        }
    }

    @Override
    public boolean setProperties(Namespace namespace, Map<String, String> properties) {
        throw new UnsupportedOperationException(
                "Cannot set namespace properties " + namespace + " : setProperties is not supported");
    }

    @Override
    public boolean removeProperties(Namespace namespace, Set<String> properties) {
        throw new UnsupportedOperationException(
                "Cannot remove properties " + namespace + " : removeProperties is not supported");
    }

    @Override
    public Map<String, String> loadNamespaceMetadata(Namespace namespace) {
        Path nsPath = new Path(warehouseLocation, SLASH.join(namespace.levels()));

        if (!isNamespace(nsPath) || namespace.isEmpty()) {
            throw new NoSuchNamespaceException("Namespace does not exist: %s", namespace);
        }

        return ImmutableMap.of("location", relativeLocation(nsPath.toString()));
    }

    private boolean isNamespace(Path path) {
        return isDirectory(path) && !isTableDir(path);
    }

    @Override
    public void close() throws IOException {
        closeableGroup.close();
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                          .add("name", catalogName)
                          .add("location", warehouseLocation)
                          .toString();
    }

    @Override
    public TableBuilder buildTable(TableIdentifier identifier, Schema schema) {
        return new HadoopRelativeCatalogTableBuilder(identifier, schema);
    }

    @Override
    public Configuration getConf() {
        return conf;
    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    @Override
    protected Map<String, String> properties() {
        return catalogProperties == null ? ImmutableMap.of() : catalogProperties;
    }

    private class HadoopRelativeCatalogTableBuilder extends BaseMetastoreCatalogTableBuilder {
        private final String location;

        private HadoopRelativeCatalogTableBuilder(TableIdentifier identifier, Schema schema) {
            super(identifier, schema);
            location = defaultWarehouseLocation(identifier);
        }

        @Override
        public TableBuilder withLocation(String location) {
            Preconditions.checkArgument(
                    location == null || location.equals(this.location),
                    "Cannot set a custom location for a path-based table. Expected "
                            + this.location
                            + " but got "
                            + location);
            return this;
        }
    }
}
