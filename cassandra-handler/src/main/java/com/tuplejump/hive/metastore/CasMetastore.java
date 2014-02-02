package com.tuplejump.hive.metastore;

import com.google.common.collect.ImmutableMap;
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.exceptions.BadRequestException;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.entitystore.DefaultEntityManager;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;
import com.tuplejump.hive.metastore.model.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.RawStore;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.*;
import org.apache.hadoop.hive.metastore.model.*;

import javax.persistence.PersistenceException;
import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;

/**
 * Created by rohit on 1/30/14.
 */
public class CasMetastore implements RawStore {

    private static final Logger log = Logger.getLogger(CasMetastore.class.getCanonicalName());
    private static final String CONF_CAS_PORT = "metastore.cassandra.port";
    private static final String CONF_CAS_HOST = "metastore.cassandra.host";
    private static final String CONF_CAS_METASTORE_KS = "metastore.cassandra.metastoreks";
    private static final String CONF_CAS_CON_PER_HOST = "metastore.cassandra.max_connections_per_host";
    private static final String CONF_CAS_REPLICATION = "metastore.cassandra.replication_factor";
    private static final String CONF_CAS_REPLICATION_STRATEGY = "metastore.cassandra.replication_strategy";
    private static final String CONF_CAS_READ_CONSISTENCY = "metastore.cassandra.read_consistency";
    private static final String CONF_CAS_WRITE_CONSISTENCY = "metastore.cassandra.write_consistency";
    public static final String GLOBAL_OBJECT_NAME = "GLOBAL";

    private static Lock confLock = new ReentrantLock();
    private static boolean isStaticInitialized = false;
    private Configuration conf;

    private static AstyanaxContext<Keyspace> systemKs;
    private static AstyanaxContext<Keyspace> metastoreKs;


    private DefaultEntityManager<CDatabase, String> cdbEm;
    private DefaultEntityManager<CType, String> cTypeEm;
    private DefaultEntityManager<CTable, String> cTableEm;
    private DefaultEntityManager<CPartition, String> cPartEm;
    private DefaultEntityManager<CIndex, String> cIndexEm;
    private DefaultEntityManager<CRole, String> cRoleEm;
    private DefaultEntityManager<CPrincipal, String> cPrincipalEm;

    @Override
    public void setConf(Configuration entries) {
        conf = entries;
        confLock.lock();
        try {
            if (!isStaticInitialized) {
                //Do everything else

            /* Initialize System Keyspace */
                systemKs = getKeyspaceAstyanaxContext("system", entries);
                systemKs.start();

            /* Initialize Metastore Keyspace connection */
                metastoreKs = getKeyspaceAstyanaxContext(entries.get(CONF_CAS_METASTORE_KS, "tj_metastore"), entries);
                metastoreKs.start();

                //TODO: Should get read/write consistency from configuration
                initializeEntityManagers();

                try {
                    metastoreKs.getClient().describeKeyspace();
                } catch (BadRequestException bre) {
                    try {
                        log.info("Metastore keyspace doesn't exist. Creating one now!");
                        createMSKeyspace(entries);
                        cdbEm.createStorage(null);
                        cTableEm.createStorage(null);
                        cTypeEm.createStorage(null);
                        cPartEm.createStorage(null);
                        cIndexEm.createStorage(null);
                        cRoleEm.createStorage(null);
                        cPrincipalEm.createStorage(null);

                    } catch (ConnectionException e) {
                        throw new RuntimeException(e);
                    } catch (PersistenceException pe) {
                        throw new RuntimeException(pe);
                    }
                } catch (ConnectionException e) {
                    throw new RuntimeException(e);
                }

                isStaticInitialized = true;
            } else {
                initializeEntityManagers();
            }
        } finally {
            confLock.unlock();
        }
    }

    private void initializeEntityManagers() {
        cdbEm = new DefaultEntityManager
                .Builder<CDatabase, String>()
                .withEntityType(CDatabase.class)
                .withKeyspace(metastoreKs.getClient())
                .withAutoCommit(true)
                .withReadConsistency(ConsistencyLevel.CL_QUORUM)
                .withWriteConsistency(ConsistencyLevel.CL_QUORUM)
                .build();

        cTypeEm = new DefaultEntityManager
                .Builder<CType, String>()
                .withEntityType(CType.class)
                .withKeyspace(metastoreKs.getClient())
                .withAutoCommit(true)
                .withReadConsistency(ConsistencyLevel.CL_QUORUM)
                .withWriteConsistency(ConsistencyLevel.CL_QUORUM)
                .build();

        cTableEm = new DefaultEntityManager
                .Builder<CTable, String>()
                .withEntityType(CTable.class)
                .withKeyspace(metastoreKs.getClient())
                .withAutoCommit(true)
                .withReadConsistency(ConsistencyLevel.CL_QUORUM)
                .withWriteConsistency(ConsistencyLevel.CL_QUORUM)
                .build();


        cPartEm = new DefaultEntityManager
                .Builder<CPartition, String>()
                .withEntityType(CPartition.class)
                .withKeyspace(metastoreKs.getClient())
                .withAutoCommit(true)
                .withReadConsistency(ConsistencyLevel.CL_QUORUM)
                .withWriteConsistency(ConsistencyLevel.CL_QUORUM)
                .build();


        cIndexEm = new DefaultEntityManager
                .Builder<CIndex, String>()
                .withEntityType(CIndex.class)
                .withKeyspace(metastoreKs.getClient())
                .withAutoCommit(true)
                .withReadConsistency(ConsistencyLevel.CL_QUORUM)
                .withWriteConsistency(ConsistencyLevel.CL_QUORUM)
                .build();

        cRoleEm = new DefaultEntityManager
                .Builder<CRole, String>()
                .withEntityType(CRole.class)
                .withKeyspace(metastoreKs.getClient())
                .withAutoCommit(true)
                .withReadConsistency(ConsistencyLevel.CL_QUORUM)
                .withWriteConsistency(ConsistencyLevel.CL_QUORUM)
                .build();

        cPrincipalEm = new DefaultEntityManager
                .Builder<CPrincipal, String>()
                .withEntityType(CPrincipal.class)
                .withKeyspace(metastoreKs.getClient())
                .withAutoCommit(true)
                .withReadConsistency(ConsistencyLevel.CL_QUORUM)
                .withWriteConsistency(ConsistencyLevel.CL_QUORUM)
                .build();

    }

    private void createMSKeyspace(Configuration entries) throws ConnectionException {
        String sclass = entries.get(CONF_CAS_REPLICATION_STRATEGY, "SimpleStrategy");
        int repFactor = entries.getInt(CONF_CAS_REPLICATION, 1);
        Map<String, Object> sopts = ImmutableMap.<String, Object>builder().put(
                "replication_factor", repFactor + ""
        )
                .build();

        Map<String, Object> ksopts = ImmutableMap.<String, Object>builder()
                .put("strategy_options", sopts)
                .put("strategy_class", sclass)
                .build();

        metastoreKs.getClient().createKeyspace(ksopts);


    }

    private AstyanaxContext<Keyspace> getKeyspaceAstyanaxContext(String ksname, Configuration entries) {

        int casPort = entries.getInt(CONF_CAS_PORT, 9160);

        return new AstyanaxContext.Builder()
                .forKeyspace(ksname)
                .withAstyanaxConfiguration(

                        new AstyanaxConfigurationImpl()
                                .setDiscoveryType(NodeDiscoveryType.RING_DESCRIBE)
                                .setCqlVersion("3.0.0")
                                .setTargetCassandraVersion("1.2")
                )
                .withConnectionPoolConfiguration(
                        new ConnectionPoolConfigurationImpl(ksname + "KeyPool")
                                .setPort(casPort)
                                .setMaxConnsPerHost(entries.getInt(CONF_CAS_CON_PER_HOST, 5))
                                .setSeeds(entries.get(CONF_CAS_HOST, "127.0.0.1") + ":" + casPort)
                )
                .withConnectionPoolMonitor(new CountingConnectionPoolMonitor())
                .buildKeyspace(ThriftFamilyFactory.getInstance());
    }

    @Override
    public Configuration getConf() {
        return conf;
    }


    @Override
    public void shutdown() {
        systemKs.shutdown();
        metastoreKs.shutdown();
    }

    @Override
    public boolean openTransaction() {
        //TODO: Implement transaction mechanism
        return false;
    }

    @Override
    public boolean commitTransaction() {
        //TODO: Implement transaction mechanism
        return false;
    }

    @Override
    public void rollbackTransaction() {
        //TODO: Implement transaction mechanism
    }

    @Override
    public void createDatabase(Database database) throws InvalidObjectException, MetaException {
        try {
            getCDatabase(database.getName());
            throw new InvalidObjectException("Database with name " + database.getName() + " already exists!");
        } catch (NoSuchObjectException nsoe) {
            CDatabase db = new CDatabase(database.getName(),
                    database.getDescription(),
                    database.getLocationUri(),
                    database.getParameters());

            cdbEm.put(db);
        }
    }

    private CDatabase getCDatabase(String s) throws NoSuchObjectException {
        CDatabase cDatabase = cdbEm.get(s);
        if (cDatabase != null) {
            return cDatabase;
        } else {
            throw new NoSuchObjectException("No database with the name: " + s);
        }
    }

    @Override
    public Database getDatabase(String s) throws NoSuchObjectException {
        CDatabase cDatabase = getCDatabase(s);

        Database db = new Database(cDatabase.getName(),
                cDatabase.getDescription(),
                cDatabase.getLocationUri(),
                cDatabase.getParameters());
        return db;
    }

    @Override
    public boolean dropDatabase(String s) throws NoSuchObjectException, MetaException {
        CDatabase db = getCDatabase(s);
        try {
            cdbEm.delete(s);
        } catch (PersistenceException pe) {
            return false;
        }
        return true;
    }

    @Override
    public boolean alterDatabase(String s, Database database) throws NoSuchObjectException, MetaException {
        CDatabase db = getCDatabase(s);
        try {
            db.setParameters(database.getParameters());
            cdbEm.put(db);
        } catch (PersistenceException pe) {
            log.log(Level.SEVERE, "Eror altering table: ", pe);
            return false;
        }
        return true;
    }

    @Override
    public List<String> getDatabases(String pattern) throws MetaException {
        List<String> subpatterns = Arrays.asList(pattern.trim().split("\\|"));
        List<Pattern> cpatterns = new ArrayList<Pattern>();

        for (String p : subpatterns) {
            cpatterns.add(Pattern.compile(p.replaceAll("\\*", ".*")));
        }

        //TODO: Optimize using Database name index
        List<CDatabase> dbs = cdbEm.getAll();
        List<String> dbNames = new ArrayList<String>();

        for (CDatabase db : dbs) {
            for (Pattern cp : cpatterns) {
                if (cp.matcher(db.getName()).matches()) {
                    dbNames.add(db.getName());
                }
            }
        }

        return dbNames;
    }

    @Override
    public List<String> getAllDatabases() throws MetaException {
        List<CDatabase> dbs = cdbEm.getAll();
        List<String> dbNames = new ArrayList<String>();

        for (CDatabase cdb : dbs) {
            dbNames.add(cdb.getName());
        }

        return dbNames;
    }

    @Override
    public boolean createType(Type type) {
        CType ct = new CType(type);

        try {
            cTypeEm.put(ct);
            return true;
        } catch (Exception ex) {
            log.log(Level.SEVERE, "Error creating type: " + type.getName(), ex);
            return false;
        }
    }

    @Override
    public Type getType(String s) {
        CType ct = cTypeEm.get(s);

        if (ct == null) {
            return null;
        } else {
            return ct.toType();
        }
    }

    @Override
    public boolean dropType(String s) {
        CType ct = cTypeEm.get(s);
        if (ct != null) {
            cTypeEm.delete(s);
        }
        return true;
    }

    @Override
    public void createTable(Table table) throws InvalidObjectException, MetaException {
        try {
            CTable ct = getCTable(table.getDbName(), table.getTableName());
            if (ct != null) {
                throw new MetaException("Table <" + table.getTableName()
                        + "> in database <" + table.getDbName() + "> already exists!");
            }
        } catch (NoSuchObjectException e) {
            //Do nothing as the table doesn't exist
        }
        CDatabase cdb = null;
        try {
            cdb = getCDatabase(table.getDbName());
            cdb.addTable(table.getTableName());
            CTable ct = new CTable(table);

            cdbEm.put(cdb);
            cTableEm.put(ct);

        } catch (NoSuchObjectException e) {
            throw new InvalidObjectException(e.getMessage());
        }

    }

    @Override
    public boolean dropTable(String dbName, String tableName) throws MetaException {
        CDatabase cdb = cdbEm.get(dbName);
        String tableId = getTableKey(dbName, tableName);
        CTable cTable = cTableEm.get(tableId);
        if (cdb != null) {
            cdb.getTables().remove(tableName);
            cdbEm.put(cdb);
        }
        if (cTable != null) {
            cTableEm.delete(tableId);
        }
        return true;
    }

    private String getTableKey(String dbName, String tableName) {
        return dbName + "#" + tableName;
    }

    @Override
    public Table getTable(String dbName, String tableName) throws MetaException {
        try {
            CTable ctb = getCTable(dbName, tableName);
            return ctb.toTable();
        } catch (NoSuchObjectException ex) {
            throw new MetaException(ex.getMessage());
        }
    }

    private CTable getCTable(String dbName, String tableName) throws NoSuchObjectException {
        CTable ctb = cTableEm.get(getTableKey(dbName, tableName));
        if (ctb == null) {
            throw new NoSuchObjectException("Table <" + tableName + "> in Database <" + dbName + ">doesn't exist");
        } else {
            return ctb;
        }
    }


    @Override
    public void alterTable(String dbname, String tname, Table newt) throws InvalidObjectException, MetaException {
        CTable oldt = null;
        try {
            oldt = getCTable(dbname, tname);

            // For now only alter name, owner, paramters, cols, bucketcols are allowed
            oldt.setTableName(newt.getTableName().toLowerCase());
            oldt.setParameters(newt.getParameters());
            oldt.setOwner(newt.getOwner());
            oldt.setSd(new CStorageDescriptor(newt.getSd()));
            oldt.setDbName(newt.getDbName());
            oldt.setRetention(newt.getRetention());
            oldt.setTableType(newt.getTableType());
            oldt.setLastAccessTime(newt.getLastAccessTime());
            oldt.setViewOriginalText(newt.getViewOriginalText());
            oldt.setViewExpandedText(newt.getViewExpandedText());

            List<CFieldSchema> cols = new ArrayList<CFieldSchema>();

            for (FieldSchema fs : newt.getPartitionKeys()) {
                cols.add(new CFieldSchema(fs));
            }

            oldt.setPartitionKeys(cols);

            cTableEm.put(oldt);

        } catch (NoSuchObjectException ex) {
            throw new MetaException(ex.getMessage());
        }
    }

    @Override
    public List<String> getTables(String dbname, String pattern) throws MetaException {
        List<String> subpatterns = Arrays.asList(pattern.trim().split("\\|"));
        List<Pattern> cpatterns = new ArrayList<Pattern>();

        for (String p : subpatterns) {
            cpatterns.add(Pattern.compile(p.replaceAll("\\*", ".*")));
        }

        List<String> tNames = new ArrayList<String>();
        //TODO: Optimize using Table name index
        CDatabase db = cdbEm.get(dbname);
        if (db == null) {
            throw new MetaException("Database <" + dbname + "> does not exist!");
        } else {
            for (String table : db.getTables()) {
                for (Pattern cp : cpatterns) {
                    if (cp.matcher(table).matches()) {
                        tNames.add(table);
                    }
                }
            }
        }

        return tNames;

    }

    @Override
    public List<Table> getTableObjectsByName(String dbname, List<String> tableNames)
            throws MetaException, UnknownDBException {
        try {
            getCDatabase(dbname);
            List<Table> tables = new ArrayList<Table>();
            for (String tname : tableNames) {
                CTable ctb = getCTable(dbname, tname);
                if (ctb != null) {
                    tables.add(ctb.toTable());
                } else {
                    throw new MetaException("Table <" + tname + "> in database <" + dbname + "> does not exist!");
                }
            }

            return tables;
        } catch (NoSuchObjectException e) {
            throw new UnknownDBException(e.getMessage());
        }
    }

    @Override
    public List<String> getAllTables(String dbname) throws MetaException {
        CDatabase db = cdbEm.get(dbname);
        if (db == null) {
            throw new MetaException("Database <" + dbname + "> does not exist!");
        } else {
            return db.getTables();
        }
    }

    @Override
    public List<String> listTableNamesByFilter(String dbname, String filter, short maxTables)
            throws MetaException, UnknownDBException {
        try {
            CDatabase cdb = getCDatabase(dbname);
            List<String> tnames = new ArrayList<String>();


            //Todo: Implement filter parsing and filter the tables by it
            return cdb.getTables();
        } catch (NoSuchObjectException e) {
            throw new UnknownDBException(e.getMessage());
        }
    }


    @Override
    public boolean addPartition(Partition partition) throws InvalidObjectException, MetaException {
        CTable ctb = null;
        String dbName = partition.getDbName();
        String tableName = partition.getTableName();
        try {
            ctb = getCTable(dbName, tableName);
            List<CFieldSchema> partitionKeys = ctb.getPartitionKeys();

            String pn = getPartitionName(partitionKeys, partition.getValues(), null);
            String partName = buildPartitionId(dbName, tableName, pn);

            CPartition cp = new CPartition(partName, partition);
            ctb.addPartition(pn);

            cPartEm.put(cp);
            cTableEm.put(ctb);
        } catch (NoSuchObjectException e) {
            throw new MetaException("Table <" + dbName + "> in database <" + tableName + "> does not exist!");
        }

        return true;
    }

    private String getPartitionName(List<CFieldSchema> partitionKeys, List<String> part_vals, String filler)
            throws MetaException {
        List<FieldSchema> fs = new ArrayList<FieldSchema>();

        for (CFieldSchema cfs : partitionKeys) {
            fs.add(cfs.toFieldSchema());
        }

        if (filler == null) {
            return Warehouse.makePartName(fs, part_vals);
        } else {
            return Warehouse.makePartName(fs, part_vals, filler);
        }
    }

    private String buildPartitionId(String dbName, String tableName, String pname) {
        return dbName + "#" + tableName + "#" + pname;
    }

    @Override
    public Partition getPartition(String dbName, String tableName, List<String> part_vals)
            throws MetaException, NoSuchObjectException {
        CTable ctb = getCTable(dbName, tableName);
        return getCPartition(dbName, tableName, ctb.getPartitionKeys(), part_vals).toPartition();
    }

    private CPartition getCPartition(String dbName, String tableName, List<CFieldSchema> pkeys, List<String> part_vals)
            throws NoSuchObjectException, MetaException {
        String partName = buildPartitionId(dbName, tableName, getPartitionName(pkeys, part_vals, null));
        return fetchCPartitionByName(dbName, tableName, partName);
    }

    private CPartition fetchCPartitionByName(String dbName, String tableName, String partName) throws NoSuchObjectException {
        CPartition cp;
        cp = cPartEm.get(buildPartitionId(dbName, tableName, partName));

        if (cp == null) {
            throw new NoSuchObjectException("Partition not found in Database <"
                    + dbName + "> for table <" + tableName + "> with name" + partName);
        } else {
            return cp;
        }
    }

    @Override
    public boolean dropPartition(String dbName, String tableName, List<String> part_vals) throws MetaException {
        try {
            CTable ctb = getCTable(dbName, tableName);
            CPartition cp = getCPartition(dbName, tableName, ctb.getPartitionKeys(), part_vals);

            ctb.removePartition(cp.getPartitionName());

            cTableEm.put(ctb);
            cPartEm.delete(buildPartitionId(dbName, tableName, cp.getPartitionName()));
            return true;
        } catch (NoSuchObjectException e) {
            throw new MetaException(e.getMessage());
        }
    }

    @Override
    public List<Partition> getPartitions(String dbName, String tableName, int max) throws MetaException {
        try {
            Set<String> pNames = getPartitionNames(dbName, tableName);
            return getPartitions(dbName, tableName, pNames, max);
        } catch (NoSuchObjectException e) {
            throw new MetaException(e.getMessage());
        }
    }

    private List<Partition> getPartitions(String dbName, String tableName, Set<String> pNames, int max) {
        List<Partition> partitions = new ArrayList<Partition>();

        int count = 0;
        for (String partName : pNames) {
            count++;
            partitions.add(fetchCPartition(dbName, tableName, partName).toPartition());
            if (count >= max) {
                break;
            }
        }
        return partitions;
    }

    private CPartition fetchCPartition(String dbName, String tableName, String partName) {
        return cPartEm.get(buildPartitionId(dbName, tableName, partName));
    }

    private Set<String> getPartitionNames(String dbName, String tableName) throws NoSuchObjectException {
        CTable ctb = getCTable(dbName, tableName);
        return ctb.getPartitions();
    }

    @Override
    public List<String> listPartitionNames(String dbName, String tableName, short i) throws MetaException {
        try {
            Set<String> partitionNames = this.getPartitionNames(dbName, tableName);
            List<String> pnames = new ArrayList<String>(partitionNames);
            if (pnames.size() > i) {
                pnames = pnames.subList(0, i - 1);
            }
            return pnames;
        } catch (NoSuchObjectException e) {
            throw new MetaException(e.getMessage());
        }
    }

    @Override
    public List<String> listPartitionNamesByFilter(String dbName, String tableName, String filter, short max)
            throws MetaException {
        //TODO: Implement filters
        return listPartitionNames(dbName, tableName, max);
    }

    @Override
    public void alterPartition(String dbName, String tableName, List<String> part_vals, Partition partition)
            throws InvalidObjectException, MetaException {
        this.dropPartition(dbName, tableName, part_vals);
        partition.setDbName(dbName);
        partition.setTableName(tableName);
        this.addPartition(partition);
    }

    @Override
    public List<Partition> getPartitionsByFilter(String dbName, String tableName, String filter, short max)
            throws MetaException, NoSuchObjectException {
        HashSet<String> pnames = new HashSet<String>(listPartitionNamesByFilter(dbName, tableName, filter, max));
        return getPartitions(dbName, tableName, pnames, max);
    }

    @Override
    public List<Partition> getPartitionsByNames(String dbName, String tableName, List<String> names)
            throws MetaException, NoSuchObjectException {
        CTable ctb = getCTable(dbName, tableName);
        List<Partition> partitions = new ArrayList<Partition>();
        for (String n : names) {
            CPartition cp = fetchCPartitionByName(dbName, tableName, n);
            partitions.add(cp.toPartition());
        }
        return partitions;

    }


    private String getPartitionStr(CTable tbl, Map<String, String> partName) throws InvalidPartitionException {
        if (tbl.getPartitionKeys().size() != partName.size()) {
            throw new InvalidPartitionException("Number of partition columns in table: " +
                    tbl.getPartitionKeys().size() +
                    " doesn't match with number of supplied partition values: " + partName.size());
        }

        final List<String> storedVals = new ArrayList<String>(tbl.getPartitionKeys().size());
        for (CFieldSchema partKey : tbl.getPartitionKeys()) {
            String partVal = partName.get(partKey.getName());
            if (null == partVal) {
                throw new InvalidPartitionException("No value found for partition column: " + partKey.getName());
            }
            storedVals.add(partVal);
        }
        try {
            return getPartitionName(tbl.getPartitionKeys(), storedVals, null);
        } catch (MetaException e) {
            InvalidPartitionException ipe = new InvalidPartitionException(e.getMessage());
            ipe.setStackTrace(e.getStackTrace());
            throw ipe;
        }
    }


    @Override
    public Table markPartitionForEvent(String dbName, String tableName,
                                       Map<String, String> part, PartitionEventType partitionEventType)
            throws MetaException, UnknownTableException, InvalidPartitionException, UnknownPartitionException {
        CTable ctb;
        try {
            ctb = getCTable(dbName, tableName);
        } catch (NoSuchObjectException e) {
            UnknownTableException ute = new UnknownTableException(e.getMessage());
            ute.setStackTrace(e.getStackTrace());
            throw ute;
        }
        try {
            CPartition cp = fetchCPartitionByName(dbName, tableName, getPartitionStr(ctb, part));
            cp.addEvent(new CPartition.CPartitionEvent(System.currentTimeMillis(), partitionEventType.getValue()));

            cPartEm.put(cp);
            return ctb.toTable();
        } catch (NoSuchObjectException e) {
            UnknownPartitionException upe = new UnknownPartitionException(e.getMessage());
            upe.setStackTrace(e.getStackTrace());
            throw upe;
        }
    }

    @Override
    public boolean isPartitionMarkedForEvent(String dbName, String tableName, Map<String,
            String> part, PartitionEventType partitionEventType)
            throws MetaException, UnknownTableException, InvalidPartitionException, UnknownPartitionException {
        CTable ctb;
        try {
            ctb = getCTable(dbName, tableName);
        } catch (NoSuchObjectException e) {
            UnknownTableException ute = new UnknownTableException(e.getMessage());
            ute.setStackTrace(e.getStackTrace());
            throw ute;
        }
        try {
            CPartition cp = fetchCPartitionByName(dbName, tableName, getPartitionStr(ctb, part));

            List<CPartition.CPartitionEvent> cpes = cp.getEvents();

            for (CPartition.CPartitionEvent cpe : cpes) {
                if (cpe.getEventType() == partitionEventType.getValue()) {
                    return true;
                }
            }
            return false;

        } catch (NoSuchObjectException e) {
            UnknownPartitionException upe = new UnknownPartitionException(e.getMessage());
            upe.setStackTrace(e.getStackTrace());
            throw upe;
        }
    }

    private String buildIndexId(String dbName, String tableName, String indexName) {
        return dbName + "#" + tableName + "#" + indexName;
    }

    @Override
    public boolean addIndex(Index index) throws InvalidObjectException, MetaException {
        try {
            CTable ctb = getCTable(index.getDbName(), index.getOrigTableName());
            ctb.addIndex(index.getIndexName());

            cIndexEm.put(new CIndex(buildIndexId(index.getDbName(),
                    index.getOrigTableName(), index.getIndexName()), index));
            cTableEm.put(ctb);

            return true;
        } catch (NoSuchObjectException e) {
            MetaException me = new MetaException(e.getMessage());
            me.setStackTrace(e.getStackTrace());
            throw me;
        }
    }

    @Override
    public Index getIndex(String dbName, String tableName, String indexName) throws MetaException {
        checkIndexEntry(dbName, tableName, indexName);
        CIndex ci = getCIndex(dbName, tableName, indexName);
        return ci.toIndex();
    }

    private CTable checkIndexEntry(String dbName, String tableName, String indexName) throws MetaException {
        try {
            //Check if the table exists
            CTable ctb = getCTable(dbName, tableName);
            if (!ctb.getIndices().contains(indexName)) {
                throw new MetaException("Index <" + indexName + "> not found for table <" +
                        tableName + "> in Database <" + dbName + ">");
            }
            return ctb;
        } catch (NoSuchObjectException e) {
            MetaException me = new MetaException(e.getMessage());
            me.setStackTrace(e.getStackTrace());
            throw me;
        }
    }

    private CIndex getCIndex(String dbName, String tableName, String indexName) throws MetaException {
        CIndex ci = cIndexEm.get(buildIndexId(dbName, tableName, indexName));
        if (ci == null) {
            throw new MetaException("Index <" + indexName + "> not found for table <" +
                    tableName + "> in Database <" + dbName + ">");
        }
        return ci;
    }

    @Override
    public boolean dropIndex(String dbName, String tableName, String indexName) throws MetaException {
        CTable ctb = checkIndexEntry(dbName, tableName, indexName);
        ctb.removeIndex(indexName);
        CIndex ci = getCIndex(dbName, tableName, indexName);
        cTableEm.put(ctb);
        cIndexEm.delete(ci.getIndexId());
        return true;
    }

    @Override
    public List<Index> getIndexes(String dbName, String tableName, int max) throws MetaException {
        List<String> iNames = listIndexNames(dbName, tableName, (short) max);
        List<Index> indices = new ArrayList<Index>();
        for (String i : iNames) {
            CIndex ci = cIndexEm.get(buildIndexId(dbName, tableName, i));
            indices.add(ci.toIndex());
        }
        return indices;
    }

    @Override
    public List<String> listIndexNames(String dbName, String tableName, short max) throws MetaException {
        try {
            CTable ctb = getCTable(dbName, tableName);
            List<String> iNames = new ArrayList<String>(ctb.getIndices());
            if (iNames.size() > max) {
                iNames = iNames.subList(0, max - 1);
            }
            return iNames;
        } catch (NoSuchObjectException e) {
            MetaException me = new MetaException(e.getMessage());
            me.setStackTrace(e.getStackTrace());
            throw me;
        }
    }

    @Override
    public void alterIndex(String dbName, String tableName, String indexName, Index index)
            throws InvalidObjectException, MetaException {
        CIndex oldi = getCIndex(dbName, tableName, indexName);
        oldi.setParameters(index.getParameters());
        cIndexEm.put(oldi);
    }

    @Override
    public boolean addRole(String roleName, String ownerName)
            throws InvalidObjectException, MetaException, NoSuchObjectException {
        CRole cr = new CRole(roleName, (int) System.currentTimeMillis() / 1000, ownerName);
        if (cRoleEm.get(roleName) != null) {
            throw new InvalidObjectException("Role <" + roleName + "> already exists.");
        }
        cRoleEm.put(cr);
        return true;
    }

    @Override
    public boolean removeRole(String roleName) throws MetaException, NoSuchObjectException {
        getCRole(roleName);
        cRoleEm.delete(roleName);
        return true;
    }

    private CRole getCRole(String roleName) throws NoSuchObjectException {
        CRole cr = cRoleEm.get(roleName);
        if (cr == null) {
            throw new NoSuchObjectException("Role <" + roleName + "> does not exist.");
        }
        return cr;
    }


    @Override
    public Role getRole(String rolename) throws NoSuchObjectException {
        return getCRole(rolename).toRole();
    }

    @Override
    public List<String> listRoleNames() {
        List<String> roleNames = new ArrayList<String>();
        List<CRole> roles = cRoleEm.getAll();

        for (CRole cr : roles) {
            roleNames.add(cr.getRoleName());
        }

        return roleNames;
    }

    @Override
    public List<MRoleMap> listRoles(String principal, PrincipalType principalType) {
        List<MRoleMap> roleMap = new ArrayList<MRoleMap>();
        try {
            CPrincipal cp = getCPrincipal(principal, principalType.getValue());

            List<CPrincipal.RoleAssignment> roles = new ArrayList<CPrincipal.RoleAssignment>(cp.getRoles().values());

            for (CPrincipal.RoleAssignment ra : roles) {
                CRole cr = getCRole(ra.getRole());
                roleMap.add(new MRoleMap(cp.getPrincipalName(),
                        PrincipalType.findByValue(cp.getPrincipalType()).toString(), cr.toMRole(), ra.getAddTime(),
                        ra.getGrantor(), PrincipalType.findByValue(ra.getGrantorType()).toString(), ra.isGrantOption()));
            }
        } catch (NoSuchObjectException e) {
            //Do nothing
        }
        return roleMap;
    }


    @Override
    public boolean grantRole(Role role, String principal, PrincipalType principalType, String grantor,
                             PrincipalType grantorType, boolean grantOption)
            throws MetaException, NoSuchObjectException, InvalidObjectException {
        //Fetch or create the mentioned role
        CRole cr;
        try {
            cr = getCRole(role.getRoleName());
        } catch (NoSuchObjectException ex) {
            cr = new CRole(role);
        }

        int now = (int) System.currentTimeMillis() / 1000;
        String principalId = buildPrincipalId(principal, principalType.getValue());

        //Fetch or create principal
        CPrincipal cp = cPrincipalEm.get(principalId);
        if (cp == null) {
            cp = new CPrincipal(principalId, principal, principalType.getValue());
        }

        //Add Role mappings
        cp.addRole(role.getRoleName(), now, grantor, grantorType.getValue(), grantOption);
        cr.addPrincipal(principalId);

        cPrincipalEm.put(cp);
        cRoleEm.put(cr);
        return true;
    }

    private String buildPrincipalId(String principal, int principalType) {
        return principal + "#" + principalType;
    }

    @Override
    public boolean revokeRole(Role role, String principal, PrincipalType principalType)
            throws MetaException, NoSuchObjectException {
        CRole cr = getCRole(role.getRoleName());
        CPrincipal cp = getCPrincipal(principal, principalType.getValue());

        cp.removeRole(role.getRoleName());
        cr.removePrincipal(cp.getPrincipalId());
        return true;
    }

    private CPrincipal getCPrincipal(String principal, int principalType) throws NoSuchObjectException {
        String principalId = buildPrincipalId(principal, principalType);
        CPrincipal cp = cPrincipalEm.get(principalId);
        if (cp == null) {
            throw new NoSuchObjectException("Principal <" + principal + "> of type <" + principalType + "> does not exist");
        }
        return cp;
    }


    @Override
    public PrincipalPrivilegeSet getUserPrivilegeSet(String userName, List<String> groupNames)
            throws InvalidObjectException, MetaException {

        return getAllPrincipalPrivilegeSet(userName, groupNames, HiveObjectType.GLOBAL.getValue(), GLOBAL_OBJECT_NAME);
    }

    private List<PrivilegeGrantInfo> getPrivilegeGrantInfos(String pname, int principalType,
                                                            int objectType, String object) throws NoSuchObjectException {
        CPrincipal user = getCPrincipal(pname, principalType);
        return getPrivilegeGrantInfos(user, objectType, object);
    }

    private Map<String, List<PrivilegeGrantInfo>> getPrivilegeGrantInfos(List<String> pnames,
                                                                         int principalType, int objectType, String object) {
        Map<String, List<PrivilegeGrantInfo>> ret = new HashMap<String, List<PrivilegeGrantInfo>>();
        List<CPrincipal> principals = getCPrincipals(pnames, principalType);
        for (CPrincipal cp : principals) {
            ret.put(cp.getPrincipalName(), getPrivilegeGrantInfos(cp, objectType, object));
        }
        return ret;
    }

    private List<PrivilegeGrantInfo> getPrivilegeGrantInfos(CPrincipal principal, int objectType, String objectName) {
        List<PrivilegeGrantInfo> grantInfos = new ArrayList<PrivilegeGrantInfo>();

        for (CPrincipal.Privilege p : principal.getPrivileges().values()) {
            if (p.getObjectType() == objectType && p.getObject().equalsIgnoreCase(objectName)) {
                grantInfos.add(new PrivilegeGrantInfo(p.getPrivilege(), p.getCreateTime(), p.getGrantor(),
                        PrincipalType.findByValue(p.getGrantorType()), p.isGrantOption()));
            }
        }
        return grantInfos;
    }

    @Override
    public PrincipalPrivilegeSet getDBPrivilegeSet(String dbName, String userName, List<String> groupNames)
            throws InvalidObjectException, MetaException {
        return getAllPrincipalPrivilegeSet(userName, groupNames, HiveObjectType.DATABASE.getValue(), dbName);
    }

    private PrincipalPrivilegeSet getAllPrincipalPrivilegeSet(String userName,
                                                              List<String> groupNames, int objectType, String objectName)
            throws MetaException {

        PrincipalPrivilegeSet ret = new PrincipalPrivilegeSet();
        try {

            if (userName != null) {
                Map<String, List<PrivilegeGrantInfo>> dbUserPriv = new HashMap<String, List<PrivilegeGrantInfo>>();
                dbUserPriv.put(userName, getPrivilegeGrantInfos(userName, PrincipalType.USER.getValue(),
                        objectType, objectName));
                ret.setUserPrivileges(dbUserPriv);
            }

            if (groupNames != null && groupNames.size() > 0) {
                Map<String, List<PrivilegeGrantInfo>> dbGroupPriv = getPrivilegeGrantInfos(
                        groupNames, PrincipalType.GROUP.getValue(), objectType, objectName);

                ret.setGroupPrivileges(dbGroupPriv);
            }

            List<String> roles = getAllRolesForUserAndGroups(userName, groupNames);
            if (roles != null && roles.size() > 0) {
                Map<String, List<PrivilegeGrantInfo>> dbRolePriv = getPrivilegeGrantInfos(
                        groupNames, PrincipalType.ROLE.getValue(), objectType, objectName);

                ret.setRolePrivileges(dbRolePriv);
            }

        } catch (NoSuchObjectException e) {
            MetaException me = new MetaException(e.getMessage());
            me.setStackTrace(e.getStackTrace());
            throw me;
        }
        return ret;
    }

    private List<String> getAllRolesForUserAndGroups(String user, List<String> groups) throws NoSuchObjectException {
        Set<String> roles = new HashSet<String>();
        if (user != null && !user.isEmpty()) {
            CPrincipal cp = getCPrincipal(user, PrincipalType.USER.getValue());
            roles.addAll(cp.getRoles().keySet());
        }

        if (groups != null && !groups.isEmpty()) {
            List<CPrincipal> lcp = getCPrincipals(groups, PrincipalType.GROUP.getValue());
            for (CPrincipal cp : lcp) {
                roles.addAll(cp.getRoles().keySet());
            }
        }

        return new ArrayList<String>(roles);
    }

    private List<CPrincipal> getCPrincipals(List<String> groups, int principalType) {
        List<String> gprincipals = new ArrayList<String>();
        for (String group : groups) {
            gprincipals.add(buildPrincipalId(group, principalType));
        }
        return cPrincipalEm.get(gprincipals);
    }

    @Override
    public PrincipalPrivilegeSet getTablePrivilegeSet(String dbName, String tableName, String username, List<String> groups)
            throws InvalidObjectException, MetaException {
        return getAllPrincipalPrivilegeSet(username, groups, HiveObjectType.TABLE.getValue(), dbName + "#" + tableName);
    }

    @Override
    public PrincipalPrivilegeSet getPartitionPrivilegeSet(String dbname, String tableName, String partitionName,
                                                          String username, List<String> groups)
            throws InvalidObjectException, MetaException {
        return getAllPrincipalPrivilegeSet(username, groups, HiveObjectType.PARTITION.getValue(),
                buildPartitionId(dbname, tableName, partitionName));
    }

    @Override
    public PrincipalPrivilegeSet getColumnPrivilegeSet(String dbname, String tableName, String partitionName,
                                                       String columnName, String username, List<String> groups)
            throws InvalidObjectException, MetaException {

        if (partitionName != null) {
            return getAllPrincipalPrivilegeSet(username, groups, HiveObjectType.COLUMN.getValue(),
                    buildPartitionId(dbname, tableName, partitionName) + "#" + columnName);
        } else {
            return getAllPrincipalPrivilegeSet(username, groups, HiveObjectType.COLUMN.getValue(),
                    dbname + "#" + tableName + "#" + columnName);
        }
    }

    @Override
    public List<MGlobalPrivilege> listPrincipalGlobalGrants(String principal, PrincipalType principalType) {
        List<MGlobalPrivilege> mgp = new ArrayList<MGlobalPrivilege>();
        try {
            List<PrivilegeGrantInfo> pgi = getPrivilegeGrantInfos(
                    getCPrincipal(principal, principalType.getValue()),
                    HiveObjectType.GLOBAL.getValue(), GLOBAL_OBJECT_NAME);

            for (PrivilegeGrantInfo p : pgi) {
                mgp.add(new MGlobalPrivilege(principal, principalType.toString(), p.getPrivilege(),
                        p.getCreateTime(), p.getGrantor(), p.getGrantorType().toString(), p.isGrantOption()));
            }
        } catch (NoSuchObjectException e) {
            e.printStackTrace();
        }
        return mgp;
    }

    @Override
    public List<MDBPrivilege> listPrincipalDBGrants(String principal, PrincipalType principalType, String dbName) {
        List<MDBPrivilege> mdbp = new ArrayList<MDBPrivilege>();

        try {
            CDatabase cdb = getCDatabase(dbName);

            MDatabase database = new MDatabase(cdb.getName(), cdb.getLocationUri(),
                    cdb.getDescription(), cdb.getParameters());

            List<PrivilegeGrantInfo> pgis = getPrivilegeGrantInfos(getCPrincipal(principal, principalType.getValue()),
                    HiveObjectType.DATABASE.getValue(), dbName);

            for (PrivilegeGrantInfo pgi : pgis) {

                mdbp.add(new MDBPrivilege(principal,
                        principalType.toString(),
                        database, pgi.getPrivilege(), pgi.getCreateTime(), pgi.getGrantor(),
                        pgi.getGrantorType().toString(), pgi.isGrantOption()));
            }

        } catch (NoSuchObjectException nsoe) {
            nsoe.printStackTrace();
            //Do nothing
        }

        return mdbp;
    }

    @Override
    public List<MTablePrivilege> listAllTableGrants(String principal,
                                                    PrincipalType principalType, String dbname, String table) {
        List<MTablePrivilege> mtp = new ArrayList<MTablePrivilege>();

        try {
            CDatabase cdb = getCDatabase(dbname);

            CTable ctb = getCTable(dbname, table);

            MTable mt = converToMTable(cdb, ctb);


            List<PrivilegeGrantInfo> pgis = getPrivilegeGrantInfos(getCPrincipal(principal, principalType.getValue()),
                    HiveObjectType.TABLE.getValue(), dbname + "#" + table);

            for (PrivilegeGrantInfo pgi : pgis) {
                mtp.add(new MTablePrivilege(principal, principalType.toString(), mt, pgi.getPrivilege(),
                        pgi.getCreateTime(), pgi.getGrantor(), pgi.getGrantorType().toString(), pgi.isGrantOption()));
            }

        } catch (NoSuchObjectException e) {
            e.printStackTrace();
            //Do nothing
        }

        return mtp;
    }

    private MTable converToMTable(CDatabase cdb, CTable ctb) {
        MDatabase database = new MDatabase(cdb.getName(), cdb.getLocationUri(),
                cdb.getDescription(), cdb.getParameters());

        CStorageDescriptor csd = ctb.getSd();

        MStorageDescriptor msd = convertToMStorageDescriptor(csd);

        List<MFieldSchema> mfs = new ArrayList<MFieldSchema>();

        for (CFieldSchema cfs : ctb.getPartitionKeys()) {
            mfs.add(new MFieldSchema(cfs.getName(), cfs.getType(), cfs.getComment()));
        }

        return new MTable(ctb.getTableName(), database, msd, ctb.getOwner(), ctb.getCreateTime(),
                ctb.getLastAccessTime(), ctb.getRetention(), mfs, ctb.getParameters(), ctb.getViewOriginalText(),
                ctb.getViewExpandedText(), ctb.getTableType());
    }

    private MStorageDescriptor convertToMStorageDescriptor(CStorageDescriptor csd) {
        List<MFieldSchema> msdCols = new ArrayList<MFieldSchema>();
        for (CFieldSchema cfs : csd.getCd()) {
            msdCols.add(new MFieldSchema(cfs.getName(), cfs.getType(), cfs.getComment()));
        }

        MColumnDescriptor mcd = new MColumnDescriptor(msdCols);

        CSerDeInfo cserDeInfo = csd.getSerDeInfo();
        MSerDeInfo mserde = new MSerDeInfo(cserDeInfo.getName(), cserDeInfo.getSerializationLib(), cserDeInfo.getParameters());

        List<MOrder> morders = new ArrayList<MOrder>();
        for (COrder cOrder : csd.getSortCols()) {
            morders.add(new MOrder(cOrder.getCol(), cOrder.getOrder()));
        }

        return new MStorageDescriptor(mcd, csd.getLocation(), csd.getInputFormat(),
                csd.getOutputFormat(), csd.isCompressed(), csd.getNumBuckets(), mserde, csd.getBucketCols(),
                morders, csd.getParameters());
    }

    @Override
    public List<MPartitionPrivilege> listPrincipalPartitionGrants(String principal,
                                                                  PrincipalType principalType, String dbname,
                                                                  String tablename, String partitionname) {
        List<MPartitionPrivilege> mpp = new ArrayList<MPartitionPrivilege>();

        try {
            CDatabase cdb = getCDatabase(dbname);
            CTable ctb = getCTable(dbname, tablename);
            CPartition cpart = fetchCPartitionByName(dbname, tablename, partitionname);

            MPartition mp = convertToPartition(cdb, ctb, cpart);

            List<PrivilegeGrantInfo> pgis = getPrivilegeGrantInfos(getCPrincipal(principal, principalType.getValue()),
                    HiveObjectType.PARTITION.getValue(), buildPartitionId(dbname, tablename, partitionname));

            for (PrivilegeGrantInfo pgi : pgis) {
                mpp.add(new MPartitionPrivilege(principal, principalType.toString(), mp, pgi.getPrivilege(),
                        pgi.getCreateTime(), pgi.getGrantor(), pgi.getGrantorType().toString(), pgi.isGrantOption()));
            }

        } catch (NoSuchObjectException e) {
            e.printStackTrace();
            //DO nothing
        }


        return mpp;
    }

    private MPartition convertToPartition(CDatabase cdb, CTable ctb, CPartition cpart) {
        return new MPartition(cpart.getPartitionName(), converToMTable(cdb, ctb),
                cpart.getValues(), cpart.getCreateTime(), cpart.getLastAccessTime(),
                convertToMStorageDescriptor(cpart.getSd()), cpart.getParameters());
    }

    @Override
    public List<MTableColumnPrivilege> listPrincipalTableColumnGrants(
            String principal, PrincipalType principalType, String dbname, String tname, String columnName) {
        List<MTableColumnPrivilege> mtcp = new ArrayList<MTableColumnPrivilege>();

        try {
            CDatabase cdb = getCDatabase(dbname);
            CTable ctb = getCTable(dbname, tname);

            MTable mt = converToMTable(cdb, ctb);

            List<PrivilegeGrantInfo> pgis = getPrivilegeGrantInfos(getCPrincipal(principal, principalType.getValue()),
                    HiveObjectType.COLUMN.getValue(), dbname + "#" + tname + "#" + columnName);

            for (PrivilegeGrantInfo pgi : pgis) {
                mtcp.add(new MTableColumnPrivilege(principal, principalType.toString(), mt,
                        columnName, pgi.getPrivilege(), pgi.getCreateTime(),
                        pgi.getGrantor(), pgi.getGrantorType().toString(), pgi.isGrantOption()));
            }

        } catch (NoSuchObjectException e) {
            e.printStackTrace();
            //Do nothing
        }

        return mtcp;
    }

    @Override
    public List<MPartitionColumnPrivilege> listPrincipalPartitionColumnGrants(
            String principal, PrincipalType principalType, String dbname, String tname,
            String partition, String columnName) {

        List<MPartitionColumnPrivilege> mtcp = new ArrayList<MPartitionColumnPrivilege>();

        try {
            CDatabase cdb = getCDatabase(dbname);
            CTable ctb = getCTable(dbname, tname);
            CPartition cp = fetchCPartitionByName(dbname, tname, partition);

            MPartition mp = convertToPartition(cdb, ctb, cp);


            List<PrivilegeGrantInfo> pgis = getPrivilegeGrantInfos(getCPrincipal(principal, principalType.getValue()),
                    HiveObjectType.COLUMN.getValue(), buildPartitionId(dbname, tname, partition) + "#" + columnName);

            for (PrivilegeGrantInfo pgi : pgis) {
                mtcp.add(new MPartitionColumnPrivilege(principal, principalType.toString(), mp,
                        columnName, pgi.getPrivilege(), pgi.getCreateTime(), pgi.getGrantor(),
                        pgi.getGrantorType().toString(), pgi.isGrantOption()));
            }

        } catch (NoSuchObjectException e) {
            e.printStackTrace();
            //Do nothing
        }

        return mtcp;
    }

    @Override
    public boolean grantPrivileges(PrivilegeBag privilegeBag) throws InvalidObjectException,
            MetaException, NoSuchObjectException {
        List<HiveObjectPrivilege> privs = privilegeBag.getPrivileges();
        int now = (int) (System.currentTimeMillis() / 1000);

        for (HiveObjectPrivilege privDef : privs) {
            HiveObjectRef hiveObject = privDef.getHiveObject();
            String privilegeStr = privDef.getGrantInfo().getPrivilege();
            String principal = privDef.getPrincipalName();
            PrincipalType principalType = privDef.getPrincipalType();
            String grantor = privDef.getGrantInfo().getGrantor();
            int grantorType = privDef.getGrantInfo().getGrantorType().getValue();
            boolean grantOption = privDef.getGrantInfo().isGrantOption();

            HiveObjectType objectType = hiveObject.getObjectType();

            String objectName = getHiveObjectName(hiveObject);

            CPrincipal cp = getCPrincipal(principal, principalType.getValue());
            if (cp.hasPrevilege(objectType.getValue(), objectName, privilegeStr)) {
                CPrincipal.Privilege p = cp.getPrevilege(objectType.getValue(), objectName, privilegeStr);
                throw new InvalidObjectException(p.getPrivilege()
                        + " is already granted by " + p.getGrantor() + " \n Privilege is " + hiveObject);
            }

            cp.addPrivilege(objectType.getValue(), objectName, privilegeStr, now, grantor, grantorType, grantOption);
            cPrincipalEm.put(cp);
        }

        return true;
    }

    private String getHiveObjectName(HiveObjectRef hiveObject) throws NoSuchObjectException, MetaException {
        String objectName = GLOBAL_OBJECT_NAME;
        String dbName = hiveObject.getDbName();
        String tablename = hiveObject.getObjectName();
        CTable ctb = getCTable(dbName, tablename);

        switch (hiveObject.getObjectType()) {
            case GLOBAL:
                objectName = GLOBAL_OBJECT_NAME;
                break;
            case DATABASE:
                objectName = hiveObject.getDbName();
                break;
            case TABLE:
                objectName = hiveObject.getDbName() + "#" + hiveObject.getObjectName();
                break;
            case PARTITION:
                String pname = getPartitionName(ctb.getPartitionKeys(), hiveObject.getPartValues(), null);
                objectName = buildPartitionId(dbName, tablename, pname);
                break;
            case COLUMN:
                if (hiveObject.getPartValues() == null) {
                    String pn = getPartitionName(ctb.getPartitionKeys(), hiveObject.getPartValues(), null);
                    objectName = buildPartitionId(dbName, tablename, pn) + "#" + hiveObject.getColumnName();
                } else {
                    objectName = dbName + "#" + tablename + "#" + hiveObject.getColumnName();
                }
                break;
        }
        return objectName;
    }

    @Override
    public boolean revokePrivileges(PrivilegeBag privilegeBag)
            throws InvalidObjectException, MetaException, NoSuchObjectException {

        List<HiveObjectPrivilege> privs = privilegeBag.getPrivileges();
        int now = (int) (System.currentTimeMillis() / 1000);

        for (HiveObjectPrivilege privDef : privs) {
            String principal = privDef.getPrincipalName();
            int principalType = privDef.getPrincipalType().getValue();
            String privilegeStr = privDef.getGrantInfo().getPrivilege();

            CPrincipal cp = getCPrincipal(principal, principalType);

            HiveObjectRef hiveObject = privDef.getHiveObject();
            String objName = getHiveObjectName(hiveObject);

            int objectType = hiveObject.getObjectType().getValue();
            cp.removePrivilege(objectType, objName, privilegeStr);

            cPrincipalEm.put(cp);
        }
        return true;
    }

    @Override
    public Partition getPartitionWithAuth(
            String dbname, String tblName, List<String> partVals, String username, List<String> groups)
            throws MetaException, NoSuchObjectException, InvalidObjectException {
        CTable ctbl = getCTable(dbname, tblName);

        Partition part = getPartitionWithAuth(username, groups, ctbl, partVals);

        return part;
    }

    private Partition getPartitionWithAuth(String username,
                                           List<String> groups, CTable ctbl, List<String> partVals)
            throws NoSuchObjectException, MetaException, InvalidObjectException {

        CPartition cPartition = getCPartition(ctbl.getDbName(), ctbl.getTableName(), ctbl.getPartitionKeys(), partVals);
        Partition part = cPartition.toPartition();
        String partName = cPartition.getPartitionName();

        getPartitionWithAuth(username, groups, ctbl, part, partName);
        return part;
    }

    private Partition getPartitionWithAuth(String username, List<String> groups, CTable ctbl, Partition part, String partName) throws InvalidObjectException, MetaException {
        if ("TRUE".equalsIgnoreCase(ctbl.getParameters().get("PARTITION_LEVEL_PRIVILEGE"))) {
            PrincipalPrivilegeSet partAuth = this.getPartitionPrivilegeSet(ctbl.getDbName(), ctbl.getTableName(),
                    partName, username, groups);

            part.setPrivileges(partAuth);
        }
        return part;
    }

    @Override
    public List<Partition> getPartitionsWithAuth(
            String dbname, String tablename, short max, String username, List<String> groups)
            throws MetaException, NoSuchObjectException, InvalidObjectException {
        CTable ctbl = getCTable(dbname, tablename);

        List<String> pNames = new ArrayList<String>(getPartitionNames(dbname, tablename));

        List<Partition> partitions = fetchPartitionsWithAuth(pNames, username, groups, ctbl, max);


        return partitions;
    }

    private List<Partition> fetchPartitionsWithAuth(List<String> pNames, String username, List<String> groups, CTable ctbl, short max) throws InvalidObjectException, MetaException {
        List<Partition> partitions = new ArrayList<Partition>();

        int count = 0;
        for (String partName : pNames) {
            count++;

            CPartition cpart = fetchCPartition(ctbl.getDbName(), ctbl.getTableName(), partName);

            Partition part = getPartitionWithAuth(username, groups, ctbl, cpart.toPartition(), cpart.getPartitionName());
            partitions.add(part);
            if (count >= max) {
                break;
            }
        }
        return partitions;
    }

    @Override
    public List<String> listPartitionNamesPs(
            String dbName, String tableName, List<String> partialVals, short max) throws MetaException, NoSuchObjectException {
        CTable ctbl = getCTable(dbName, tableName);
        Set<String> partNames = ctbl.getPartitions();

        Pattern pname = Pattern.compile(getPartitionName(ctbl.getPartitionKeys(), partialVals, ".*"));
        log.log(Level.FINE, "Searching for " + pname.toString() + " in table <" + tableName + ">");

        List<String> matchNames = new ArrayList<String>();
        int count = 0;
        for (String p : partNames) {
            count++;
            if (pname.matcher(p).matches()) {
                matchNames.add(p);
            }
            if (count >= max) {
                break;
            }
        }

        return matchNames;
    }

    @Override
    public List<Partition> listPartitionsPsWithAuth(
            String dbName, String tableName, List<String> partials, short max, String user, List<String> groups)
            throws MetaException, InvalidObjectException, NoSuchObjectException {
        CTable ctbl = getCTable(dbName, tableName);
        List<String> partnames = listPartitionNamesPs(dbName, tableName, partials, max);

        return fetchPartitionsWithAuth(partnames, user, groups, ctbl, max);
    }

    @Override
    public long cleanupEvents() {
        return 0;
    }

}
