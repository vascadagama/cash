package com.tuplejump.hive.metastore.model;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Table;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import java.util.*;

/**
 * Created by rohit on 2/1/14.
 */

@Entity
public class CTable {
    @Id
    private String tableKey;

    @Column
    private String tableName;

    @Column
    private String dbName;

    @Column
    private CStorageDescriptor sd;

    @Column
    private String owner;

    @Column
    private int createTime;

    @Column
    private int lastAccessTime;

    @Column
    private int retention;

    @Column
    private List<CFieldSchema> partitionKeys;

    @Column
    private Map<String, String> parameters;

    @Column
    private String viewOriginalText;

    @Column
    private String viewExpandedText;

    @Column
    private String tableType;

    @Column
    private Set<String> partitions;

    @Column
    private Set<String> indices;

    public CTable() {
        this.partitions = new HashSet<String>();
    }

    public CTable(String tableName, String dbName, CStorageDescriptor sd, String owner, int createTime,
                  int lastAccessTime, int retention, List<CFieldSchema> partitionKeys,
                  Map<String, String> parameters, String viewOriginalText,
                  String viewExpandedText, String tableType) {

        this.tableKey = dbName + "#" + tableName;
        this.tableName = tableName;
        this.dbName = dbName;
        this.sd = sd;
        this.owner = owner;
        this.createTime = createTime;
        this.lastAccessTime = lastAccessTime;
        this.retention = retention;
        this.partitionKeys = partitionKeys;
        this.parameters = parameters;
        this.viewOriginalText = viewOriginalText;
        this.viewExpandedText = viewExpandedText;
        this.tableType = tableType;
        this.partitions = new HashSet<String>();
    }

    public CTable(Table table) {
        this(table.getTableName(), table.getDbName(), new CStorageDescriptor(table.getSd()), table.getOwner(),
                table.getCreateTime(), table.getLastAccessTime(), table.getRetention(), new ArrayList<CFieldSchema>(),
                table.getParameters(), table.getViewOriginalText(), table.getViewExpandedText(), table.getTableType());

        for (FieldSchema fs : table.getPartitionKeys()) {
            this.partitionKeys.add(new CFieldSchema(fs));
        }
    }

    public Set<String> getPartitions() {

        return partitions;
    }

    public void addPartition(String partName) {
        this.partitions.add(partName);
    }

    public void removePartition(String partName) {
        this.partitions.remove(partName);
    }

    public void setPartitions(Set<String> partitions) {
        this.partitions = partitions;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getOwner() {
        return owner;
    }

    public void setOwner(String owner) {
        this.owner = owner;
    }

    public int getCreateTime() {
        return createTime;
    }

    public void setCreateTime(int createTime) {
        this.createTime = createTime;
    }

    public int getLastAccessTime() {
        return lastAccessTime;
    }

    public void setLastAccessTime(int lastAccessTime) {
        this.lastAccessTime = lastAccessTime;
    }

    public int getRetention() {
        return retention;
    }

    public void setRetention(int retention) {
        this.retention = retention;
    }

    public List<CFieldSchema> getPartitionKeys() {
        return partitionKeys;
    }

    public void setPartitionKeys(List<CFieldSchema> partitionKeys) {
        this.partitionKeys = partitionKeys;
    }

    public Map<String, String> getParameters() {
        return parameters;
    }

    public void setParameters(Map<String, String> parameters) {
        this.parameters = parameters;
    }

    public String getViewOriginalText() {
        return viewOriginalText;
    }

    public void setViewOriginalText(String viewOriginalText) {
        this.viewOriginalText = viewOriginalText;
    }

    public String getViewExpandedText() {
        return viewExpandedText;
    }

    public void setViewExpandedText(String viewExpandedText) {
        this.viewExpandedText = viewExpandedText;
    }

    public String getTableType() {
        return tableType;
    }

    public void setTableType(String tableType) {
        this.tableType = tableType;
    }

    public CStorageDescriptor getSd() {
        return sd;
    }

    public void setSd(CStorageDescriptor sd) {
        this.sd = sd;
    }

    public String getTableKey() {
        return tableKey;
    }

    public void setTableKey(String tableKey) {
        this.tableKey = tableKey;
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public Set<String> getIndices() {
        return indices;
    }

    public void setIndices(Set<String> indices) {
        this.indices = indices;
    }

    public void addIndex(String index) {
        this.indices.add(index);
    }

    public void removeIndex(String index) {
        this.indices.remove(index);
    }

    public Table toTable() {
        Table t = new Table(this.tableName, this.dbName, this.owner, this.createTime, this.lastAccessTime,
                this.retention, this.sd.toStorageDescriptor(), new ArrayList<FieldSchema>(), this.parameters,
                this.viewOriginalText, this.viewExpandedText, this.tableType);

        for (CFieldSchema pk : partitionKeys) {
            t.getPartitionKeys().add(pk.toFieldSchema());
        }

        return t;
    }
}
