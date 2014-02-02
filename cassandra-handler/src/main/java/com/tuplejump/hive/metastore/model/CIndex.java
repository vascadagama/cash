package com.tuplejump.hive.metastore.model;

import org.apache.hadoop.hive.metastore.api.Index;

import javax.jdo.annotations.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import java.util.Map;

/**
 * Created by rohit on 2/2/14.
 */

@Entity
public class CIndex {
    @Id
    @Column
    private String indexId;

    @Column
    private String indexName;

    @Column
    private String dbName;

    @Column
    private String origTable;

    @Column
    private int createTime;

    @Column
    private int lastAccessTime;

    @Column
    private Map<String, String> parameters;

    @Column
    private String indexTable;

    @Column
    private CStorageDescriptor sd;

    @Column
    private String indexHandlerClass;

    @Column
    private boolean deferredRebuild;

    public CIndex() {
    }

    public CIndex(String indexId, String indexName, String dbName, String origTable, int createTime, int lastAccessTime,
                  Map<String, String> parameters, String indexTable, CStorageDescriptor sd,
                  String indexHandlerClass, boolean deferredRebuild) {
        this.indexName = indexName;
        this.origTable = origTable;
        this.createTime = createTime;
        this.lastAccessTime = lastAccessTime;
        this.parameters = parameters;
        this.indexTable = indexTable;
        this.sd = sd;
        this.indexHandlerClass = indexHandlerClass;
        this.deferredRebuild = deferredRebuild;
    }

    public CIndex(String indexId, Index index) {
        this(indexId, index.getIndexName(), index.getDbName(), index.getOrigTableName(), index.getCreateTime(), index.getLastAccessTime(),
                index.getParameters(), index.getIndexTableName(), new CStorageDescriptor(index.getSd()),
                index.getIndexHandlerClass(), index.isDeferredRebuild());
    }

    public String getIndexId() {
        return indexId;
    }

    public void setIndexId(String indexId) {
        this.indexId = indexId;
    }

    public String getIndexName() {
        return indexName;
    }

    public void setIndexName(String indexName) {
        this.indexName = indexName;
    }

    public String getOrigTable() {
        return origTable;
    }

    public void setOrigTable(String origTable) {
        this.origTable = origTable;
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

    public Map<String, String> getParameters() {
        return parameters;
    }

    public void setParameters(Map<String, String> parameters) {
        this.parameters = parameters;
    }

    public String getIndexTable() {
        return indexTable;
    }

    public void setIndexTable(String indexTable) {
        this.indexTable = indexTable;
    }

    public CStorageDescriptor getSd() {
        return sd;
    }

    public void setSd(CStorageDescriptor sd) {
        this.sd = sd;
    }

    public String getIndexHandlerClass() {
        return indexHandlerClass;
    }

    public void setIndexHandlerClass(String indexHandlerClass) {
        this.indexHandlerClass = indexHandlerClass;
    }

    public boolean isDeferredRebuild() {
        return deferredRebuild;
    }

    public void setDeferredRebuild(boolean deferredRebuild) {
        this.deferredRebuild = deferredRebuild;
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public Index toIndex() {
        return new Index(this.indexName, this.indexHandlerClass, this.dbName, this.origTable, this.createTime,
                this.lastAccessTime, this.indexTable, this.sd.toStorageDescriptor(), this.parameters, this.deferredRebuild);
    }
}
