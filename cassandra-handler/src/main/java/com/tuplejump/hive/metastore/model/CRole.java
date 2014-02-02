package com.tuplejump.hive.metastore.model;

import org.apache.hadoop.hive.metastore.api.Role;
import org.apache.hadoop.hive.metastore.model.MRole;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by rohit on 2/2/14.
 */
@Entity
public class CRole {
    @Id
    @Column
    private String roleName;

    @Column
    private int createTime;

    @Column
    private String ownerName;

    @Column
    private List<String> principals;

    public CRole() {
        this.principals = new ArrayList<String>();
    }

    public CRole(String roleName, int createTime, String ownerName) {
        this.roleName = roleName;
        this.createTime = createTime;
        this.ownerName = ownerName;
        this.principals = new ArrayList<String>();
    }

    public CRole(Role role) {
        this(role.getRoleName(), role.getCreateTime(), role.getOwnerName());
    }


    public String getRoleName() {

        return roleName;
    }

    public void setRoleName(String roleName) {
        this.roleName = roleName;
    }

    public int getCreateTime() {
        return createTime;
    }

    public void setCreateTime(int createTime) {
        this.createTime = createTime;
    }

    public String getOwnerName() {
        return ownerName;
    }

    public void setOwnerName(String ownerName) {
        this.ownerName = ownerName;
    }

    public List<String> getPrincipals() {
        return principals;
    }

    public void setPrincipals(List<String> principals) {
        this.principals = principals;
    }

    public void addPrincipal(String principalId) {
        this.principals.add(principalId);
    }

    public void removePrincipal(String principalId) {
        this.principals.remove(principalId);
    }

    public Role toRole() {
        return new Role(this.roleName, this.createTime, this.ownerName);
    }

    public MRole toMRole() {
        return new MRole(this.roleName, this.createTime, this.ownerName);
    }

}
