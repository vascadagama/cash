package com.tuplejump.hive.metastore.model;

import org.apache.hadoop.hive.metastore.api.HiveObjectType;
import org.apache.hadoop.hive.metastore.model.MRoleMap;

import javax.jdo.annotations.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by rohit on 2/2/14.
 */
@Entity
public class CPrincipal {
    @Id
    @Column
    String principalId;

    @Column
    String principalName;

    @Column
    int principalType;

    @Column
    Map<String, RoleAssignment> roles;

    @Column
    Map<String, Privilege> privileges;

    public CPrincipal() {
        this.roles = new HashMap<String, RoleAssignment>();
        this.privileges = new HashMap<String, Privilege>();
    }

    public CPrincipal(String principalId, String principalName, int principalType) {
        this.principalId = principalId;
        this.principalName = principalName;
        this.principalType = principalType;
        this.roles = new HashMap<String, RoleAssignment>();
        this.privileges = new HashMap<String, Privilege>();
    }

    public String getPrincipalName() {
        return principalName;
    }

    public void setPrincipalName(String principalName) {
        this.principalName = principalName;
    }

    public int getPrincipalType() {
        return principalType;
    }

    public void setPrincipalType(int principalType) {
        this.principalType = principalType;
    }

    public Map<String, RoleAssignment> getRoles() {
        return roles;
    }

    public void setRoles(Map<String, RoleAssignment> roles) {
        this.roles = roles;
    }

    public void addRole(String role, int addTime, String grantor, int grantorType, boolean grantOption) {
        this.roles.put(role, new RoleAssignment(role, addTime, grantor, grantorType, grantOption));
    }

    public void removeRole(String role) {
        this.roles.remove(role);
    }

    public String getPrincipalId() {
        return principalId;
    }

    public void setPrincipalId(String principalId) {
        this.principalId = principalId;
    }

    public Map<String, Privilege> getPrivileges() {
        return privileges;
    }

    public void setPrivileges(Map<String, Privilege> privileges) {
        this.privileges = privileges;
    }

    public void addPrivilege(int objectType, String object, String privilege, int createTime, String grantor,
                             int grantorType, boolean grantOption) {
        Privilege p = new Privilege(objectType, object, privilege, createTime, grantor, grantorType, grantOption);
        this.privileges.put(getPrivKey(objectType, object, privilege), p);
    }

    public void removePrivilege(int objectType, String object, String privilege) {
        this.privileges.remove(getPrivKey(objectType, object, privilege));
    }

    private String getPrivKey(int objectType, String object, String privilege) {
        return objectType + "#" + object + "#" + privilege;
    }

    public boolean hasPrevilege(int objectType, String object, String privilege) {
        return this.privileges.containsKey(getPrivKey(objectType, object, privilege));
    }

    public Privilege getPrevilege(int objectType, String object, String privilege) {
        return this.privileges.get(getPrivKey(objectType, object, privilege));
    }

    @Entity
    public static class RoleAssignment {
        @Column
        String role;

        @Column
        int addTime;

        @Column
        String grantor;

        @Column
        int grantorType;

        @Column
        boolean grantOption;

        public RoleAssignment() {
        }

        public RoleAssignment(String role, int addTime, String grantor, int grantorType, boolean grantOption) {
            this.role = role;
            this.addTime = addTime;
            this.grantor = grantor;
            this.grantorType = grantorType;
            this.grantOption = grantOption;
        }

        public String getRole() {
            return role;
        }

        public void setRole(String role) {
            this.role = role;
        }

        public int getAddTime() {
            return addTime;
        }

        public void setAddTime(int addTime) {
            this.addTime = addTime;
        }

        public String getGrantor() {
            return grantor;
        }

        public void setGrantor(String grantor) {
            this.grantor = grantor;
        }

        public int getGrantorType() {
            return grantorType;
        }

        public void setGrantorType(int grantorType) {
            this.grantorType = grantorType;
        }

        public boolean isGrantOption() {
            return grantOption;
        }

        public void setGrantOption(boolean grantOption) {
            this.grantOption = grantOption;
        }
    }

    @Entity
    public static class Privilege {
        @Column
        private int objectType;

        @Column
        private String object;

        @Column
        private String privilege;

        @Column
        private int createTime;

        @Column
        private String grantor;

        @Column
        private int grantorType;

        @Column
        private boolean grantOption;

        public Privilege() {
        }

        public Privilege(int objectType, String object, String privilege, int createTime, String grantor, int grantorType, boolean grantOption) {
            this.objectType = objectType;
            this.object = object;
            this.privilege = privilege;
            this.createTime = createTime;
            this.grantor = grantor;
            this.grantorType = grantorType;
            this.grantOption = grantOption;
        }

        public int getObjectType() {
            return objectType;
        }

        public void setObjectType(int objectType) {
            this.objectType = objectType;
        }

        public String getObject() {
            return object;
        }

        public void setObject(String object) {
            this.object = object;
        }

        public String getPrivilege() {
            return privilege;
        }

        public void setPrivilege(String privilege) {
            this.privilege = privilege;
        }

        public int getCreateTime() {
            return createTime;
        }

        public void setCreateTime(int createTime) {
            this.createTime = createTime;
        }

        public String getGrantor() {
            return grantor;
        }

        public void setGrantor(String grantor) {
            this.grantor = grantor;
        }

        public int getGrantorType() {
            return grantorType;
        }

        public void setGrantorType(int grantorType) {
            this.grantorType = grantorType;
        }

        public boolean isGrantOption() {
            return grantOption;
        }

        public void setGrantOption(boolean grantOption) {
            this.grantOption = grantOption;
        }
    }
}
