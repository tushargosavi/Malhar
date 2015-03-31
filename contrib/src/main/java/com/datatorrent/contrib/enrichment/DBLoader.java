package com.datatorrent.contrib.enrichment;

import com.datatorrent.lib.db.cache.CacheManager;
import com.google.common.collect.Lists;
import java.util.ArrayList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.constraints.NotNull;
import java.io.IOException;
import java.util.List;
import java.util.Map;

public abstract class DBLoader implements EnrichmentBackup {
  protected static final Logger logger = LoggerFactory.getLogger(DBLoader.class);
  @NotNull
  protected String userName;
  @NotNull
  protected String password;
  @NotNull
  protected String tableName;
  @NotNull
  protected String dbName;
  @NotNull
  protected String hostName;
  @NotNull
  protected DBType dbType;

  String queryStmt = "";

  List<String> lookupKeys;

  List<String> includeKeys;

  protected abstract void createDatabase();
  protected abstract Object getQueryResult(Object key);
  protected abstract ArrayList<Object> getDataFrmResult(Object stmt);


  @Override
  public Map<Object, Object> loadInitialData() {
    return null;
  }

  @Override
  public Object get(Object key) {
     return getDataFrmResult(getQueryResult(key));
  }

  @Override
  public List<Object> getAll(List<Object> keys) {
    List<Object> values = Lists.newArrayList();
    for (Object key : keys) {
      values.add(get(key));
    }
    return values;
  }

  @Override
  public void put(Object key, Object value) {
    throw new RuntimeException("Put Operation not supported");
  }

  @Override
  public void putAll(Map<Object, Object> m) {
    throw new RuntimeException("PutAll Operation not supported");
  }

  @Override
  public void remove(Object key) {
    throw new RuntimeException("remove Operation not supported");
  }

  @Override
  public void connect() throws IOException {
    createDatabase();
  }

  @Override
  public void disconnect() throws IOException {

  }

  @Override
  public boolean isConnected() {
    return false;
  }

  public static enum DBType
  {
    MYSQL,
    ORACLE,
    HSQL
  }

  public String getUserName()
  {
    return userName;
  }

  public void setUserName(String userName)
  {
    this.userName = userName;
  }

  public String getPassword()
  {
    return password;
  }

  public void setPassword(String password)
  {
    this.password = password;
  }

  public String getTableName()
  {
    return tableName;
  }

  public void setTableName(String tableName)
  {
    this.tableName = tableName;
  }

  public String getQueryStmt()
  {
    return queryStmt;
  }

  public void setQueryStmt(String queryStmt)
  {
    this.queryStmt = queryStmt;
  }

  public void setDbName(String dbName)
  {
    this.dbName = dbName;
  }

  public void setHostName(String hostName)
  {
    this.hostName = hostName;
  }

  public void setDbType(String dbType)
  {
    this.dbType = DBType.valueOf(dbType.toUpperCase());
  }

  @Override public void setLookupFields(List<String> lookupFields)
  {
    lookupKeys = lookupFields;
  }

  @Override public void setIncludeFields(List<String> includeFields)
  {
    includeKeys = includeFields;
  }
}
