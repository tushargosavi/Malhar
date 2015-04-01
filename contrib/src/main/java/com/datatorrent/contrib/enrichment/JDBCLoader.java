package com.datatorrent.contrib.enrichment;

import com.datatorrent.common.util.DTThrowable;

import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JDBCLoader extends DBLoader
{

  protected transient Connection connection = null;

  @Override protected void createDatabase()
  {
    try {
      // This will load the JDBC driver, each DB has its own driver
      String dbDriver = getDriver();
      String dbUrl = getDriverURL();
      Class.forName(dbDriver).newInstance();
      connection = DriverManager.getConnection(dbUrl, userName, password);

      logger.debug("JDBC connection Success");
    }
    catch (Throwable t) {
      DTThrowable.rethrow(t);
    }
  }

  @Override protected Object getQueryResult(Object key)
  {
    try {
      PreparedStatement getStatement ;
      if(queryStmt == null) {
        getStatement = connection.prepareStatement(generateQueryStmt(key));
      } else {
        getStatement = connection.prepareStatement(queryStmt);
        ArrayList<Object> keys = (ArrayList<Object>) key;
        for (int i = 0; i < keys.size(); i++) {
          getStatement.setObject(i+1, keys.get(i));
        }
      }
      return getStatement.executeQuery();
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @Override protected ArrayList<Object> getDataFrmResult(Object result) throws RuntimeException
  {
    try {
      ResultSet resultSet = (ResultSet) result;
      if (resultSet.next()) {
        ArrayList<Object> res = new ArrayList<Object>();
        if(queryStmt == null) {
          for(String f : includeFields) {
            res.add(resultSet.getObject(f));
          }
        } else {
          ResultSetMetaData rsdata = resultSet.getMetaData();
          int columnCount = rsdata.getColumnCount();
          // If the includefields is empty, populate it from ResultSetMetaData
          if(includeFields.size() == 0) {
            for (int i = 1; i <= columnCount; i++) {
              includeFields.add(rsdata.getColumnName(i));
            }
          }

          for (int i = 1; i <= columnCount; i++) {
            res.add(resultSet.getObject(i));
          }
        }
        return res;
      } else
        return null;
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  private String generateQueryStmt(Object key)
  {
    String stmt = "select * from " + tableName + " where ";
    ArrayList<Object> keys = (ArrayList<Object>) key;
    for (int i = 0; i < keys.size(); i++) {
      stmt = stmt + lookupFields.get(i) + " = " + keys.get(i);
      if(i != keys.size() - 1) {
        stmt = stmt + " and ";
      }
    }
    return stmt;
  }

  private String getDriver()
  {
    if(dbType.equals(DBType.MYSQL)) {
      return "org.gjt.mm.mysql.Driver";
    } else if(dbType.equals(DBType.ORACLE)) {
      return "oracle.jdbc.driver.OracleDriver";
    } else if(dbType.equals(DBType.HSQL)) {
        return "org.hsqldb.jdbcDriver";
    }

    throw new RuntimeException("Invalid DBType");
  }

  private String getDriverURL()
  {
    if(dbType.equals(DBType.MYSQL)) {
      return "jdbc:mysql://"  + hostName + "/" + dbName;
    } else if(dbType.equals(DBType.ORACLE)) {
      return "jdbc:oracle:thin:@" + hostName + ":" + dbName;
    } else if(dbType.equals(DBType.HSQL)) {
        return "jdbc:hsqldb:mem:test;sql.syntax_mys=true";
    }
    throw new RuntimeException("Invalid DBType");
  }

  @Override
  public boolean needRefresh() {
    return false;
  }

  public void disconnect() throws IOException
  {
    try {
      connection.close();
    }
    catch (SQLException ex) {
      throw new RuntimeException("closing database resource", ex);
    }
  }

  @Override
  public boolean isConnected() {
    try {
      return !connection.isClosed();
    }
    catch (SQLException e) {
      throw new RuntimeException("is isConnected", e);
    }
  }

}
