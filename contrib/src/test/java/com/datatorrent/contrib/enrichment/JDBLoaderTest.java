package com.datatorrent.contrib.enrichment;

import com.datatorrent.common.util.DTThrowable;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.slf4j.LoggerFactory;

public class JDBLoaderTest
{
  static final org.slf4j.Logger logger = LoggerFactory.getLogger(JDBLoaderTest.class);

  public static class TestMeta extends TestWatcher
  {

    JDBLoader dbloader;
    @Override
    protected void starting(Description description)
    {
        try {
          dbloader = new JDBLoader();
          dbloader.setDbType("hsql");
          dbloader.setTableName("COMPANY");

          dbloader.connect();
          createTable();
          insertRecordsInTable();
        }
        catch (Throwable e) {
            DTThrowable.rethrow(e);
        }
    }

    private void createTable()
    {
        try {
            Statement stmt = dbloader.connection.createStatement();

            String createTable = "CREATE TABLE IF NOT EXISTS " + dbloader.getTableName() +
                    "(ID INT PRIMARY KEY     NOT NULL," +
                    " NAME           TEXT    NOT NULL, " +
                    " AGE            INT     NOT NULL, " +
                    " ADDRESS        CHAR(50), " +
                    " SALARY         REAL)";
            stmt.executeUpdate(createTable);

            logger.debug("Table  created successfully...");
        }
        catch (Throwable e) {
            DTThrowable.rethrow(e);
        }
    }

    private void insertRecordsInTable()
    {
      try {
        Statement stmt = dbloader.connection.createStatement();
        String tbName = dbloader.getTableName();

        String sql = "INSERT INTO " + tbName + " (ID,NAME,AGE,ADDRESS,SALARY) " +
            "VALUES (1, 'Paul', 32, 'California', 20000.00 );";
        stmt.executeUpdate(sql);

        sql = "INSERT INTO " + tbName + " (ID,NAME,AGE,ADDRESS,SALARY) " +
            "VALUES (2, 'Allen', 25, 'Texas', 15000.00 );";
        stmt.executeUpdate(sql);

        sql = "INSERT INTO " + tbName + " (ID,NAME,AGE,ADDRESS,SALARY) " +
            "VALUES (3, 'Teddy', 23, 'Norway', 20000.00 );";
        stmt.executeUpdate(sql);

        sql = "INSERT INTO " + tbName + " (ID,NAME,AGE,ADDRESS,SALARY) " +
            "VALUES (4, 'Mark', 25, 'Rich-Mond', 65000.00 );";
        stmt.executeUpdate(sql);
      }
      catch (Throwable e) {
        DTThrowable.rethrow(e);
      }

    }

    private void cleanTable()
    {
        String sql = "delete from  " + dbloader.tableName;
        try {
          Statement stmt = dbloader.connection.createStatement();
          stmt.executeUpdate(sql);
          logger.debug("Table deleted successfully...");
        } catch (SQLException e) {
          DTThrowable.rethrow(e);
        }
    }

    @Override
    protected void finished(Description description)
    {
      cleanTable();
    }
  }

  @Rule
  public TestMeta testMeta = new TestMeta();

  @Test
  public void testMysqlDBLookup() throws Exception
  {
    CountDownLatch latch = new CountDownLatch(1);

    testMeta.dbloader.setLookupkey("ID");

    latch.await(1000, TimeUnit.MILLISECONDS);

    ArrayList<Object> keys = new ArrayList<Object>();
    keys.add("4");

    Map<String, Object> columnInfo = (Map<String, Object>) testMeta.dbloader.get(keys);

    Assert.assertEquals("ID", 4, columnInfo.get("ID"));
    Assert.assertEquals("NAME", "Mark", columnInfo.get("NAME").toString().trim());
    Assert.assertEquals("AGE", 25, columnInfo.get("AGE"));
    Assert.assertEquals("ADDRESS", "Rich-Mond", columnInfo.get("ADDRESS").toString().trim());
    Assert.assertEquals("SALARY", 65000.0, columnInfo.get("SALARY"));
  }

  @Test
  public void testMysqlDBQuery() throws Exception
  {
    CountDownLatch latch = new CountDownLatch(1);

    testMeta.dbloader.setQueryStmt("Select id, name from " + testMeta.dbloader.getTableName() + " where AGE = ? and ADDRESS = ?");

    latch.await(1000, TimeUnit.MILLISECONDS);

    ArrayList<Object> keys = new ArrayList<Object>();
    keys.add("25");
    keys.add("Texas");

    Map<String, Object> columnInfo = (Map<String, Object>) testMeta.dbloader.get(keys);

    Assert.assertEquals("ID", 2, columnInfo.get("ID"));
    Assert.assertEquals("NAME", "Allen", columnInfo.get("NAME").toString().trim());
  }
}
