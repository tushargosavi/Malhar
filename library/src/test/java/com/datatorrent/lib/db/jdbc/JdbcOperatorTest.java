package com.datatorrent.lib.db.jdbc;

import java.sql.*;
import java.util.List;

import javax.annotation.Nonnull;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Lists;

import com.datatorrent.api.DAG;

import com.datatorrent.common.util.DTThrowable;
import com.datatorrent.lib.helper.OperatorContextTestHelper;
import com.datatorrent.lib.testbench.CollectorTestSink;

/**
 * Tests for {@link AbstractJdbcTransactionableOutputOperator} and {@link AbstractJdbcInputOperator}
 */
public class JdbcOperatorTest
{
  public static final String DB_DRIVER = "org.hsqldb.jdbcDriver";
  public static final String URL = "jdbc:hsqldb:mem:test;sql.syntax_mys=true";

  private static final String TABLE_NAME = "test_event_table";
  private static String APP_ID = "JdbcOperatorTest";
  private static int OPERATOR_ID = 0;

  private static class TestEvent
  {
    int id;

    TestEvent(int id)
    {
      this.id = id;
    }
  }

  @BeforeClass
  public static void setup()
  {
    try {
      Class.forName(DB_DRIVER).newInstance();

      Connection con = DriverManager.getConnection(URL);
      Statement stmt = con.createStatement();

      String createMetaTable = "CREATE TABLE IF NOT EXISTS " + JdbcTransactionalStore.DEFAULT_META_TABLE + " ( " +
        JdbcTransactionalStore.DEFAULT_APP_ID_COL + " VARCHAR(100) NOT NULL, " +
        JdbcTransactionalStore.DEFAULT_OPERATOR_ID_COL + " INT NOT NULL, " +
        JdbcTransactionalStore.DEFAULT_WINDOW_COL + " BIGINT NOT NULL, " +
        "UNIQUE (" + JdbcTransactionalStore.DEFAULT_APP_ID_COL + ", " + JdbcTransactionalStore.DEFAULT_OPERATOR_ID_COL + ", " + JdbcTransactionalStore.DEFAULT_WINDOW_COL + ") " +
        ")";
      stmt.executeUpdate(createMetaTable);

      String createTable = "CREATE TABLE IF NOT EXISTS " + TABLE_NAME + " (ID INTEGER)";
      stmt.executeUpdate(createTable);
    }
    catch (Throwable e) {
      DTThrowable.rethrow(e);
    }
  }

  public static void cleanTable()
  {
    try {
      Connection con = DriverManager.getConnection(URL);
      Statement stmt = con.createStatement();

      String cleanTable = "delete from " + TABLE_NAME;
      stmt.executeUpdate(cleanTable);

      cleanTable = "delete from " + JdbcTransactionalStore.DEFAULT_META_TABLE;
      stmt.executeUpdate(cleanTable);
    }
    catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  private static class TestOutputOperator extends AbstractJdbcTransactionableOutputOperator<TestEvent>
  {
    private static final String INSERT_STMT = "INSERT INTO " + TABLE_NAME + " values (?)";

    TestOutputOperator()
    {
      cleanTable();
    }

    @Nonnull
    @Override
    protected String getUpdateCommand()
    {
      return INSERT_STMT;
    }

    @Override
    protected void setStatementParameters(PreparedStatement statement, TestEvent tuple) throws SQLException
    {
      statement.setInt(1, tuple.id);
    }

    public int getNumOfEventsInStore()
    {
      Connection con;
      try {
        con = DriverManager.getConnection(URL);
        Statement stmt = con.createStatement();

        String countQuery = "SELECT count(*) from " + TABLE_NAME;
        ResultSet resultSet = stmt.executeQuery(countQuery);
        resultSet.next();
        return resultSet.getInt(1);
      }
      catch (SQLException e) {
        throw new RuntimeException("fetching count", e);
      }
    }
  }

  private static class TestInputOperator extends AbstractJdbcInputOperator<TestEvent>
  {

    private static final String retrieveQuery = "SELECT * FROM " + TABLE_NAME;

    TestInputOperator()
    {
      cleanTable();
    }

    @Override
    public TestEvent getTuple(ResultSet result)
    {
      try {
        return new TestEvent(result.getInt(1));
      }
      catch (SQLException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public String queryToRetrieveData()
    {
      return retrieveQuery;
    }

    public void insertEventsInTable(int numEvents)
    {
      try {
        Connection con = DriverManager.getConnection(URL);
        String insert = "insert into " + TABLE_NAME + " values (?)";
        PreparedStatement stmt = con.prepareStatement(insert);

        for (int i = 0; i < numEvents; i++) {
          stmt.setInt(1, i);
          stmt.executeUpdate();
        }
      }
      catch (SQLException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Test
  public void testJdbcOutputOperator()
  {
    JdbcTransactionalStore transactionalStore = new JdbcTransactionalStore();
    transactionalStore.setDbDriver(DB_DRIVER);
    transactionalStore.setDbUrl(URL);

    com.datatorrent.api.Attribute.AttributeMap.DefaultAttributeMap attributeMap = new com.datatorrent.api.Attribute.AttributeMap.DefaultAttributeMap();
    attributeMap.put(DAG.APPLICATION_ID, APP_ID);
    OperatorContextTestHelper.TestIdOperatorContext context = new OperatorContextTestHelper.TestIdOperatorContext(OPERATOR_ID, attributeMap);

    TestOutputOperator outputOperator = new TestOutputOperator();
    outputOperator.setBatchSize(3);
    outputOperator.setStore(transactionalStore);

    outputOperator.setup(context);

    List<TestEvent> events = Lists.newArrayList();
    for (int i = 0; i < 10; i++) {
      events.add(new TestEvent(i));
    }

    outputOperator.beginWindow(0);
    for (TestEvent event : events) {
      outputOperator.input.process(event);
    }
    outputOperator.endWindow();

    Assert.assertEquals("rows in db", 10, outputOperator.getNumOfEventsInStore());
  }

  @Test
  public void TestJdbcInputOperator()
  {
    JdbcStore store = new JdbcStore();
    store.setDbDriver(DB_DRIVER);
    store.setDbUrl(URL);

    com.datatorrent.api.Attribute.AttributeMap.DefaultAttributeMap attributeMap = new com.datatorrent.api.Attribute.AttributeMap.DefaultAttributeMap();
    attributeMap.put(DAG.APPLICATION_ID, APP_ID);
    OperatorContextTestHelper.TestIdOperatorContext context = new OperatorContextTestHelper.TestIdOperatorContext(OPERATOR_ID, attributeMap);

    TestInputOperator inputOperator = new TestInputOperator();
    inputOperator.setStore(store);
    inputOperator.insertEventsInTable(10);

    CollectorTestSink<Object> sink = new CollectorTestSink<Object>();
    inputOperator.outputPort.setSink(sink);

    inputOperator.setup(context);
    inputOperator.beginWindow(0);
    inputOperator.emitTuples();
    inputOperator.endWindow();

    Assert.assertEquals("rows from db", 10, sink.collectedTuples.size());
  }
}

