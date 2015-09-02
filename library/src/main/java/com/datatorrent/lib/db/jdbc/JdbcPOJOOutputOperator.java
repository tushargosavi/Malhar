/**
 * Copyright (C) 2015 DataTorrent, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.lib.db.jdbc;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.lib.util.PojoUtils;
import com.datatorrent.lib.util.PojoUtils.Getter;
import com.datatorrent.lib.util.PojoUtils.GetterBoolean;
import com.datatorrent.lib.util.PojoUtils.GetterChar;
import com.datatorrent.lib.util.PojoUtils.GetterDouble;
import com.datatorrent.lib.util.PojoUtils.GetterFloat;
import com.datatorrent.lib.util.PojoUtils.GetterInt;
import com.datatorrent.lib.util.PojoUtils.GetterLong;
import com.datatorrent.lib.util.PojoUtils.GetterShort;

import java.sql.*;
import java.util.ArrayList;

import javax.validation.constraints.NotNull;

import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>
 * JdbcPOJOOutputOperator class.</p>
 * A Generic implementation of AbstractJdbcTransactionableOutputOperator which takes in any POJO.
 *
 * @displayName Jdbc Output Operator
 * @category Output
 * @tags database, sql, pojo, jdbc
 * @since 2.1.0
 */
@Evolving
public class JdbcPOJOOutputOperator extends AbstractJdbcTransactionableOutputOperator<Object>
{
  @NotNull
  private ArrayList<String> dataColumns;
  //These are extracted from table metadata
  private ArrayList<Integer> columnDataTypes;

  /*
   * An arraylist of data column names to be set in database.
   * Gets column names.
   */
  public ArrayList<String> getDataColumns()
  {
    return dataColumns;
  }

  public void setDataColumns(ArrayList<String> dataColumns)
  {
    this.dataColumns = dataColumns;
  }

  @NotNull
  private String tablename;

  /*
   * Gets the Tablename in database.
   */
  public String getTablename()
  {
    return tablename;
  }

  public void setTablename(String tablename)
  {
    this.tablename = tablename;
  }

  /*
   * An ArrayList of Java expressions that will yield the field value from the POJO.
   * Each expression corresponds to one column in the database table.
   */
  public ArrayList<String> getExpressions()
  {
    return expressions;
  }

  public void setExpressions(ArrayList<String> expressions)
  {
    this.expressions = expressions;
  }

  @NotNull
  private ArrayList<String> expressions;
  private transient ArrayList<Object> getters;
  private String insertStatement;

  @Override
  public void setup(OperatorContext context)
  {
    StringBuilder columns = new StringBuilder("");
    StringBuilder values = new StringBuilder("");
    for (int i = 0; i < dataColumns.size(); i++) {
      columns.append(dataColumns.get(i));
      values.append("?");
      if (i < dataColumns.size() - 1) {
        columns.append(",");
        values.append(",");
      }
    }
    insertStatement = "INSERT INTO "
            + tablename
            + " (" + columns.toString() + ")"
            + " VALUES (" + values.toString() + ")";
    LOG.debug("insert statement is {}", insertStatement);
    super.setup(context);
    Connection conn = store.getConnection();
    LOG.debug("Got Connection.");
    try {
      Statement st = conn.createStatement();
      ResultSet rs = st.executeQuery("select * from " + tablename);

      ResultSetMetaData rsMetaData = rs.getMetaData();

      int numberOfColumns = 0;

      numberOfColumns = rsMetaData.getColumnCount();

      LOG.debug("resultSet MetaData column Count=" + numberOfColumns);

      for (int i = 1; i <= numberOfColumns; i++) {
        // get the designated column's SQL type.
        int type = rsMetaData.getColumnType(i);
        LOG.debug("column name {}", rsMetaData.getColumnTypeName(i));
        columnDataTypes.add(type);
        LOG.debug("sql column type is " + type);
      }
    }
    catch (SQLException ex) {
      throw new RuntimeException(ex);
    }

  }

  public JdbcPOJOOutputOperator()
  {
    super();
    columnDataTypes = new ArrayList<Integer>();
    getters = new ArrayList<Object>();
  }

  @Override
  public void processTuple(Object tuple)
  {
    if (getters.isEmpty()) {
      processFirstTuple(tuple);
    }
    super.processTuple(tuple);
  }

  public void processFirstTuple(Object tuple)
  {
    final Class<?> fqcn = tuple.getClass();
    final int size = columnDataTypes.size();
    for (int i = 0; i < size; i++) {
      final int type = columnDataTypes.get(i);
      final String getterExpression = expressions.get(i);
      final Object getter;
      switch (type) {
        case Types.CHAR:
          getter = PojoUtils.createGetterChar(fqcn, getterExpression);
          break;
        case Types.VARCHAR:
          getter = PojoUtils.createGetter(fqcn, getterExpression, String.class);
          break;
        case Types.BOOLEAN:
        case Types.TINYINT:
          getter = PojoUtils.createGetterBoolean(fqcn, getterExpression);
          break;
        case Types.SMALLINT:
          getter = PojoUtils.createGetterShort(fqcn, getterExpression);
          break;
        case Types.INTEGER:
          getter = PojoUtils.createGetterInt(fqcn, getterExpression);
          break;
        case Types.BIGINT:
          getter = PojoUtils.createGetterLong(fqcn, getterExpression);
          break;
        case Types.FLOAT:
          getter = PojoUtils.createGetterFloat(fqcn, getterExpression);
          break;
        case Types.DOUBLE:
          getter = PojoUtils.createGetterDouble(fqcn, getterExpression);
          break;
        default:
          /*
           Types.DECIMAL
           Types.DATE
           Types.TIME
           Types.ARRAY
           Types.OTHER
           */
          getter = PojoUtils.createGetter(fqcn, getterExpression, Object.class);
          break;
      }
      getters.add(getter);
    }

  }

  @Override
  protected String getUpdateCommand()
  {
    LOG.debug("insertstatement is {}", insertStatement);
    return insertStatement;
  }

  @Override
  @SuppressWarnings("unchecked")
  protected void setStatementParameters(PreparedStatement statement, Object tuple) throws SQLException
  {
    final int size = columnDataTypes.size();
    for (int i = 0; i < size; i++) {
      final int type = columnDataTypes.get(i);
      switch (type) {
        case (Types.CHAR):
          statement.setString(i + 1, ((Getter<Object, String>)getters.get(i)).get(tuple));
          break;
        case (Types.VARCHAR):
          statement.setString(i + 1, ((Getter<Object, String>)getters.get(i)).get(tuple));
          break;
        case (Types.BOOLEAN):
        case (Types.TINYINT):
          statement.setBoolean(i + 1, ((GetterBoolean<Object>)getters.get(i)).get(tuple));
          break;
        case (Types.SMALLINT):
          statement.setShort(i + 1, ((GetterShort<Object>)getters.get(i)).get(tuple));
          break;
        case (Types.INTEGER):
          statement.setInt(i + 1, ((GetterInt<Object>)getters.get(i)).get(tuple));
          break;
        case (Types.BIGINT):
          statement.setLong(i + 1, ((GetterLong<Object>)getters.get(i)).get(tuple));
          break;
        case (Types.FLOAT):
          statement.setFloat(i + 1, ((GetterFloat<Object>)getters.get(i)).get(tuple));
          break;
        case (Types.DOUBLE):
          statement.setDouble(i + 1, ((GetterDouble<Object>)getters.get(i)).get(tuple));
          break;
        default:
          /*
           Types.DECIMAL
           Types.DATE
           Types.TIME
           Types.ARRAY
           Types.OTHER
           */
          statement.setObject(i + 1, ((Getter<Object, Object>)getters.get(i)).get(tuple));
          break;
      }
    }
  }

  private static final Logger LOG = LoggerFactory.getLogger(JdbcPOJOOutputOperator.class);

}
