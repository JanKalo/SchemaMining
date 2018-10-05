package javatools.database;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javatools.administrative.Announce;
import javatools.filehandlers.FileLines;

/**
 * This class is part of the Java Tools (see
 * http://mpii.de/yago-naga/javatools). It is licensed under the Creative
 * Commons Attribution License (see http://creativecommons.org/licenses/by/3.0)
 * by the YAGO-NAGA team (see http://mpii.de/yago-naga).
 * 
 * This class provides a dummy database that knows just one single table.
 * All queries run on this table and the WHERE clause is ignored.
 * Example:<PRE>
   Database d=new DummyDatabase(Arrays.asList("arg1","relation","arg2"),
				"Albert_Einstein","bornOnDate","1879-03-14",
				"Elvis_Presley","bornIn","Tupelo"
				);
  </PRE>
 */

public class DummyDatabase extends Database {
    /** Holds the table */
	public List<List<String>> columns = new ArrayList<List<String>>();
    /** Holds the column names */
	public List<String> columnNames = new ArrayList<String>();

	/** Number of rows*/
	public int numRows=0;
	
	/** Executes an SQL update query, returns the number of rows added/deleted */
	public int executeUpdate(CharSequence sqlcs) {
		Announce.debug(sqlcs);
		return (0);
	}

	/** Creates a dummy database */
	public DummyDatabase() {
		description="Empty dummy database";
	}

	/** Creates a dummy database */
	public DummyDatabase(List<String> columnNames, List<List<String>> columns) {
		this.columns=columns;
		for(String columnName : columnNames) this.columnNames.add(columnName.toLowerCase());
		if(columns.size()!=0) numRows=columns.get(0).size();
		description="Dummy database with schema "+columnNames+" and "+numRows+" rows";
	}

	/** Creates a dummy database */
	public DummyDatabase(List<String> columnNames, String... valuesAsRows) {
		for(String columnName : columnNames) {
			this.columnNames.add(columnName.toLowerCase());
			columns.add(new ArrayList<String>());
		}
		int col=0;
		for(String value : valuesAsRows) {
			columns.get(col).add(value);
			col++;
			if(col%columnNames.size()==0) {
				numRows++;
				col=0;
			}
		}
		description="Dummy database with schema "+columnNames+" and "+numRows+" rows";
	}
	
	/** Creates a dummy database with values from a TSV file 
	 * @throws IOException */
	public DummyDatabase(List<String> columnNames, File values) throws SQLException {
		for(String columnName : columnNames) {
			this.columnNames.add(columnName.toLowerCase());
			columns.add(new ArrayList<String>());
		}
		int col=0;
		try {
			for(String line : new FileLines(values,"Loading "+values)) {
				String[] split=line.split("\t");
				for(int i=0;i<columns.size();i++) {
					if(split.length>i)
					columns.get(col).add(split[i]);
					else columns.get(col).add(null);
				}
					numRows++;
			}
		} catch (IOException e) {
			throw new SQLException(e);
		}
		description="Dummy database with schema "+columnNames+" and "+numRows+" rows from file "+values;
	}
	
	/** Executes a query */
	public ResultSet query(CharSequence sqlcs, int resultSetType,
			int resultSetConcurrency) throws SQLException {
		String sql = prepareQuery(sqlcs.toString());
		if (sql.toUpperCase().startsWith("INSERT")
				|| sql.toUpperCase().startsWith("UPDATE")
				|| sql.toUpperCase().startsWith("DELETE")
				|| sql.toUpperCase().startsWith("CREATE")
				|| sql.toUpperCase().startsWith("DROP")
				|| sql.toUpperCase().startsWith("ALTER")) {
			executeUpdate(sql);
			return (null);
		}
		Matcher m=Pattern.compile("(?i)SELECT (.*) FROM.*").matcher(sqlcs);
		if(!m.matches()) throw new SQLException("Unsupported query "+sqlcs);
		if(m.group(1).equals("*")) return(new DummyResultSet(columns));
		List<List<String>> cols = new ArrayList<List<String>>();
		for(String col : m.group(1).toLowerCase().split(",")) {
			int pos=columnNames.indexOf(col.trim());
			if(pos==-1) cols.add(Arrays.asList(new String[numRows]));
			else cols.add(columns.get(pos));
		}
		return (new DummyResultSet(cols));
	}

	/** Wraps just the data */
	public static class DummyResultSet implements ResultSet {

		/** Holds the table */
		public List<List<String>> columns = new ArrayList<List<String>>();
		
		/** Points to the current row */
		public int index = -1;

		/** Number of rows*/
		public int numRows=0;
		
		/** Constructs a result set */
		public DummyResultSet(List<List<String>> columns) {
			this.columns=columns;
			if(columns.size()!=0) numRows=columns.get(0).size();
		}

		@Override
		public boolean absolute(int row) {
			if (row >= numRows)
				return (false);
			index = row;
			return true;
		}

		@Override
		public void afterLast() {

		}

		@Override
		public void beforeFirst() {
			index = -1;
		}

		@Override
		public void cancelRowUpdates() {
		}

		@Override
		public void clearWarnings() {
		}

		@Override
		public void close() {
		}

		@Override
		public void deleteRow() {
		}

		@Override
		public int findColumn(String columnLabel) {
			return -1;
		}

		@Override
		public boolean first() {
			if (numRows == 0)
				return false;
			index = 0;
			return (true);
		}

		@Override
		public Array getArray(int columnIndex) {
			return null;

		}

		@Override
		public Array getArray(String columnLabel) {
			return null;

		}

		@Override
		public InputStream getAsciiStream(int columnIndex) {
			return null;

		}

		@Override
		public InputStream getAsciiStream(String columnLabel) {
			return null;
		}

		@Override
		public BigDecimal getBigDecimal(int columnIndex) {
			return null;
		}

		@Override
		public BigDecimal getBigDecimal(String columnLabel) {
			return null;
		}

		@Override
		public BigDecimal getBigDecimal(int columnIndex, int scale) {

			return null;
		}

		@Override
		public BigDecimal getBigDecimal(String columnLabel, int scale) {

			return null;
		}

		@Override
		public InputStream getBinaryStream(int columnIndex) {

			return null;
		}

		@Override
		public InputStream getBinaryStream(String columnLabel) {

			return null;
		}

		@Override
		public Blob getBlob(int columnIndex) {

			return null;
		}

		@Override
		public Blob getBlob(String columnLabel) {

			return null;
		}

		@Override
		public boolean getBoolean(int columnIndex) {
			return (columns.get(columnIndex).get(index).equals("true")
					|| columns.get(columnIndex).get(index).equals("1") || columns.get(columnIndex).get(index).equals("yes"));
		}

		@Override
		public boolean getBoolean(String columnLabel) {

			return false;
		}

		@Override
		public byte getByte(int columnIndex) {
			return (Byte.parseByte(columns.get(columnIndex).get(index)));

		}

		@Override
		public byte getByte(String columnLabel) {

			return 0;
		}

		@Override
		public byte[] getBytes(int columnIndex) {

			return null;
		}

		@Override
		public byte[] getBytes(String columnLabel) {

			return null;
		}

		@Override
		public Reader getCharacterStream(int columnIndex) {

			return null;
		}

		@Override
		public Reader getCharacterStream(String columnLabel) {

			return null;
		}

		@Override
		public Clob getClob(int columnIndex) {

			return null;
		}

		@Override
		public Clob getClob(String columnLabel) {

			return null;
		}

		@Override
		public int getConcurrency() {

			return 0;
		}

		@Override
		public String getCursorName() {

			return null;
		}

		@Override
		public Date getDate(int columnIndex) {

			return null;
		}

		@Override
		public Date getDate(String columnLabel) {

			return null;
		}

		@Override
		public Date getDate(int columnIndex, Calendar cal) {

			return null;
		}

		@Override
		public Date getDate(String columnLabel, Calendar cal) {

			return null;
		}

		@Override
		public double getDouble(int columnIndex) {
			return (Double.parseDouble(columns.get(columnIndex).get(index)));

		}

		@Override
		public double getDouble(String columnLabel) {

			return 0;
		}

		@Override
		public int getFetchDirection() {

			return 0;
		}

		@Override
		public int getFetchSize() {

			return 0;
		}

		@Override
		public float getFloat(int columnIndex) {
			return (Float.parseFloat(columns.get(columnIndex).get(index)));

		}

		@Override
		public float getFloat(String columnLabel) {

			return 0;
		}

		@Override
		public int getHoldability() {

			return 0;
		}

		@Override
		public int getInt(int columnIndex) {
			return (Integer.parseInt(columns.get(columnIndex).get(index)));
		}

		@Override
		public int getInt(String columnLabel) {

			return 0;
		}

		@Override
		public long getLong(int columnIndex) {
			return (Long.parseLong(columns.get(columnIndex).get(index)));

		}

		@Override
		public long getLong(String columnLabel) {

			return 0;
		}

		@Override
		public ResultSetMetaData getMetaData() {

			return new ResultSetMetaData() {

				@Override
				public String getCatalogName(int column) {
					return null;
				}

				@Override
				public String getColumnClassName(int column) {
					return null;
				}

				@Override
				public int getColumnCount() {
					return(columns.size());
				}

				@Override
				public int getColumnDisplaySize(int column) {
					return 20;
				}

				@Override
				public String getColumnLabel(int column) {
					return "Column"+column;
				}

				@Override
				public String getColumnName(int column) {
					return "Column"+column;
				}

				@Override
				public int getColumnType(int column) {
					return Types.VARCHAR;
				}

				@Override
				public String getColumnTypeName(int column) {
					return "VARCHAR";
				}

				@Override
				public int getPrecision(int column) {
					return 0;
				}

				@Override
				public int getScale(int column) {
					return 0;
				}

				@Override
				public String getSchemaName(int column) {
					return null;
				}

				@Override
				public String getTableName(int column) {
					return "DummyTable";
				}

				@Override
				public boolean isAutoIncrement(int column) {
					return false;
				}

				@Override
				public boolean isCaseSensitive(int column) {
					return true;
				}

				@Override
				public boolean isCurrency(int column) {
					return false;
				}

				@Override
				public boolean isDefinitelyWritable(int column) {
					return false;
				}

				@Override
				public int isNullable(int column) {
					return 0;
				}

				@Override
				public boolean isReadOnly(int column) {
					return true;
				}

				@Override
				public boolean isSearchable(int column) {
					return false;
				}

				@Override
				public boolean isSigned(int column) {
					return false;
				}

				@Override
				public boolean isWritable(int column) {
					return false;
				}

				@Override
				public boolean isWrapperFor(Class<?> iface) {
					return false;
				}

				@Override
				public <T> T unwrap(Class<T> iface) {
					return null;
				}
				
			};
		}

		@Override
		public Reader getNCharacterStream(int columnIndex) {

			return null;
		}

		@Override
		public Reader getNCharacterStream(String columnLabel) {

			return null;
		}

		@Override
		public NClob getNClob(int columnIndex) {

			return null;
		}

		@Override
		public NClob getNClob(String columnLabel) {

			return null;
		}

		@Override
		public String getNString(int columnIndex) {

			return null;
		}

		@Override
		public String getNString(String columnLabel) {

			return null;
		}

		@Override
		public Object getObject(int columnIndex) {
			return (columns.get(columnIndex-1)
					.get(index));

		}

		@Override
		public Object getObject(String columnLabel) {

			return null;
		}

		@Override
		public Object getObject(int columnIndex, Map<String, Class<?>> map) {

			return null;
		}

		@Override
		public Object getObject(String columnLabel, Map<String, Class<?>> map) {

			return null;
		}

		@Override
		public Ref getRef(int columnIndex) {

			return null;
		}

		@Override
		public Ref getRef(String columnLabel) {

			return null;
		}

		@Override
		public int getRow() {

			return index;
		}

		@Override
		public RowId getRowId(int columnIndex) {

			return null;
		}

		@Override
		public RowId getRowId(String columnLabel) {

			return null;
		}

		@Override
		public SQLXML getSQLXML(int columnIndex) {

			return null;
		}

		@Override
		public SQLXML getSQLXML(String columnLabel) {

			return null;
		}

		@Override
		public short getShort(int columnIndex) {
			return (Short.parseShort(columns.get(columnIndex-1).get(index)));

		}

		@Override
		public short getShort(String columnLabel) {

			return 0;
		}

		@Override
		public Statement getStatement() {

			return null;
		}

		@Override
		public String getString(int columnIndex) {

			return columns.get(columnIndex-1).get(index);
		}

		@Override
		public String getString(String columnLabel) {

			return null;
		}

		@Override
		public Time getTime(int columnIndex) {

			return null;
		}

		@Override
		public Time getTime(String columnLabel) {

			return null;
		}

		@Override
		public Time getTime(int columnIndex, Calendar cal) {

			return null;
		}

		@Override
		public Time getTime(String columnLabel, Calendar cal) {

			return null;
		}

		@Override
		public Timestamp getTimestamp(int columnIndex) {

			return null;
		}

		@Override
		public Timestamp getTimestamp(String columnLabel) {

			return null;
		}

		@Override
		public Timestamp getTimestamp(int columnIndex, Calendar cal) {

			return null;
		}

		@Override
		public Timestamp getTimestamp(String columnLabel, Calendar cal) {

			return null;
		}

		@Override
		public int getType() {

			return 0;
		}

		@Override
		public URL getURL(int columnIndex) throws SQLException {

			try {
				return new URL(columns.get(columnIndex-1).get(index));
			} catch (MalformedURLException e) {
				throw new SQLException(e);
			}

		}

		@Override
		public URL getURL(String columnLabel) {

			return null;
		}

		@Override
		public InputStream getUnicodeStream(String columnLabel) {

			return null;
		}

		@Override
		public SQLWarning getWarnings() {

			return null;
		}

		@Override
		public void insertRow() {

		}

		@Override
		public boolean isAfterLast() {

			return false;
		}

		@Override
		public boolean isBeforeFirst() {

			return false;
		}

		@Override
		public boolean isClosed() {

			return false;
		}

		@Override
		public boolean isFirst() {

			return index == 0;
		}

		@Override
		public boolean isLast() {

			return index == columns.size() - 1;
		}

		@Override
		public boolean last() {

			return index == columns.size() - 1;
		}

		@Override
		public void moveToCurrentRow() {

		}

		@Override
		public void moveToInsertRow() {

		}

		@Override
		public boolean next() {
			index++;
			return index < numRows;
		}

		@Override
		public boolean previous() {
			if (index < 0)
				return false;
			index--;
			return (true);
		}

		@Override
		public void refreshRow() {

		}

		@Override
		public boolean relative(int rows) {

			return false;
		}

		@Override
		public boolean rowDeleted() {

			return false;
		}

		@Override
		public boolean rowInserted() {

			return false;
		}

		@Override
		public boolean rowUpdated() {

			return false;
		}

		@Override
		public void setFetchDirection(int direction) {

		}

		@Override
		public void setFetchSize(int rows) {

		}

		@Override
		public void updateArray(int columnIndex, Array x) {

		}

		@Override
		public void updateArray(String columnLabel, Array x) {

		}

		@Override
		public void updateAsciiStream(int columnIndex, InputStream x) {

		}

		@Override
		public void updateAsciiStream(String columnLabel, InputStream x) {

		}

		@Override
		public void updateAsciiStream(int columnIndex, InputStream x, int length) {

		}

		@Override
		public void updateAsciiStream(String columnLabel, InputStream x,
				int length) {

		}

		@Override
		public void updateAsciiStream(int columnIndex, InputStream x,
				long length) {

		}

		@Override
		public void updateAsciiStream(String columnLabel, InputStream x,
				long length) {

		}

		@Override
		public void updateBigDecimal(int columnIndex, BigDecimal x) {

		}

		@Override
		public void updateBigDecimal(String columnLabel, BigDecimal x) {

		}

		@Override
		public void updateBinaryStream(int columnIndex, InputStream x) {

		}

		@Override
		public void updateBinaryStream(String columnLabel, InputStream x) {

		}

		@Override
		public void updateBinaryStream(int columnIndex, InputStream x,
				int length) {

		}

		@Override
		public void updateBinaryStream(String columnLabel, InputStream x,
				int length) {

		}

		@Override
		public void updateBinaryStream(int columnIndex, InputStream x,
				long length) {

		}

		@Override
		public void updateBinaryStream(String columnLabel, InputStream x,
				long length) {

		}

		@Override
		public void updateBlob(int columnIndex, Blob x) {

		}

		@Override
		public void updateBlob(String columnLabel, Blob x) {

		}

		@Override
		public void updateBlob(int columnIndex, InputStream inputStream) {

		}

		@Override
		public void updateBlob(String columnLabel, InputStream inputStream) {

		}

		@Override
		public void updateBlob(int columnIndex, InputStream inputStream,
				long length) {

		}

		@Override
		public void updateBlob(String columnLabel, InputStream inputStream,
				long length) {

		}

		@Override
		public void updateBoolean(int columnIndex, boolean x) {

		}

		@Override
		public void updateBoolean(String columnLabel, boolean x) {

		}

		@Override
		public void updateByte(int columnIndex, byte x) {

		}

		@Override
		public void updateByte(String columnLabel, byte x) {

		}

		@Override
		public void updateBytes(int columnIndex, byte[] x) {

		}

		@Override
		public void updateBytes(String columnLabel, byte[] x) {

		}

		@Override
		public void updateCharacterStream(int columnIndex, Reader x) {

		}

		@Override
		public void updateCharacterStream(String columnLabel, Reader reader) {

		}

		@Override
		public void updateCharacterStream(int columnIndex, Reader x, int length) {

		}

		@Override
		public void updateCharacterStream(String columnLabel, Reader reader,
				int length) {

		}

		@Override
		public void updateCharacterStream(int columnIndex, Reader x, long length) {

		}

		@Override
		public void updateCharacterStream(String columnLabel, Reader reader,
				long length) {

		}

		@Override
		public void updateClob(int columnIndex, Clob x) {

		}

		@Override
		public void updateClob(String columnLabel, Clob x) {

		}

		@Override
		public void updateClob(int columnIndex, Reader reader) {

		}

		@Override
		public void updateClob(String columnLabel, Reader reader) {

		}

		@Override
		public void updateClob(int columnIndex, Reader reader, long length) {

		}

		@Override
		public void updateClob(String columnLabel, Reader reader, long length) {

		}

		@Override
		public void updateDate(int columnIndex, Date x) {

		}

		@Override
		public void updateDate(String columnLabel, Date x) {

		}

		@Override
		public void updateDouble(int columnIndex, double x) {

		}

		@Override
		public void updateDouble(String columnLabel, double x) {

		}

		@Override
		public void updateFloat(int columnIndex, float x) {

		}

		@Override
		public void updateFloat(String columnLabel, float x) {

		}

		@Override
		public void updateInt(int columnIndex, int x) {

		}

		@Override
		public void updateInt(String columnLabel, int x) {

		}

		@Override
		public void updateLong(int columnIndex, long x) {

		}

		@Override
		public void updateLong(String columnLabel, long x) {

		}

		@Override
		public void updateNCharacterStream(int columnIndex, Reader x) {

		}

		@Override
		public void updateNCharacterStream(String columnLabel, Reader reader) {

		}

		@Override
		public void updateNCharacterStream(int columnIndex, Reader x,
				long length) {

		}

		@Override
		public void updateNCharacterStream(String columnLabel, Reader reader,
				long length) {

		}

		@Override
		public void updateNClob(int columnIndex, NClob nClob) {

		}

		@Override
		public void updateNClob(String columnLabel, NClob nClob) {

		}

		@Override
		public void updateNClob(int columnIndex, Reader reader) {

		}

		@Override
		public void updateNClob(String columnLabel, Reader reader) {

		}

		@Override
		public void updateNClob(int columnIndex, Reader reader, long length) {

		}

		@Override
		public void updateNClob(String columnLabel, Reader reader, long length) {

		}

		@Override
		public void updateNString(int columnIndex, String nString) {

		}

		@Override
		public void updateNString(String columnLabel, String nString) {

		}

		@Override
		public void updateNull(int columnIndex) {

		}

		@Override
		public void updateNull(String columnLabel) {

		}

		@Override
		public void updateObject(int columnIndex, Object x) {

		}

		@Override
		public void updateObject(String columnLabel, Object x) {

		}

		@Override
		public void updateObject(int columnIndex, Object x, int scaleOrLength) {

		}

		@Override
		public void updateObject(String columnLabel, Object x, int scaleOrLength) {

		}

		@Override
		public void updateRef(int columnIndex, Ref x) {

		}

		@Override
		public void updateRef(String columnLabel, Ref x) {

		}

		@Override
		public void updateRow() {

		}

		@Override
		public void updateRowId(int columnIndex, RowId x) {

		}

		@Override
		public void updateRowId(String columnLabel, RowId x) {

		}

		@Override
		public void updateSQLXML(int columnIndex, SQLXML xmlObject) {

		}

		@Override
		public void updateSQLXML(String columnLabel, SQLXML xmlObject) {

		}

		@Override
		public void updateShort(int columnIndex, short x) {

		}

		@Override
		public void updateShort(String columnLabel, short x) {

		}

		@Override
		public void updateString(int columnIndex, String x) {

		}

		@Override
		public void updateString(String columnLabel, String x) {

		}

		@Override
		public void updateTime(int columnIndex, Time x) {

		}

		@Override
		public void updateTime(String columnLabel, Time x) {

		}

		@Override
		public void updateTimestamp(int columnIndex, Timestamp x) {

		}

		@Override
		public void updateTimestamp(String columnLabel, Timestamp x) {

		}

		@Override
		public boolean wasNull() {

			return false;
		}

		@Override
		public boolean isWrapperFor(Class<?> iface) {

			return false;
		}

		@Override
		public <T> T unwrap(Class<T> iface) {

			return null;
		}

		public <T> T getObject(int arg0, Class<T> arg1) {
			// TODO Auto-generated method stub
			return null;
		}

		public <T> T getObject(String arg0, Class<T> arg1) {
			// TODO Auto-generated method stub
			return null;
		}

    @Override
    public InputStream getUnicodeStream(int columnIndex) {
      // TODO Auto-generated method stub
      return null;
    }

	}
	
	public static void main(String[] args) {
		Database d=new DummyDatabase(Arrays.asList("arg1","relation","arg2"),
				"Albert_Einstein","bornOnDate","1879-03-14",
				"Elvis_Presley","bornIn","Tupelo"
				);
		d.runInterface();
	}

  @Override
  public void connect() {
    // TODO Auto-generated method stub
    
  }
}
