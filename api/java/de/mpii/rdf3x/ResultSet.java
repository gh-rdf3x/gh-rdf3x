package de.mpii.rdf3x;

import java.sql.*;
import java.util.Map;
import java.util.Properties;
import java.io.*;

// RDF-3X
// (c) 2009 Thomas Neumann. Web site: http://www.mpi-inf.mpg.de/~neumann/rdf3x
//
// This work is licensed under the Creative Commons
// Attribution-Noncommercial-Share Alike 3.0 Unported License. To view a copy
// of this license, visit http://creativecommons.org/licenses/by-nc-sa/3.0/
// or send a letter to Creative Commons, 171 Second Street, Suite 300,
// San Francisco, California, 94105, USA.

final class ResultSet implements java.sql.ResultSet
{
   // The header
   private String[] header;
   // The data
   private String[][] data;
   // The current position
   private int row;
   // The last column
   private int lastCol;

   // Constructor
   ResultSet(String[] header,String[][] data) {
      this.header=header;
      this.data=data;
      row=-1;
   }

   // Move absolutely
   public boolean absolute(int row) {
      if (row>0) {
         if (row>(data.length+1))
            return false;
         this.row=row-1;
         return true;
      } else {
         if ((-row)>data.length)
            return false;
         this.row=data.length-row;
         return true;
      }
   }
   // Move after the last entry
   public void afterLast() { row=data.length; }
   // Move before the first entry
   public void beforeFirst() throws SQLException { throw new SQLFeatureNotSupportedException(); }
   // Cancel all updates
   public void cancelRowUpdates() throws SQLException { throw new SQLFeatureNotSupportedException(); }
   // Clear all warnings
   public void clearWarnings() {}
   // Releases resources
   public void close() { data=null; }
   // Deletes the current row
   public void deleteRow() throws SQLException { throw new SQLFeatureNotSupportedException(); }
   // Find a column
   public int findColumn(String columnLabel) throws SQLException {
      for (int index=0;index<header.length;index++)
         if (header[index].equals(columnLabel))
            return index+1;
      throw new SQLException();
   }
   // Go to the first entry
   public boolean first() {
      row=0;
      return row<data.length;
   }
   // Get an entry as array
   public Array getArray(int columnIndex) throws SQLException { throw new SQLFeatureNotSupportedException(); }
   // Get an entry as array
   public Array	getArray(String columnLabel) throws SQLException { return getArray(findColumn(columnLabel)); }
   // Get an entry as ascii stream
   public InputStream getAsciiStream(int columnIndex) throws SQLException { throw new SQLFeatureNotSupportedException(); }
   // Get an entry as ascii stream
   public InputStream getAsciiStream(String columnLabel) throws SQLException { return getAsciiStream(findColumn(columnLabel)); }
   // Get an entry as big decimal
   public java.math.BigDecimal getBigDecimal(int columnIndex) throws SQLException { throw new SQLFeatureNotSupportedException(); }
   /**
     * Get an entry as big decimal
     * @deprecated
     */
   public java.math.BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException { throw new SQLFeatureNotSupportedException(); }
   // Get an entry as big decimal
   public java.math.BigDecimal getBigDecimal(String columnLabel) throws SQLException { return getBigDecimal(findColumn(columnLabel)); }
   /**
     * Get an entry as big decimal.
     * @deprecated
     */
   public java.math.BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException { return getBigDecimal(findColumn(columnLabel),scale); }
   // Get an entry as binary stream
   public InputStream getBinaryStream(int columnIndex) throws SQLException { throw new SQLFeatureNotSupportedException(); }
   // Get an entry as binary stream
   public InputStream getBinaryStream(String columnLabel) throws SQLException { return getBinaryStream(findColumn(columnLabel)); }
   // Get an entry as blob
   public Blob getBlob(int columnIndex) throws SQLException { throw new SQLFeatureNotSupportedException(); }
   // Get an entry as blob
   public Blob getBlob(String columnLabel) throws SQLException { return getBlob(findColumn(columnLabel)); }
   // Get an entry as boolean
   public boolean getBoolean(int columnIndex) throws SQLException { throw new SQLFeatureNotSupportedException(); }
   // Get an entry as boolean
   public boolean getBoolean(String columnLabel) throws SQLException { return getBoolean(findColumn(columnLabel)); }
   // Get an entry as byte
   public byte getByte(int columnIndex) throws SQLException { throw new SQLFeatureNotSupportedException(); }
   // Get an entry as byte
   public byte getByte(String columnLabel) throws SQLException { return getByte(findColumn(columnLabel)); }
   // Get an entry as bytes
   public  byte[] getBytes(int columnIndex) throws SQLException { throw new SQLFeatureNotSupportedException(); }
   // Get an entry as bytes
   public byte[] getBytes(String columnLabel) throws SQLException { return getBytes(findColumn(columnLabel)); }
   // Get an entry as character stream
   public Reader getCharacterStream(int columnIndex) throws SQLException { throw new SQLFeatureNotSupportedException(); }
   // Get an entry as character stream
   public Reader getCharacterStream(String columnLabel) throws SQLException { return getCharacterStream(findColumn(columnLabel)); }
   // Get an entry as clob
   public Clob getClob(int columnIndex) throws SQLException { throw new SQLFeatureNotSupportedException(); }
   // Get an entry as clob
   public Clob getClob(String columnLabel) throws SQLException { return getClob(findColumn(columnLabel)); }
   // Get the concurrency setting
   public int getConcurrency() { return java.sql.ResultSet.CONCUR_READ_ONLY; }
   // Get the cursor name
   public String getCursorName() throws SQLException { throw new SQLFeatureNotSupportedException(); }
   // Get an entry as date
   public Date getDate(int columnIndex) throws SQLException { throw new SQLFeatureNotSupportedException(); }
   // Get an entry as date
   public Date getDate(int columnIndex, java.util.Calendar cal) throws SQLException { throw new SQLFeatureNotSupportedException(); }
   // Get an entry as date
   public Date getDate(String columnLabel) throws SQLException { return getDate(findColumn(columnLabel)); }
   // Get an entry as date
   public Date getDate(String columnLabel, java.util.Calendar cal) throws SQLException { return getDate(findColumn(columnLabel),cal); }
   // Get an entry as double
   public double getDouble(int columnIndex) throws SQLException { throw new SQLFeatureNotSupportedException(); }
   // Get an entry as double
   public double getDouble(String columnLabel) throws SQLException { return getDouble(findColumn(columnLabel)); }
   // Get the fetch direction
   public int getFetchDirection() { return java.sql.ResultSet.FETCH_FORWARD; }
   // Get the fetch size
   public int getFetchSize() { return 0; }
   // Get an entry as float
   public float getFloat(int columnIndex) throws SQLException { throw new SQLFeatureNotSupportedException(); }
   // Get an entry as float
   public float getFloat(String columnLabel) throws SQLException { return getFloat(findColumn(columnLabel)); }
   // Get the holdability
   public int getHoldability() { return java.sql.ResultSet.CLOSE_CURSORS_AT_COMMIT; }
   // Get an entry as int
   public int getInt(int columnIndex) throws SQLException { throw new SQLFeatureNotSupportedException(); }
   // Get an entry as int
   public int getInt(String columnLabel) throws SQLException { return getInt(findColumn(columnLabel)); }
   // Get an entry as long
   public long getLong(int columnIndex) throws SQLException { throw new SQLFeatureNotSupportedException(); }
   // Get an entry as long
   public long getLong(String columnLabel) throws SQLException { return getLong(findColumn(columnLabel)); }
   // Get the meta data
   public java.sql.ResultSetMetaData getMetaData() { return new ResultSetMetaData(header); }
   // Get an entry as stream
   public Reader getNCharacterStream(int columnIndex) throws SQLException { throw new SQLFeatureNotSupportedException(); }
   // Get an entry as stream
   public Reader getNCharacterStream(String columnLabel) throws SQLException { return getNCharacterStream(findColumn(columnLabel)); }
   // Get an entry as nclob
   public NClob getNClob(int columnIndex) throws SQLException { throw new SQLFeatureNotSupportedException(); }
   // Get an entry as nclob
   public NClob getNClob(String columnLabel) throws SQLException { return getNClob(findColumn(columnLabel)); }
   // Get an entry as string
   public String getNString(int columnIndex) throws SQLException { return getString(columnIndex); }
   // Get an entry as string
   public String getNString(String columnLabel) throws SQLException { return getNString(findColumn(columnLabel)); }
   // Get an entry
   public Object getObject(int columnIndex) throws SQLException { return getString(columnIndex); }
   // Get an entry
   public Object getObject(int columnIndex, Map<String,Class<?>> map) throws SQLException { return getString(columnIndex); }
   // Get an entry
   public Object getObject(String columnLabel) throws SQLException { return getObject(findColumn(columnLabel)); }
   // Get an entry
   public Object getObject(String columnLabel, Map<String,Class<?>> map) throws SQLException { return getObject(findColumn(columnLabel),map); }
   // Get an entry as ref
   public Ref getRef(int columnIndex) throws SQLException { throw new SQLFeatureNotSupportedException(); }
   // Get an entry as ref
   public Ref getRef(String columnLabel) throws SQLException { return getRef(findColumn(columnLabel)); }
   // Get the current row number
   public int getRow() { return row+1; }
   // Get an entry as rowid
   public RowId getRowId(int columnIndex) throws SQLException { throw new SQLFeatureNotSupportedException(); }
   // Get an entry as rowid
   public RowId getRowId(String columnLabel) throws SQLException { throw new SQLFeatureNotSupportedException(); }
   // Get an entry as short
   public short getShort(int columnIndex) throws SQLException { throw new SQLFeatureNotSupportedException(); }
   // Get an entry as short
   public short getShort(String columnLabel) throws SQLException { return getShort(findColumn(columnLabel)); }
   // Get an entry as SQL
   public SQLXML getSQLXML(int columnIndex) throws SQLException { throw new SQLFeatureNotSupportedException(); }
   // Get an entry as SQL
   public SQLXML getSQLXML(String columnLabel) throws SQLException { throw new SQLFeatureNotSupportedException(); }
   // Get the corresponding statement
   public Statement getStatement() throws SQLException { throw new SQLFeatureNotSupportedException(); }
   // Get an entry as string
   public String getString(int columnIndex) throws SQLException {
      if ((row>=data.length)||(columnIndex<1)||(columnIndex>data[row].length))
         throw new SQLException();
      String s=data[row][columnIndex-1];
      lastCol=columnIndex;
      if ("NULL".equals(s))
         return null; else
         return s;
   }
   // Get an entry as string
   public String getString(String columnLabel) throws SQLException { return getString(findColumn(columnLabel)); }
   // Get an entry as time
   public Time getTime(int columnIndex) throws SQLException { throw new SQLFeatureNotSupportedException(); }
   // Get an entry as time
   public Time getTime(int columnIndex, java.util.Calendar cal) throws SQLException { throw new SQLFeatureNotSupportedException(); }
   // Get an entry as time
   public Time getTime(String columnLabel) throws SQLException { return getTime(findColumn(columnLabel)); }
   // Get an entry as tme
   public Time getTime(String columnLabel, java.util.Calendar cal) throws SQLException { return getTime(findColumn(columnLabel),cal); }
   // Get an entry as timestamp
   public Timestamp getTimestamp(int columnIndex) throws SQLException { throw new SQLFeatureNotSupportedException(); }
   // Get an entry as timestamp
   public Timestamp getTimestamp(int columnIndex, java.util.Calendar cal) throws SQLException { throw new SQLFeatureNotSupportedException(); }
   // Get an entry as timestamp
   public Timestamp getTimestamp(String columnLabel) throws SQLException { return getTimestamp(findColumn(columnLabel)); }
   // Get an entry as timestamp
   public Timestamp getTimestamp(String columnLabel, java.util.Calendar cal) throws SQLException { return getTimestamp(findColumn(columnLabel),cal); }
   // Get the type
   public int getType() { return java.sql.ResultSet.TYPE_FORWARD_ONLY; }
   /**
     * Get an entry as unicode stream
     * @deprecated
     */
   public InputStream getUnicodeStream(int columnIndex) throws SQLException { throw new SQLFeatureNotSupportedException(); }
   /**
     * Get an entry as unicode stream
     * @deprecated
     */
   public InputStream getUnicodeStream(String columnLabel) throws SQLException { return getUnicodeStream(findColumn(columnLabel)); }
   // Get an entry as URL
   public java.net.URL getURL(int columnIndex) throws SQLException { throw new SQLFeatureNotSupportedException(); }
   // Get an entry as URL
   public java.net.URL getURL(String columnLabel) throws SQLException { return getURL(findColumn(columnLabel)); }
   // Get warnings
   public SQLWarning getWarnings() { return null; }
   // Insert a row
   public void insertRow() throws SQLException { throw new SQLFeatureNotSupportedException(); }
   // After the last row
   public boolean isAfterLast() { return row>=data.length; }
   // Before the first row
   public boolean isBeforeFirst() { return false; }
   // Closed
   public boolean isClosed() { return data==null; }
   // At first row
   public boolean isFirst() { return row==0; }
   // At last row
   public boolean isLast() { return row==(data.length-1); }
   // Go to the last row
   public boolean last() {
      if (data.length>0) {
         row=data.length-1;
         return true;
      } else return false;
   }
   // Move the cursor
   public void moveToCurrentRow() throws SQLException { throw new SQLFeatureNotSupportedException(); }
   // Move the cursor
   public void moveToInsertRow() throws SQLException { throw new SQLFeatureNotSupportedException(); }
   // Go to the next row
   public boolean next() {
      if (row>=data.length)
         return false;
      ++row;
      return row<data.length;
   }
   // Go to the previous row
   public boolean previous() {
      if (row==0)
         return false;
      --row;
      return true;
   }
   // Refresh the current tow
   public void	refreshRow() {}
   // Move the cursor relatively
   public boolean relative(int rows) {
      if (rows>=0) {
         if (row+rows>=data.length) {
            row=data.length;
            return false;
         } else {
            row+=rows;
            return true;
         }
      } else {
         if (row+rows<0) {
            row=0;
            return true;
         } else {
            row+=rows;
            return true;
         }
      }
   }
   // Deleted
   public boolean rowDeleted() { return false; }
   // Inserted
   public boolean rowInserted() { return false; }
   // Updated
   public boolean rowUpdated() { return false; }
   // Fetch direction
   public void setFetchDirection(int direction) throws SQLException { throw new SQLFeatureNotSupportedException(); }
   // Fetch size
   public void setFetchSize(int rows) throws SQLException { throw new SQLFeatureNotSupportedException(); }
   // Update
   public void updateArray(int columnIndex, Array x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
   // Update
   public void updateArray(String columnLabel, Array x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
   // Update
   public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
   // Update
   public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException { throw new SQLFeatureNotSupportedException(); }
   // Update
   public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException { throw new SQLFeatureNotSupportedException(); }
   // Update
   public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
   // Update
   public void updateAsciiStream(String columnLabel, InputStream x, int length) throws SQLException { throw new SQLFeatureNotSupportedException(); }
   // Update
   public void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException { throw new SQLFeatureNotSupportedException(); }
   // Update
   public void updateBigDecimal(int columnIndex, java.math.BigDecimal x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
   // Update
   public void updateBigDecimal(String columnLabel, java.math.BigDecimal x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
   // Update
   public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
   // Update
   public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException { throw new SQLFeatureNotSupportedException(); }
   // Update
   public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException { throw new SQLFeatureNotSupportedException(); }
   // Update
   public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
   // Updare
   public void updateBinaryStream(String columnLabel, InputStream x, int length) throws SQLException { throw new SQLFeatureNotSupportedException(); }
   // Update
   public void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException { throw new SQLFeatureNotSupportedException(); }
   // Update
   public void updateBlob(int columnIndex, Blob x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
   // Update
   public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException { throw new SQLFeatureNotSupportedException(); }
   // Update
   public void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException { throw new SQLFeatureNotSupportedException(); }
   // Update
   public void updateBlob(String columnLabel, Blob x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
   // Update
   public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException { throw new SQLFeatureNotSupportedException(); }
   // Update
   public void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException { throw new SQLFeatureNotSupportedException(); }
   // Update
   public void updateBoolean(int columnIndex, boolean x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
   // Update
   public void updateBoolean(String columnLabel, boolean x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
   // Update
   public void updateByte(int columnIndex, byte x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
   // Update
   public void updateByte(String columnLabel, byte x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
   // Update
   public void updateBytes(int columnIndex, byte[] x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
   // Update
   public void updateBytes(String columnLabel, byte[] x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
   // Update
   public void updateCharacterStream(int columnIndex, Reader x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
   // Update
   public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException { throw new SQLFeatureNotSupportedException(); }
   // Update
   public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException { throw new SQLFeatureNotSupportedException(); }
   // Update
   public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException { throw new SQLFeatureNotSupportedException(); }
   // Update
   public void updateCharacterStream(String columnLabel, Reader reader, int length) throws SQLException { throw new SQLFeatureNotSupportedException(); }
   // Update
   public void updateCharacterStream(String columnLabel, Reader reader, long length) throws SQLException { throw new SQLFeatureNotSupportedException(); }
   // Update
   public void updateClob(int columnIndex, Clob x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
   // Update
   public void updateClob(int columnIndex, Reader reader) throws SQLException { throw new SQLFeatureNotSupportedException(); }
   // Update
   public void updateClob(int columnIndex, Reader reader, long length) throws SQLException { throw new SQLFeatureNotSupportedException(); }
   // Update
   public void updateClob(String columnLabel, Clob x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
   // Update
   public void updateClob(String columnLabel, Reader reader) throws SQLException { throw new SQLFeatureNotSupportedException(); }
   // Update
   public void updateClob(String columnLabel, Reader reader, long length) throws SQLException { throw new SQLFeatureNotSupportedException(); }
   // Update
   public void updateDate(int columnIndex, Date x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
   // Update
   public void updateDate(String columnLabel, Date x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
   // Update
   public void updateDouble(int columnIndex, double x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
   // Update
   public void updateDouble(String columnLabel, double x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
   // Update
   public void updateFloat(int columnIndex, float x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
   // Update
   public void updateFloat(String columnLabel, float x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
   // Update
   public void updateInt(int columnIndex, int x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
   // Update
   public void updateInt(String columnLabel, int x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
   // Update
   public void updateLong(int columnIndex, long x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
   // Update
   public void updateLong(String columnLabel, long x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
   // Update
   public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
   // Update
   public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException { throw new SQLFeatureNotSupportedException(); }
   // Update
   public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException { throw new SQLFeatureNotSupportedException(); }
   // Update
   public void updateNCharacterStream(String columnLabel, Reader reader, long length) throws SQLException { throw new SQLFeatureNotSupportedException(); }
   // Update
   public void updateNClob(int columnIndex, NClob nClob) throws SQLException { throw new SQLFeatureNotSupportedException(); }
   // Update
   public void updateNClob(int columnIndex, Reader reader) throws SQLException { throw new SQLFeatureNotSupportedException(); }
   // Update
   public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException { throw new SQLFeatureNotSupportedException(); }
   // Update
   public void updateNClob(String columnLabel, NClob nClob) throws SQLException { throw new SQLFeatureNotSupportedException(); }
   // Update
   public void updateNClob(String columnLabel, Reader reader) throws SQLException { throw new SQLFeatureNotSupportedException(); }
   // Update
   public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException { throw new SQLFeatureNotSupportedException(); }
   // Update
   public void updateNString(int columnIndex, String nString) throws SQLException { throw new SQLFeatureNotSupportedException(); }
   // Update
   public void updateNString(String columnLabel, String nString) throws SQLException { throw new SQLFeatureNotSupportedException(); }
   // Update
   public void updateNull(int columnIndex) throws SQLException { throw new SQLFeatureNotSupportedException(); }
   // Update
   public void updateNull(String columnLabel) throws SQLException { throw new SQLFeatureNotSupportedException(); }
   // Update
   public void updateObject(int columnIndex, Object x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
   // Update
   public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException { throw new SQLFeatureNotSupportedException(); }
   // Update
   public void updateObject(String columnLabel, Object x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
   // Update
   public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException { throw new SQLFeatureNotSupportedException(); }
   // Update
   public void updateRef(int columnIndex, Ref x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
   // Update
   public void updateRef(String columnLabel, Ref x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
   // Update
   public void updateRow() throws SQLException { throw new SQLFeatureNotSupportedException(); }
   // Update
   public void updateRowId(int columnIndex, RowId x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
   // Update
   public void updateRowId(String columnLabel, RowId x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
   // Update
   public void updateShort(int columnIndex, short x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
   // Update
   public void updateShort(String columnLabel, short x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
   // Update
   public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException { throw new SQLFeatureNotSupportedException(); }
   // Update
   public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException { throw new SQLFeatureNotSupportedException(); }
   // Update
   public void updateString(int columnIndex, String x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
   // Update
   public void updateString(String columnLabel, String x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
   // Update
   public void updateTime(int columnIndex, Time x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
   // Update
   public void updateTime(String columnLabel, Time x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
   // Update
   public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
   // Update
   public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
   // Was the last column NULL?
   public boolean wasNull() throws SQLException {
      return getString(lastCol)==null;
   }

   // Wrapper?
   public boolean isWrapperFor(Class<?> iface) { return false; }
   // Unwrap
   public <T> T	unwrap(Class<T> iface) throws SQLException { throw new SQLException(); }
}
