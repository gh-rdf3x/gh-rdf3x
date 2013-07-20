package de.mpii.rdf3x;

import java.sql.*;

// RDF-3X
// (c) 2009 Thomas Neumann. Web site: http://www.mpi-inf.mpg.de/~neumann/rdf3x
//
// This work is licensed under the Creative Commons
// Attribution-Noncommercial-Share Alike 3.0 Unported License. To view a copy
// of this license, visit http://creativecommons.org/licenses/by-nc-sa/3.0/
// or send a letter to Creative Commons, 171 Second Street, Suite 300,
// San Francisco, California, 94105, USA.

final class ResultSetMetaData implements java.sql.ResultSetMetaData
{
   // The header
   private final String[] header;

   // Constructor
   ResultSetMetaData(String[] header) {
      this.header=header;
   }

   // Catalog name
   public String getCatalogName(int column) { return "default"; }
   // Column class name
   public String getColumnClassName(int column) { return "java.lang.String"; }
   // Number of columns
   public int getColumnCount() { return header.length; }
   // Column widths
   public int getColumnDisplaySize(int column) { return 0; }
   // Column label
   public String getColumnLabel(int column) { return header[column-1]; }
   // Column name
   public String getColumnName(int column) { return header[column-1]; }
   /// Column type
   public int getColumnType(int column) { return Types.VARCHAR; }
   /// Column type
   public String getColumnTypeName(int column) { return "VARCHAR"; }
   /// Precision
   public int getPrecision(int column) { return 0; }
   /// Scale
   public int getScale(int column) { return 0; }
   /// Schema name
   public String getSchemaName(int column) { return ""; }
   /// Table name
   public String getTableName(int column) { return ""; }
   /// Auto-increment
   public boolean isAutoIncrement(int column) { return false; }
   /// Case sensitive
   public boolean isCaseSensitive(int column) { return true; }
   /// Currency
   public boolean isCurrency(int column) { return false; }
   /// Writable
   public boolean isDefinitelyWritable(int column) { return false; }
   /// Nullable
   public int isNullable(int column) { return columnNullableUnknown; }
   /// Readonly
   public boolean isReadOnly(int column) { return true; }
   /// Searchable
   public boolean isSearchable(int column) { return true; }
   /// Signed number
   public boolean isSigned(int column) { return false; }
   /// Writable
   public boolean isWritable(int column) { return false; }

   // Wrapper?
   public boolean isWrapperFor(Class<?> iface) { return false; }
   // Unwrap
   public <T> T	unwrap(Class<T> iface) throws SQLException { throw new SQLException(); }
}
