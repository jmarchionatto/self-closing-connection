package conn;

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

import javax.sql.DataSource;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Wrapper for a Connection which closes it after a configured timeout
 * and intercepts calls in order to open a new connection when needed.
 * 
 * This is to avoid the connection pool exhaustion produced by Crystal Reports
 * wrong use of connections, which are not closed until user closes report
 * pages, which sometimes does not happen.
 * 
 * For the moment, the only modified override is for the createStatement method,
 * as that is the only which was detected to try to use connections after 
 * the initial use (which opens the connection). In case other method would be 
 * detected to try to use an already closed connection, the same pattern 
 * should be applied to the method override: adding the reopenWrappedIfNeeded
 * method call as in the createStatement method. Don't forget to test that 
 * new use works as expected. 
 * 
 * @author juan.marchionatto
 *
 */
public class SelfClosingConnectionWrapper implements Connection {
    protected transient final Logger log = LogManager.getLogger();

	private DataSource dataSource;
	private Connection proxiedConnection;
	private long autoCloseTimeMsecs;

	
	public SelfClosingConnectionWrapper( DataSource dataSource, long autoCloseTimeMsecs ) throws SQLException {
		super();
		this.dataSource = dataSource;
		this.autoCloseTimeMsecs = autoCloseTimeMsecs;
		this.proxiedConnection = dataSource.getConnection();
		log.debug("Creating");
		closeAfterTimeout( autoCloseTimeMsecs );
	}

	
	/**
	 * If the connection was (automatically) closed, obtain a new one
	 */
	private void reopenWrappedIfNeeded() throws SQLException {
		if ( proxiedConnection.isClosed() ) 	{
			log.debug("Reopening wrapped connection");
			proxiedConnection = dataSource.getConnection(); 
			closeAfterTimeout( autoCloseTimeMsecs );
		} else
			log.debug("Reopen not needed (it is still open)");
	}
	
	
	
	private void closeAfterTimeout( long millisecs ) {
		log.trace("Scheduling close");
		new java.util.Timer().schedule(
			    new java.util.TimerTask() {
			    	
			        @Override
			        public void run() {
						try {
							if ( proxiedConnection != null
									&& ! proxiedConnection.isClosed() ) {
								proxiedConnection.close();
								log.debug("Closing wrapped connection");
							}
							
						} catch (SQLException e) {
							log.error( "Error trying to close connection: " + e.getMessage() );
						}
			        }
			    }, 
			    millisecs 
			);		
	}
	
	
	@Override
	public <T> T unwrap(Class<T> iface) throws SQLException {
		log.debug(" ");
		return proxiedConnection.unwrap(iface);
	}

	@Override
	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		log.debug(" ");
		return proxiedConnection.isWrapperFor(iface);
	}

	@Override
	public Statement createStatement() throws SQLException {
		log.debug(" ");
		reopenWrappedIfNeeded();
		return proxiedConnection.createStatement();
	}

	@Override
	public PreparedStatement prepareStatement(String sql) throws SQLException {
		log.debug(" ");
		return proxiedConnection.prepareStatement(sql);
	}

	@Override
	public CallableStatement prepareCall(String sql) throws SQLException {
		log.debug(" ");
		return proxiedConnection.prepareCall(sql);
	}

	@Override
	public String nativeSQL(String sql) throws SQLException {
		log.debug(" ");
		return proxiedConnection.nativeSQL(sql);
	}

	@Override
	public void setAutoCommit(boolean autoCommit) throws SQLException {
		log.debug(" ");
		proxiedConnection.setAutoCommit(autoCommit);
	}

	@Override
	public boolean getAutoCommit() throws SQLException {
		log.debug(" ");
		return proxiedConnection.getAutoCommit();
	}

	@Override
	public void commit() throws SQLException {
		log.debug(" ");
		proxiedConnection.commit();
	}

	@Override
	public void rollback() throws SQLException {
		log.debug(" ");
		proxiedConnection.rollback();
	}

	@Override
	public void close() throws SQLException {
		log.debug(" ");
		proxiedConnection.close();
	}

	@Override
	public boolean isClosed() throws SQLException {
		log.debug(" ");
		return proxiedConnection.isClosed();
	}

	@Override
	public DatabaseMetaData getMetaData() throws SQLException {
		log.debug(" ");
		return proxiedConnection.getMetaData();
	}

	@Override
	public void setReadOnly(boolean readOnly) throws SQLException {
		log.debug(" ");
		proxiedConnection.setReadOnly(readOnly);
	}

	@Override
	public boolean isReadOnly() throws SQLException {
		log.debug(" ");
		return proxiedConnection.isReadOnly();
	}

	@Override
	public void setCatalog(String catalog) throws SQLException {
		log.debug(" ");
		proxiedConnection.setCatalog(catalog);
	}

	@Override
	public String getCatalog() throws SQLException {
		log.debug(" ");
		return proxiedConnection.getCatalog();
	}

	@Override
	public void setTransactionIsolation(int level) throws SQLException {
		log.debug(" ");
		proxiedConnection.setTransactionIsolation(level);
	}

	@Override
	public int getTransactionIsolation() throws SQLException {
		log.debug(" ");
		return proxiedConnection.getTransactionIsolation();
	}

	@Override
	public SQLWarning getWarnings() throws SQLException {
		log.debug(" ");
		return proxiedConnection.getWarnings();
	}

	@Override
	public void clearWarnings() throws SQLException {
		log.debug(" ");
		proxiedConnection.clearWarnings();
	}

	@Override
	public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
		log.debug(" ");
		return proxiedConnection.createStatement(resultSetType, resultSetConcurrency);
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency)
			throws SQLException {
		log.debug(" ");
		return proxiedConnection.prepareStatement(sql, resultSetType, resultSetConcurrency);
	}

	@Override
	public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency)
			throws SQLException {
		log.debug(" ");
		return proxiedConnection.prepareCall(sql, resultSetType, resultSetConcurrency);
	}

	@Override
	public Map<String, Class<?>> getTypeMap() throws SQLException {
		log.debug(" ");
		return proxiedConnection.getTypeMap();
	}

	@Override
	public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
		log.debug(" ");
		proxiedConnection.setTypeMap(map);
	}

	@Override
	public void setHoldability(int holdability) throws SQLException {
		log.debug(" ");
		proxiedConnection.setHoldability(holdability);
	}
	

	@Override
	public int getHoldability() throws SQLException {
		log.debug(" ");
		return proxiedConnection.getHoldability();
	}

	@Override
	public Savepoint setSavepoint() throws SQLException {
		log.debug(" ");
		return proxiedConnection.setSavepoint();
	}

	@Override
	public Savepoint setSavepoint(String name) throws SQLException {
		log.debug(" ");
		return proxiedConnection.setSavepoint(name);
	}

	@Override
	public void rollback(Savepoint savepoint) throws SQLException {
		log.debug(" ");
		proxiedConnection.rollback(savepoint);
	}

	@Override
	public void releaseSavepoint(Savepoint savepoint) throws SQLException {
		log.debug(" ");
		proxiedConnection.releaseSavepoint(savepoint);
	}

	@Override
	public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability)
			throws SQLException {
		log.debug(" ");
		return proxiedConnection.createStatement(resultSetType, resultSetConcurrency, resultSetHoldability);
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency,
			int resultSetHoldability) throws SQLException {
		log.debug(" ");
		return proxiedConnection.prepareStatement(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
	}

	@Override
	public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency,
			int resultSetHoldability) throws SQLException {
		log.debug(" ");
		return proxiedConnection.prepareCall(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
		log.debug(" ");
		return proxiedConnection.prepareStatement(sql, autoGeneratedKeys);
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
		log.debug(" ");
		return proxiedConnection.prepareStatement(sql, columnIndexes);
	}

	@Override
	public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
		log.debug(" ");
		return proxiedConnection.prepareStatement(sql, columnNames);
	}

	@Override
	public Clob createClob() throws SQLException {
		log.debug(" ");
		return proxiedConnection.createClob();
	}

	@Override
	public Blob createBlob() throws SQLException {
		log.debug(" ");
		return proxiedConnection.createBlob();
	}

	@Override
	public NClob createNClob() throws SQLException {
		log.debug(" ");
		return proxiedConnection.createNClob();
	}

	@Override
	public SQLXML createSQLXML() throws SQLException {
		log.debug(" ");
		return proxiedConnection.createSQLXML();
	}

	@Override
	public boolean isValid(int timeout) throws SQLException {
		log.debug(" ");
		return proxiedConnection.isValid(timeout);
	}

	@Override
	public void setClientInfo(String name, String value) throws SQLClientInfoException {
		log.debug(" ");
		proxiedConnection.setClientInfo(name, value);
	}

	@Override
	public void setClientInfo(Properties properties) throws SQLClientInfoException {
		log.debug(" ");
		proxiedConnection.setClientInfo(properties);
	}

	@Override
	public String getClientInfo(String name) throws SQLException {
		log.debug(" ");
		return proxiedConnection.getClientInfo(name);
	}

	@Override
	public Properties getClientInfo() throws SQLException {
		log.debug(" ");
		return proxiedConnection.getClientInfo();
	}

	@Override
	public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
		log.debug(" ");
		return proxiedConnection.createArrayOf(typeName, elements);
	}

	@Override
	public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
		log.debug(" ");
		return proxiedConnection.createStruct(typeName, attributes);
	}

	@Override
	public void setSchema(String schema) throws SQLException {
		log.debug(" ");
		proxiedConnection.setSchema(schema);
	}

	@Override
	public String getSchema() throws SQLException {
		log.debug(" ");
		return proxiedConnection.getSchema();
	}

	@Override
	public void abort(Executor executor) throws SQLException {
		log.debug(" ");
		proxiedConnection.abort(executor);
	}

	@Override
	public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
		log.debug(" ");
		proxiedConnection.setNetworkTimeout(executor, milliseconds);
	}

	@Override
	public int getNetworkTimeout() throws SQLException {
		log.debug(" ");
		return proxiedConnection.getNetworkTimeout();
	}
	
	
}
