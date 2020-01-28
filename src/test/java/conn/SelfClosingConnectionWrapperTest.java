package org.marchio.crystal;

import java.sql.Connection;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.junit.Test;

import mockit.Expectations;
import mockit.Injectable;
import mockit.Mocked;
import mockit.Verifications;

/**
 * Validate that SelfClosingConnectionWrapper closes wrapped connection after spefified timeout
 * and that automatically reopens it on later request. 
 * 
 * You can see detail of performed actions in trace log output.
 * 
 * @author Juan.Marchionatto
 *
 */
public class SelfClosingConnectionWrapperTest {

	
	
	@Test
	public void check_connection_auto_closes_and_is_reopened_on_new_request(
				final @Injectable DataSource dataSource
				,final @Mocked Connection connection
			) throws SQLException, InterruptedException {
		
		final long FIVE_SECS_IN_MILLISECS = 2 * 1000;
		
		new Expectations() {{
			dataSource.getConnection(); result = connection;
			
			// first call is initial check, second is check by auto-closing method, third is when action requested after auto-close
			connection.isClosed(); result = new boolean[] {false, false, true}; 
		}};
		
		@SuppressWarnings("resource")
		SelfClosingConnectionWrapper selfClosingConnection = new SelfClosingConnectionWrapper( dataSource, FIVE_SECS_IN_MILLISECS );
		
		// use connection immediately after requesting it
		selfClosingConnection.createStatement();

		Thread.sleep( FIVE_SECS_IN_MILLISECS + 1000 ); // wait enought for wrapper to close wrapped connection 
		
		new Verifications() {{
			connection.close();  // SelfClosingConnection should have closed wrapped connection after timeout
		}};
		
		// if action requested after self-closing timeout, wrapper should reopen it automatically 
		selfClosingConnection.createStatement();
	}


}
