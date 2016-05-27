package com.github;

import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.googlecode.scheme2ddl.dao.ConnectionDao;
import oracle.jdbc.pool.OracleDataSource;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.plugins.annotations.LifecyclePhase;
import org.apache.maven.plugins.annotations.Mojo;
import org.springframework.context.ConfigurableApplicationContext;

/**
 * Goal which tests an Oracle database connection.
 */
@Mojo(name = "test-connection",
      defaultPhase = LifecyclePhase.PROCESS_RESOURCES)
public class TestConnection extends AbstractSchema2dllMojo {

  public void execute() throws MojoExecutionException, MojoFailureException {
    System.out.println("test-connection action");
    ConfigurableApplicationContext context = loadApplicationContext();
    modifyContext(context);
    validateContext(context);
    try {
      testDBConnection(context);
    } catch (SQLException ex) {
      Logger.getLogger(TestConnection.class.getName()).log(Level.SEVERE, null, ex);
    }
  }

  private void testDBConnection(ConfigurableApplicationContext context) throws SQLException {
    ConnectionDao connectionDao = (ConnectionDao) context.getBean("connectionDao");
    OracleDataSource dataSource = (OracleDataSource) context.getBean("dataSource");
    if (connectionDao.isConnectionAvailable()) {
      System.out.println("OK success connection to " + dataSource.getURL());
    } else {
      System.out.println("FAIL connect to " + dataSource.getURL());
    }
  }

}
