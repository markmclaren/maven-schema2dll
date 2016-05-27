package com.github;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Logger;

import com.googlecode.scheme2ddl.FileNameConstructor;
import com.googlecode.scheme2ddl.UserObjectProcessor;
import com.googlecode.scheme2ddl.UserObjectReader;
import com.googlecode.scheme2ddl.UserObjectWriter;
import com.googlecode.scheme2ddl.dao.ConnectionDao;
import oracle.jdbc.pool.OracleDataSource;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugins.annotations.Parameter;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.context.support.FileSystemXmlApplicationContext;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.util.Assert;

import static com.googlecode.scheme2ddl.Main.outputPath;
import static com.googlecode.scheme2ddl.Main.parallelCount;

public abstract class AbstractSchema2dllMojo extends AbstractMojo {

  private final static Logger log = Logger.getLogger(AbstractSchema2dllMojo.class.getName());

  private final static String defaultConfigLocation = "scheme2ddl.config.xml";

  /**
   * JDBC connection URL (e.g. jdbc:oracle:thin:@localhost:1521/ORCL)
   */
  @Parameter(required = true)
  String jdbcUrl;

  /**
   * Database username (e.g. scott)
   */
  @Parameter(required = true)
  String username;

  /**
   * Database password (e.g. tiger)
   */
  @Parameter(required = true)
  String password;

  /**
   * Output directory
   */
  @Parameter
  String outputDirectory;

  /**
   * Number of parallel threads
   */
  @Parameter(defaultValue = "4")
  String parallelThreads;

  /**
   * Custom configuration location
   */
  @Parameter
  String customConfigLocation;

  /**
   * Filter for specific DDL objects, every LIKE wildcard can be used
   */
  @Parameter
  String filter;

  /**
   * Filter for specific DDL object types
   */
  @Parameter
  String typeFilter = "";

  /**
   * Mode for type filter: include or exclude
   */
  @Parameter(defaultValue = "include")
  String typeFilterMode;

  /**
   * Stop on getting DDL error
   */
  @Parameter(defaultValue = "false")
  boolean stopOnWarning = Boolean.FALSE;

  /**
   * Replace actual sequence values with 1
   */
  @Parameter(defaultValue = "true")
  boolean replaceSequenceValues = Boolean.TRUE;

  /**
   * A comma separated list of schemas for processing (works only if connected to oracle as sysdba)
   */
  @Parameter
  String schemas;

  private List<String> schemaList;

  protected boolean isLaunchedByDBA;

  protected String objectFilter = "%";

  ConfigurableApplicationContext loadApplicationContext() {
    ConfigurableApplicationContext context = null;
    if (customConfigLocation != null) {
      context = new FileSystemXmlApplicationContext(customConfigLocation);
    } else {
      context = new ClassPathXmlApplicationContext(defaultConfigLocation);
    }
    return context;
  }

  void modifyContext(final ConfigurableApplicationContext context) {
    OracleDataSource dataSource = (OracleDataSource) context.getBean("dataSource");
    dataSource.setURL(jdbcUrl);
    // for OracleDataSource in connectionCachingEnabled mode need explicitly set user and password
    dataSource.setUser(username);
    dataSource.setPassword(password);
    if (outputPath != null) {
      UserObjectWriter writer = (UserObjectWriter) context.getBean("writer");
      writer.setOutputPath(outputPath);
    }
    if (parallelCount > 0) {
      SimpleAsyncTaskExecutor taskExecutor = (SimpleAsyncTaskExecutor) context.getBean(
        "taskExecutor");
      taskExecutor.setConcurrencyLimit(parallelCount);
    }
    String userName = ((OracleDataSource) context.getBean("dataSource")).getUser();
    isLaunchedByDBA = userName.toLowerCase().matches(".+as +sysdba *");
    if (!isLaunchedByDBA) {
      ConnectionDao connectionDao = (ConnectionDao) context.getBean("connectionDao");
      isLaunchedByDBA = connectionDao.hasSelectCatalogRole(); //todo rename isLaunchedByDBA -> processForeignSchema
    }
    //process schemas
    processSchemas(context);

    FileNameConstructor fileNameConstructor = retrieveFileNameConstructor(context);   //will create new one if not exist
    if (isLaunchedByDBA) {
      fileNameConstructor.setTemplate(fileNameConstructor.getTemplateForSysDBA());
      fileNameConstructor.afterPropertiesSet();
    }

    if (stopOnWarning) {
      UserObjectProcessor processor = (UserObjectProcessor) context.getBean("processor");
      processor.setStopOnWarning(stopOnWarning);
    }
    if (replaceSequenceValues) {
      UserObjectProcessor processor = (UserObjectProcessor) context.getBean("processor");
      processor.setReplaceSequenceValues(replaceSequenceValues);
    }

  }

  private void processSchemas(final ConfigurableApplicationContext context) {
    List<String> listFromContext = retrieveSchemaListFromContext(context);
    if (schemas == null) {
      if (listFromContext.isEmpty()) {
        //get default schema from username
        schemaList = extactSchemaListFromUserName(context);
      } else if (isLaunchedByDBA) {
        schemaList = new ArrayList<String>(listFromContext);
      } else {
        log.warning(
          "Ignore 'schemaList' from advanced config, becouse oracle user is not connected as sys dba");
        schemaList = extactSchemaListFromUserName(context);
      }
    } else {
      String[] array = schemas.split(",");
      schemaList = new ArrayList<String>(Arrays.asList(array));
    }

    listFromContext.clear();
    for (String s : schemaList) {
      listFromContext.add(s.toUpperCase().trim());
    }

    //for compabality with old config
    if (listFromContext.size() == 1) {
      try {
        UserObjectReader userObjectReader = (UserObjectReader) context.getBean("reader");
        userObjectReader.setSchemaName(listFromContext.get(0));
      } catch (ClassCastException e) {
        // this mean that new config used, nothing to do
      }
    }
  }

  private static List<String> extactSchemaListFromUserName(
    final ConfigurableApplicationContext context) {
    OracleDataSource dataSource = (OracleDataSource) context.getBean("dataSource");
    String schemaName = dataSource.getUser().split(" ")[0];
    List<String> list = new ArrayList<String>();
    list.add(schemaName);
    return list;
  }

  private void fillSchemaListFromUserName(final ConfigurableApplicationContext context) {
    OracleDataSource dataSource = (OracleDataSource) context.getBean("dataSource");
    String schemaName = dataSource.getUser().split(" ")[0];
    schemaList = new ArrayList<String>();
    schemaList.add(schemaName);
  }

  /**
   * @param context
   *
   * @return existing bean 'schemaList', if this exists, or create and register new bean
   */
  private static List<String> retrieveSchemaListFromContext(
    final ConfigurableApplicationContext context) {
    List list;
    try {
      list = (List) context.getBean("schemaList");
    } catch (NoSuchBeanDefinitionException e) {
      DefaultListableBeanFactory beanFactory = (DefaultListableBeanFactory) context.getBeanFactory();
      beanFactory.registerBeanDefinition("schemaList", BeanDefinitionBuilder.rootBeanDefinition(
                                         ArrayList.class).getBeanDefinition());
      list = (List) context.getBean("schemaList");
    }
    return list;
  }

  /**
   * @param context
   *
   * @return existing bean 'fileNameConstructor', if this exists, or create and register new bean
   */
  private static FileNameConstructor retrieveFileNameConstructor(
    final ConfigurableApplicationContext context) {
    FileNameConstructor fileNameConstructor;
    try {
      fileNameConstructor = (FileNameConstructor) context.getBean("fileNameConstructor");
    } catch (NoSuchBeanDefinitionException e) {
      DefaultListableBeanFactory beanFactory = (DefaultListableBeanFactory) context.getBeanFactory();
      beanFactory.registerBeanDefinition("fileNameConstructor",
                                         BeanDefinitionBuilder.rootBeanDefinition(
                                           FileNameConstructor.class).getBeanDefinition());
      fileNameConstructor = (FileNameConstructor) context.getBean("fileNameConstructor");
      fileNameConstructor.afterPropertiesSet();
      //for compatability with old config without fileNameConstructor bean
      UserObjectProcessor userObjectProcessor = (UserObjectProcessor) context.getBean("processor");
      userObjectProcessor.setFileNameConstructor(fileNameConstructor);
    }
    return fileNameConstructor;
  }

  void validateContext(final ConfigurableApplicationContext context) {
    String userName = ((OracleDataSource) context.getBean("dataSource")).getUser().toUpperCase();
    schemaList = (List) context.getBean("schemaList");
    Assert.state(isLaunchedByDBA || schemaList.size() == 1,
                 "Cannot process multiply schemas if oracle user is not connected as sysdba");
    if (!isLaunchedByDBA) {
      String schemaName = schemaList.get(0).toUpperCase();
      Assert.state(userName.startsWith(schemaName),
                   String.format(
                     "Cannot process schema '%s' with oracle user '%s', if it's not connected as sysdba",
                     schemaName, userName.toLowerCase()));
    }
  }

}
