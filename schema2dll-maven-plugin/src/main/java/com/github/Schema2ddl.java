package com.github;

import java.util.logging.Level;
import java.util.logging.Logger;

import com.googlecode.scheme2ddl.UserObjectJobRunner;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.plugins.annotations.LifecyclePhase;
import org.apache.maven.plugins.annotations.Mojo;
import org.springframework.context.ConfigurableApplicationContext;

/**
 * Goal which exports an Oracle schema to set of ddl scripts.
 */
@Mojo(name = "schema2ddl",
      defaultPhase = LifecyclePhase.PROCESS_RESOURCES)
public class Schema2ddl extends AbstractSchema2dllMojo {

  public void execute() throws MojoExecutionException, MojoFailureException {
    System.out.println("schema2dll action");
    ConfigurableApplicationContext context = loadApplicationContext();
    modifyContext(context);
    validateContext(context);

    System.out.println("DDL object filter: " + objectFilter);
    System.out.println("DDL type filter: " + typeFilter);
    System.out.println("DDL type filter mode: " + typeFilterMode);
    try {
      new UserObjectJobRunner().start(context, isLaunchedByDBA, objectFilter.toLowerCase(),
                                      typeFilter.toUpperCase(), typeFilterMode.toLowerCase());
    } catch (Exception ex) {
      Logger.getLogger(Schema2ddl.class.getName()).log(Level.SEVERE, null, ex);
    }

  }

}
