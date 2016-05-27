package com.github;

import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.plugins.annotations.LifecyclePhase;
import org.apache.maven.plugins.annotations.Mojo;

@Mojo(name = "schema2ddl",
      defaultPhase = LifecyclePhase.PROCESS_RESOURCES)
public class Schema2ddl extends AbstractSchema2dllMojo {

  public void execute() throws MojoExecutionException, MojoFailureException {
    System.out.println("schema2dll action");
  }

}
