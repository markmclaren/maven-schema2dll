package com.github;

import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugins.annotations.Parameter;

public abstract class AbstractSchema2dllMojo extends AbstractMojo {

  @Parameter
  String url;

  @Parameter
  String outputDirectory;

  @Parameter
  String parallelThreads;

  @Parameter
  String configPath;

  @Parameter
  String filter;

  @Parameter
  String typeFilter;

  @Parameter
  String typeFilterMode;

  @Parameter
  boolean stopOnWarning;

  @Parameter
  boolean replaceSequenceValues;

}
