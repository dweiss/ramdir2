package com.carrotsearch.ramdir2;

import java.io.IOException;

import com.beust.jcommander.ParametersDelegate;

public abstract class CommandModule {
  @ParametersDelegate
  public ArgsHelp helpDelegate = new ArgsHelp();

  public abstract boolean validateArguments() throws IOException;
  public abstract void execute() throws Exception;
}
