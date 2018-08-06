package com.carrotsearch.ramdir2;

import com.beust.jcommander.Parameter;

class ArgsHelp {
  public final static String ARG_HELP = "--help";
  public final static String ARG_EXIT = "--exit";
  public final static String ARG_CONFIGURE_LOGGING = "--configure-logging";

  @Parameter(
      hidden = true,
      names = {ARG_HELP, "-h"},
      description = "Display command(s) help.",
      help = true)
  public boolean help;

  @Parameter(
      hidden = true,
      names = {ARG_EXIT},
      arity = 1,
      description = "Call System.exit() at end of command.")
  public boolean callSystemExit = true;

  @Parameter(
      hidden = true,
      names = {ARG_CONFIGURE_LOGGING},
      arity = 1,
      description = "Configure the logging subsystem.")
  public boolean configureLogging = true;
}

