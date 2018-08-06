package com.carrotsearch.ramdir2;

enum ExitStatus {
  SUCCESS                     (0),
  ERROR_OTHER                 (1),
  ERROR_INVALID_ARGUMENTS     (2);

  public final int code;

  private ExitStatus(int systemExitCode) {
    this.code = systemExitCode;
  }
}
