package com.carrotsearch.ramdir2;

import java.io.PrintStream;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

import org.apache.logging.log4j.core.config.Configurator;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.MissingCommandException;
import com.beust.jcommander.ParameterException;

/**
 */
public final class Main {
  public void exec(String... args) throws Exception {
    final ArgsHelp parsedArgs = new ArgsHelp();
    final JCommander jc = new JCommander(parsedArgs);
    jc.setProgramName(getClass().getSimpleName());

    List<CommandModule> commandModules = Arrays.asList(
        new RamDirCmd()
    );
    commandModules.forEach((module) -> jc.addCommand(module));

    ExitStatus exitStatus = ExitStatus.SUCCESS;
    try {
      jc.parse(args);

      if (parsedArgs.configureLogging) {
        configureLogging();
      }
      final String commandName = jc.getParsedCommand();
      if (commandName == null || parsedArgs.help) {
        displayCommandHelp(System.out, jc);
      } else {
        final JCommander commandParser = jc.getCommands().get(commandName);
        List<Object> objects = commandParser.getObjects();
        assert objects.size() == 1: "Expected exactly one object for command '" + commandName + "': " + objects;
        CommandModule commandModule = (CommandModule) objects.get(0);

        if (commandModule.helpDelegate.help) {
          displayHelp(System.out, commandParser);
        } else if (!commandModule.validateArguments()) {
          exitStatus = ExitStatus.ERROR_INVALID_ARGUMENTS;
        } else {
          commandModule.execute();          
        }
      }
    } catch (MissingCommandException e) {
      if (parsedArgs.help) {
        displayCommandHelp(System.out, jc);
        exitStatus = ExitStatus.ERROR_INVALID_ARGUMENTS;
      }
    } catch (ParameterException e) {
      Loggers.CONSOLE.error(
          "Invalid argument (type {} for help): {}", ArgsHelp.ARG_HELP, e.getMessage());
      exitStatus = ExitStatus.ERROR_INVALID_ARGUMENTS;
    } catch (Exception e) {
      Loggers.CONSOLE.error("Unhandled exception: " + e.toString(), e);
      exitStatus = ExitStatus.ERROR_OTHER;
    }

    if (parsedArgs.callSystemExit) {
      System.exit(exitStatus.code);
    }
  }

  private void displayCommandHelp(PrintStream pw, JCommander jc) {
    pw.println(String.format(Locale.ROOT,
        "The following commands are available (type command name followed by %s for options):\n",
        ArgsHelp.ARG_HELP));
    pw.println(jc.getCommands().keySet().stream()
          .map(cmd -> String.format(Locale.ROOT, "  %-18s %s", cmd, jc.getCommandDescription(cmd)))
          .collect(Collectors.joining("\n")));
  }

  private void configureLogging() {
    try {
      Configurator.initialize("log4j2-default.xml",
          getClass().getClassLoader(),
          getClass().getClassLoader().getResource("log4j2-default.xml").toURI());
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }  

  private static void displayHelp(PrintStream pw, JCommander jc) {
    StringBuilder sb = new StringBuilder();
    jc.usage(sb, "");
    pw.print(sb);
  }

  public static void main(String[] args) throws Exception {
    new Main().exec(args);
  }
}
