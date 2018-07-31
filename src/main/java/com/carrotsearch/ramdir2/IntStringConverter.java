package com.carrotsearch.ramdir2;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import com.beust.jcommander.IStringConverter;

public class IntStringConverter implements IStringConverter<List<Integer>> {

  @Override
  public List<Integer> convert(String value) {
    return Arrays.stream(value.split("\\,\\s"))
        .map(v -> Integer.parseInt(value))
        .collect(Collectors.toList());
  }
}
