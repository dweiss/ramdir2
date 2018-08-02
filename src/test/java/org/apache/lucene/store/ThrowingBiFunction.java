package org.apache.lucene.store;

@FunctionalInterface
interface ThrowingBiFunction<T, U, R> {
  R apply(T t, U u) throws Exception;
}