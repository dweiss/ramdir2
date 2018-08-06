package org.apache.lucene.store;

@FunctionalInterface
interface ThrowingConsumer<T> {
    void accept(T t) throws Exception;
}
