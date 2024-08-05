/*
 * Copyright (C) 2013, 2014 Brett Wooldridge
 *
 * Modifications made by Foulest (https://github.com/Foulest)
 * for the HikariCP fork (https://github.com/Foulest/HikariCP).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.zaxxer.hikari.util;

import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.Serializable;
import java.lang.reflect.Array;
import java.util.*;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;

/**
 * Fast list without range checking.
 *
 * @author Brett Wooldridge
 */
@SuppressWarnings({"unused", "unchecked", "WeakerAccess"})
public final class FastList<T> implements List<T>, RandomAccess, Serializable {

    private static final long serialVersionUID = -4598088075242913858L;

    private final Class<?> clazz;

    private transient T[] elementData;
    private int size;

    /**
     * Construct a FastList with a default size of 32.
     *
     * @param clazz the Class stored in the collection
     */
    public FastList(Class<?> clazz) {
        elementData = (T[]) Array.newInstance(clazz, 32);
        this.clazz = clazz;
    }

    /**
     * Construct a FastList with a specified size.
     *
     * @param clazz    the Class stored in the collection
     * @param capacity the initial size of the FastList
     */
    public FastList(Class<?> clazz, int capacity) {
        elementData = (T[]) Array.newInstance(clazz, capacity);
        this.clazz = clazz;
    }

    /**
     * Construct a FastList by copying another FastList.
     *
     * @param other the FastList to copy
     */
    @Contract(pure = true)
    public FastList(@NotNull FastList<T> other) {
        clazz = other.clazz;
        size = other.size;
        elementData = Arrays.copyOf(other.elementData, other.elementData.length);
    }

    /**
     * Create a new FastList by copying another FastList.
     *
     * @param other the FastList to copy
     * @return a new FastList with the same elements as the other list
     */
    @Contract(value = "_ -> new", pure = true)
    public static <T> @NotNull FastList<T> copyOf(FastList<T> other) {
        return new FastList<>(other);
    }

    /**
     * Add an element to the tail of the FastList.
     *
     * @param element the element to add
     */
    @Override
    public boolean add(T element) {
        if (size < elementData.length) {
            elementData[size] = element;
            size++;
        } else {
            // overflow-conscious code
            int oldCapacity = elementData.length;
            int newCapacity = oldCapacity << 1;
            T[] newElementData = (T[]) Array.newInstance(clazz, newCapacity);
            System.arraycopy(elementData, 0, newElementData, 0, oldCapacity);
            newElementData[size] = element;
            size++;
            elementData = newElementData;
        }
        return true;
    }

    /**
     * Get the element at the specified index.
     *
     * @param index the index of the element to get
     * @return the element, or ArrayIndexOutOfBounds is thrown if the index is invalid
     */
    @Override
    public T get(int index) {
        return elementData[index];
    }

    /**
     * Remove the last element from the list.  No bound check is performed, so if this
     * method is called on an empty list and ArrayIndexOutOfBounds exception will be
     * thrown.
     *
     * @return the last element of the list
     */
    public T removeLast() {
        --size;
        T element = elementData[size];
        elementData[size] = null;
        return element;
    }

    /**
     * This remove method is most efficient when the element being removed
     * is the last element.  Equality is identity based, not equals() based.
     * Only the first matching element is removed.
     *
     * @param element the element to remove
     */
    @Override
    public boolean remove(Object element) {
        for (int index = size - 1; index >= 0; index--) {
            if (element == elementData[index]) {
                int numMoved = size - index - 1;

                if (numMoved > 0) {
                    System.arraycopy(elementData, index + 1, elementData, index, numMoved);
                }

                --size;
                elementData[size] = null;
                return true;
            }
        }
        return false;
    }

    /**
     * Clear the FastList.
     */
    @Override
    public void clear() {
        for (int i = 0; i < size; i++) {
            elementData[i] = null;
        }

        size = 0;
    }

    /**
     * Get the current number of elements in the FastList.
     *
     * @return the number of current elements
     */
    @Override
    public int size() {
        return size;
    }

    @Override
    public boolean isEmpty() {
        return size == 0;
    }

    @Override
    public T set(int index, T element) {
        T old = elementData[index];
        elementData[index] = element;
        return old;
    }

    @Override
    @Contract(mutates = "this")
    @SuppressWarnings("UnstableApiUsage")
    public @Nullable T remove(int index) {
        if (size == 0) {
            return null;
        }

        T old = elementData[index];
        int numMoved = size - index - 1;

        if (numMoved > 0) {
            System.arraycopy(elementData, index + 1, elementData, index, numMoved);
        }

        --size;
        elementData[size] = null;
        return old;
    }

    @Override
    public boolean contains(Object o) {
        throw new UnsupportedOperationException();
    }

    @Override
    @Contract(value = " -> new", pure = true)
    public @NotNull Iterator<T> iterator() {
        return new Iterator<T>() {
            private int index;

            @Override
            public boolean hasNext() {
                return index < size;
            }

            @Override
            public T next() {
                if (index < size) {
                    T t = elementData[index];
                    index++;
                    return t;
                }
                throw new NoSuchElementException("No more elements in FastList");
            }
        };
    }

    @Override
    public Object @NotNull [] toArray() {
        throw new UnsupportedOperationException();
    }

    @Override
    public <E> E @NotNull [] toArray(E @NotNull [] a) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean containsAll(@NotNull Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean addAll(@NotNull Collection<? extends T> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean addAll(int index, @NotNull Collection<? extends T> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean removeAll(@NotNull Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean retainAll(@NotNull Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void add(int index, T element) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int indexOf(Object o) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int lastIndexOf(Object o) {
        throw new UnsupportedOperationException();
    }

    @Override
    public @NotNull ListIterator<T> listIterator() {
        throw new UnsupportedOperationException();
    }

    @Override
    public @NotNull ListIterator<T> listIterator(int index) {
        throw new UnsupportedOperationException();
    }

    @Override
    public @NotNull List<T> subList(int fromIndex, int toIndex) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void forEach(Consumer<? super T> action) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Spliterator<T> spliterator() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean removeIf(Predicate<? super T> filter) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void replaceAll(UnaryOperator<T> operator) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void sort(Comparator<? super T> c) {
        throw new UnsupportedOperationException();
    }

    private void readObject(java.io.ObjectInputStream in) throws java.io.IOException, ClassNotFoundException {
        throw new java.io.NotSerializableException("com.zaxxer.hikari.util.FastList");
    }

    private void writeObject(java.io.ObjectOutputStream out) throws java.io.IOException {
        throw new java.io.NotSerializableException("com.zaxxer.hikari.util.FastList");
    }
}
