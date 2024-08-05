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

import com.zaxxer.hikari.pool.*;
import javassist.*;
import javassist.bytecode.ClassFile;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.lang.reflect.Array;
import java.sql.*;
import java.util.*;

/**
 * This class generates the proxy objects for {@link Connection}, {@link Statement},
 * {@link PreparedStatement}, and {@link CallableStatement}. Additionally, it injects
 * method bodies into the {@link ProxyFactory} class methods that can instantiate
 * instances of the generated proxies.
 *
 * @author Brett Wooldridge
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class JavassistProxyFactory {

    private static ClassPool classPool;
    private static String genDirectory = "";

    public static void main(String @NotNull ... args)
            throws NotFoundException, CannotCompileException, IOException,
            ClassNotFoundException, NoSuchMethodException {
        classPool = new ClassPool();
        classPool.importPackage("java.sql");
        classPool.appendClassPath(new LoaderClassPath(JavassistProxyFactory.class.getClassLoader()));

        if (args.length > 0) {
            genDirectory = args[0];
        }

        // Cast is not needed for these
        String methodBody = "{ try { return delegate.method($$); }"
                + " catch (SQLException e) { throw checkException(e); } }";

        generateProxyClass(Connection.class, ProxyConnection.class.getName(), methodBody);
        generateProxyClass(Statement.class, ProxyStatement.class.getName(), methodBody);
        generateProxyClass(ResultSet.class, ProxyResultSet.class.getName(), methodBody);
        generateProxyClass(DatabaseMetaData.class, ProxyDatabaseMetaData.class.getName(), methodBody);

        // For these we have to cast the delegate
        methodBody = "{ try { return ((cast) delegate).method($$); }"
                + " catch (SQLException e) { throw checkException(e); } }";

        generateProxyClass(PreparedStatement.class, ProxyPreparedStatement.class.getName(), methodBody);
        generateProxyClass(CallableStatement.class, ProxyCallableStatement.class.getName(), methodBody);

        modifyProxyFactory();
    }

    private static void modifyProxyFactory()
            throws NotFoundException, CannotCompileException, IOException {
        log.debug("Generating method bodies for com.zaxxer.hikari.proxy.ProxyFactory");

        String packageName = ProxyConnection.class.getPackage().getName();
        CtClass proxyCt = classPool.getCtClass("com.zaxxer.hikari.pool.ProxyFactory");

        for (CtMethod method : proxyCt.getMethods()) {
            switch (method.getName()) {
                case "getProxyConnection":
                    method.setBody("{return new " + packageName + ".HikariProxyConnection($$);}");
                    break;
                case "getProxyStatement":
                    method.setBody("{return new " + packageName + ".HikariProxyStatement($$);}");
                    break;
                case "getProxyPreparedStatement":
                    method.setBody("{return new " + packageName + ".HikariProxyPreparedStatement($$);}");
                    break;
                case "getProxyCallableStatement":
                    method.setBody("{return new " + packageName + ".HikariProxyCallableStatement($$);}");
                    break;
                case "getProxyResultSet":
                    method.setBody("{return new " + packageName + ".HikariProxyResultSet($$);}");
                    break;
                case "getProxyDatabaseMetaData":
                    method.setBody("{return new " + packageName + ".HikariProxyDatabaseMetaData($$);}");
                    break;
                default:
                    // unhandled method
                    break;
            }
        }

        proxyCt.writeFile(genDirectory + "target/classes");
    }

    /**
     * Generate Javassist Proxy Classes
     */
    private static <T> void generateProxyClass(Class<T> primaryInterface, @NotNull String superClassName, String methodBody)
            throws NotFoundException, CannotCompileException, IOException, ClassNotFoundException, NoSuchMethodException {
        String newClassName = superClassName.replaceAll("(.+)\\.(\\w+)", "$1.Hikari$2");
        CtClass superCt = classPool.getCtClass(superClassName);

        CtClass targetCt = classPool.makeClass(newClassName, superCt);
        targetCt.setModifiers(Modifier.PUBLIC);

        log.debug("Generating {}", newClassName);

        // Make a set of method signatures we inherit implementation for, so we don't generate delegates for these
        Collection<String> superSigs = new HashSet<>();
        for (CtMethod method : superCt.getMethods()) {
            if ((method.getModifiers() & Modifier.FINAL) == Modifier.FINAL) {
                superSigs.add(method.getName() + method.getSignature());
            }
        }

        Collection<String> methods = new HashSet<>();

        for (Class<?> intf : getAllInterfaces(primaryInterface)) {
            CtClass intfCt = classPool.getCtClass(intf.getName());
            targetCt.addInterface(intfCt);

            for (CtMethod intfMethod : intfCt.getDeclaredMethods()) {
                String signature = intfMethod.getName() + intfMethod.getSignature();

                // Don't generate delegates for methods we override, and
                // ignore already added methods that come from other interfaces
                if (superSigs.contains(signature) || methods.contains(signature)) {
                    continue;
                }

                // Track what methods we've added
                methods.add(signature);

                // Clone the method we want to inject into
                CtMethod method = CtNewMethod.copy(intfMethod, targetCt, null);

                String modifiedBody = methodBody;

                // If the super-Proxy has concrete methods (non-abstract),
                // transform the call into a simple super.method() call
                CtMethod superMethod = superCt.getMethod(intfMethod.getName(), intfMethod.getSignature());
                if ((superMethod.getModifiers() & Modifier.ABSTRACT) != Modifier.ABSTRACT
                        && !isDefaultMethod(intf, intfMethod)) {
                    modifiedBody = modifiedBody.replace("((cast) ", "");
                    modifiedBody = modifiedBody.replace("delegate", "super");
                    modifiedBody = modifiedBody.replace("super)", "super");
                }

                modifiedBody = modifiedBody.replace("cast", primaryInterface.getName());

                // Generate a method that simply invokes the same method on the delegate
                if (isThrowsSqlException(intfMethod)) {
                    modifiedBody = modifiedBody.replace("method", method.getName());
                } else {
                    modifiedBody = "{ return ((cast) delegate).method($$); }".replace("method",
                            method.getName()).replace("cast", primaryInterface.getName());
                }

                if (method.getReturnType() == CtClass.voidType) {
                    modifiedBody = modifiedBody.replace("return", "");
                }

                method.setBody(modifiedBody);
                targetCt.addMethod(method);
            }
        }

        targetCt.getClassFile().setMajorVersion(ClassFile.JAVA_8);
        targetCt.writeFile(genDirectory + "target/classes");
    }

    private static boolean isThrowsSqlException(@NotNull CtMethod method) {
        try {
            for (CtClass clazz : method.getExceptionTypes()) {
                if (clazz.getSimpleName().equals("SQLException")) {
                    return true;
                }
            }
        } catch (NotFoundException ignored) {
        }
        return false;
    }

    private static boolean isDefaultMethod(Class<?> intf, @NotNull CtMethod intfMethod)
            throws NotFoundException, NoSuchMethodException, ClassNotFoundException {
        List<Class<?>> paramTypes = new ArrayList<>();

        for (CtClass pt : intfMethod.getParameterTypes()) {
            paramTypes.add(toJavaClass(pt));
        }

        return intf.getDeclaredMethod(intfMethod.getName(),
                paramTypes.toArray(new Class[0])).toString().contains("default ");
    }

    private static @NotNull Set<Class<?>> getAllInterfaces(@NotNull Class<?> clazz) {
        Set<Class<?>> interfaces = new LinkedHashSet<>();

        for (Class<?> intf : clazz.getInterfaces()) {
            if (intf.getInterfaces().length > 0) {
                interfaces.addAll(getAllInterfaces(intf));
            }

            interfaces.add(intf);
        }

        if (clazz.getSuperclass() != null) {
            interfaces.addAll(getAllInterfaces(clazz.getSuperclass()));
        }

        if (clazz.isInterface()) {
            interfaces.add(clazz);
        }
        return interfaces;
    }

    private static Class<?> toJavaClass(@NotNull CtClass cls) throws ClassNotFoundException {
        if (cls.getName().endsWith("[]")) {
            return Array.newInstance(toJavaClass(cls.getName()
                    .replace("[]", "")), 0).getClass();
        } else {
            return toJavaClass(cls.getName());
        }
    }

    private static Class<?> toJavaClass(@NotNull String cn) throws ClassNotFoundException {
        switch (cn) {
            case "int":
                return int.class;
            case "long":
                return long.class;
            case "short":
                return short.class;
            case "byte":
                return byte.class;
            case "float":
                return float.class;
            case "double":
                return double.class;
            case "boolean":
                return boolean.class;
            case "char":
                return char.class;
            case "void":
                return void.class;
            default:
                return Class.forName(cn);
        }
    }
}
