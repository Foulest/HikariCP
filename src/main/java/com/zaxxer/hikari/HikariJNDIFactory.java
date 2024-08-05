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
package com.zaxxer.hikari;

import com.zaxxer.hikari.util.PropertyElf;
import lombok.NoArgsConstructor;
import lombok.Synchronized;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import javax.naming.*;
import javax.naming.spi.ObjectFactory;
import javax.sql.DataSource;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Properties;
import java.util.Set;

/**
 * A JNDI factory that produces HikariDataSource instances.
 *
 * @author Brett Wooldridge
 */
@NoArgsConstructor
@SuppressWarnings("unused")
public class HikariJNDIFactory implements ObjectFactory {

    @Override
    @Synchronized
    public @Nullable Object getObjectInstance(Object obj, Name name, Context nameCtx,
                                              Hashtable<?, ?> environment) throws NamingException {
        // We only know how to deal with <code>javax.naming.Reference</code>
        // that specify a class name of "javax.sql.DataSource"
        if (obj instanceof Reference && "javax.sql.DataSource".equals(((Reference) obj).getClassName())) {
            Reference ref = (Reference) obj;
            Set<String> hikariPropSet = PropertyElf.getPropertyNames(HikariConfig.class);
            Properties properties = new Properties();
            Enumeration<RefAddr> enumeration = ref.getAll();

            while (enumeration.hasMoreElements()) {
                RefAddr element = enumeration.nextElement();
                String type = element.getType();

                if (type.startsWith("dataSource.") || hikariPropSet.contains(type)) {
                    properties.setProperty(type, element.getContent().toString());
                }
            }
            return createDataSource(properties, nameCtx);
        }
        return null;
    }

    private DataSource createDataSource(@NotNull Properties properties, Context context) throws NamingException {
        String jndiName = properties.getProperty("dataSourceJNDI");

        if (jndiName != null) {
            return lookupJndiDataSource(properties, context, jndiName);
        }
        return new HikariDataSource(new HikariConfig(properties));
    }

    @SuppressWarnings("MethodMayBeStatic")
    @Contract("_, null, _ -> fail")
    private @Nullable DataSource lookupJndiDataSource(Properties properties,
                                                      Context context, String jndiName) throws NamingException {
        if (context == null) {
            throw new RuntimeException("JNDI context does not found for dataSourceJNDI : " + jndiName);
        }

        DataSource jndiDS = (DataSource) context.lookup(jndiName);

        if (jndiDS == null) {
            Context ic = new InitialContext();
            jndiDS = (DataSource) ic.lookup(jndiName);
            ic.close();
        }

        if (jndiDS != null) {
            HikariConfig config = new HikariConfig(properties);
            config.setDataSource(jndiDS);
            return new HikariDataSource(config);
        }
        return null;
    }
}
