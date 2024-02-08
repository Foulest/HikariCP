package com.zaxxer.hikari;

import com.zaxxer.hikari.util.PropertyElf;
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
public class HikariJNDIFactory implements ObjectFactory {

    @Override
    synchronized public Object getObjectInstance(Object obj, Name name, Context nameCtx,
                                                 Hashtable<?, ?> environment) throws Exception {
        // We only know how to deal with <code>javax.naming.Reference</code> that specify a class name of "javax.sql.DataSource"
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
