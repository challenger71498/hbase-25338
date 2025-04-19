package hbase;

import common.TestHelper;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.URL;
import java.net.URLClassLoader;

import static org.junit.jupiter.api.Assertions.*;

public class ConnectionTest {
    private static final String TABLE_NAME = "tb1";
    private static final String ROW_KEY = "r1";

    private static URLClassLoader classLoader;

    private static Object config;

    private static Object connection;
    private static Object admin;
    private static Object table;

    @BeforeAll
    public static void beforeAll() throws IOException, ClassNotFoundException, NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        TestHelper.loadProps();

        final var file = new File("hbase-wrapper/build/libs/hbase-wrapper-1.0-SNAPSHOT.jar");
        final var urls = new URL[] {file.toURI().toURL()};

        classLoader = new URLClassLoader(urls, ClassLoader.getPlatformClassLoader());

        // Load HBaseConfiguration
        final var klass = classLoader.loadClass("org.apache.hadoop.hbase.HBaseConfiguration");
        config = klass.getMethod("create").invoke(null);

        try {
            // Set config
            setConfig(config, "hbase.zookeeper.quorum");
            setConfig(config, "hbase.zookeeper.property.clientPort");
            setConfig(config, "hbase.security.authentication");
            setConfig(config, "hadoop.security.authentication");

            if(System.getProperty("hadoop.security.authentication").equals("kerberos")) {
                setConfig(config, "hbase.master.kerberos.principal");
                setConfig(config, "hbase.regionserver.kerberos.principal");

                // Set UGI
                final var principal = System.getProperty("user.kerberos.principal");
                final var keytabPath = System.getProperty("user.keytab.file");

                final var ugi = classLoader.loadClass("org.apache.hadoop.security.UserGroupInformation");
                final var setConfiguration = ugi.getMethod("setConfiguration", config.getClass());
                final var loginUserFromKeytab = ugi.getMethod("loginUserFromKeytab", String.class, String.class);

                setConfiguration.invoke(ugi, config);
                loginUserFromKeytab.invoke(loginUserFromKeytab, principal, keytabPath);
            }

            setConfig(config, "hbase.client.retries.number", "1");
            setConfig(config, "hbase.rpc.timeout", "15000");
            setConfig(config, "hbase.client.operation.timeout", "15000");

            // Create connection
            final var connectionFactory = classLoader.loadClass("org.apache.hadoop.hbase.client.ConnectionFactory");
            final var createConnection = connectionFactory.getMethod("createConnection", config.getClass());

            connection = createConnection.invoke(connectionFactory, config);

            // Get admin
            final var getAdmin = connection.getClass().getMethod("getAdmin");
            admin = getAdmin.invoke(connection);

            // Get table
            final var tableNameClass = classLoader.loadClass("org.apache.hadoop.hbase.TableName");
            final var valueOf = tableNameClass.getMethod("valueOf", String.class);
            final var tableName = valueOf.invoke(null, TABLE_NAME);

            final var getTable = connection.getClass().getMethod("getTable", tableNameClass);
            table = getTable.invoke(connection, tableName);
        }
        catch (Exception e) {
            // Release properly on error
            release();

            throw e;
        }
    }

    private static void setConfig(Object config, String key) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        setConfig(config, key, System.getProperty(key));
    }

    private static void setConfig(Object config, String key, String value) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        final var set = config.getClass().getMethod("set", String.class, String.class);
        set.invoke(config, key, value);
    }

    @AfterAll
    public static void afterAll() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException, IOException {
        // Release properly
        release();

        classLoader.close();
    }

    private static void release() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        if(table != null) {
            table.getClass().getMethod("close").invoke(table);
        }

        if(admin != null) {
            admin.getClass().getMethod("close").invoke(admin);
        }

        if(connection != null) {
            connection.getClass().getMethod("close").invoke(connection);
        }
    }

    @Test
    public void get_WithExtraProvider_DoesNotThrow() throws ClassNotFoundException, NoSuchMethodException, InvocationTargetException, IllegalAccessException, InstantiationException {
        setConfig(config, "hbase.client.sasl.provider.extras", "org.apache.hadoop.hbase.security.provider.SimpleSaslClientAuthenticationProvider,org.apache.hadoop.hbase.security.provider.GssSaslClientAuthenticationProvider,org.apache.hadoop.hbase.security.provider.DigestSaslClientAuthenticationProvider");

        executeTableGet();
    }

    @Test
    public void get_WithoutExtraProvider_DoesThrowIllegalStateException() throws ClassNotFoundException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
        executeTableGet();
    }

    private void executeTableGet() throws ClassNotFoundException, NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {
        final var bytesClass = classLoader.loadClass("org.apache.hadoop.hbase.util.Bytes");
        final var toBytes = bytesClass.getMethod("toBytes", String.class);

        final var getClass = classLoader.loadClass("org.apache.hadoop.hbase.client.Get");
        final var getCtor = getClass.getConstructor(byte[].class);

        final var get = getCtor.newInstance(toBytes.invoke(null, ROW_KEY));

        final var tableGet = table.getClass().getMethod("get", getClass);
        final var result = tableGet.invoke(table, get);

        assertNotNull(result);
    }
}
