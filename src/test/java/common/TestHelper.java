package common;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

public class TestHelper {
    private static Boolean loaded = false;

    public static void loadProps() throws IOException {
        if(loaded)
            return;

        loaded = true;

        final var props = new Properties();

        final var input = TestHelper.class.getResourceAsStream("/config.properties");
        if (input == null) {
            throw new FileNotFoundException("config.properties not found");
        }

        props.load(input);
        props.forEach((key, value) -> System.setProperty(key.toString(), value.toString()));
    }
}
