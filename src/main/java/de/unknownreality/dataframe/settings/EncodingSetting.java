package de.unknownreality.dataframe.settings;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public class EncodingSetting extends ColumnSetting {
    public static EncodingSetting UTF8 = new EncodingSetting(StandardCharsets.UTF_8);

    private final Charset charset;

    public EncodingSetting(Charset charset) {
        this.charset = charset;
    }

    public Charset getCharset() {
        return charset;
    }
}
