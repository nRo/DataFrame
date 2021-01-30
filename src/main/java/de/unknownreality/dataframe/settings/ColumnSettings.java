package de.unknownreality.dataframe.settings;

import java.util.*;

public class ColumnSettings {
    private final Map<Class<? extends ColumnSetting>, ColumnSetting> settingMap = new HashMap<>();

    public ColumnSettings() {

    }

    public <T extends ColumnSetting> T remove(T setting) {
        Set<ColumnSetting> settingList = new HashSet<>(settingMap.values());
        if (settingList.contains(setting)) {
            settingMap.remove(setting.getClass());
            return setting;
        }
        return null;
    }

    public <T extends ColumnSetting> T remove(Class<T> cl) {
        Object o = settingMap.remove(cl);
        if (!cl.isInstance(o)) {
            return null;
        }
        return cl.cast(o);
    }

    public void applyTo(ColumnSettings settings) {
        settingMap.values().forEach(settings::add);
    }

    public void add(ColumnSetting setting) {
        settingMap.put(setting.getClass(), setting);
    }

    public <T extends ColumnSetting> T get(Class<T> cl) {
        Object o = settingMap.get(cl);
        if (cl.isInstance(o)) {
            return cl.cast(o);
        }
        return null;
    }

    public <T extends ColumnSetting> T getOrDefault(Class<T> cl, T defaultSetting) {
        T v = get(cl);
        return v == null ? defaultSetting : v;
    }

    public static ColumnSettings create(Collection<ColumnSetting> settings) {
        ColumnSettings columnSettings = new ColumnSettings();
        settings.forEach(columnSettings::add);
        return columnSettings;
    }
}
