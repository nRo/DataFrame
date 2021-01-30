package de.unknownreality.dataframe.settings;

import de.unknownreality.dataframe.DataFrameColumn;

import java.util.*;

public class DataFrameSettings {
    private final Map<ColumnSetting, List<ColumnMatcher>> columnSettings = new HashMap<>();

    public DataFrameSettings() {
    }

    public DataFrameSettings(Map<ColumnSetting, List<ColumnMatcher>> columnSettings) {
        columnSettings.forEach((s, m) -> columnSettings.put(s, new ArrayList<>(m)));
    }

    public void applyToColumn(DataFrameColumn<?, ?> column) {
        HashSet<ColumnSetting> settings = new HashSet<>();
        columnSettings.forEach((setting, matchers) -> {
            for (ColumnMatcher matcher : matchers) {
                if (matcher.match(column)) {
                    settings.add(setting);
                    break;
                }
            }
        });
        settings.forEach((s) -> column.getSettings().add(s));
    }

    /*public DataFrameSettings add(ColumnSetting setting, String... names) {
        add(byName(names), setting);
        return this;
    }

    public DataFrameSettings add(ColumnSetting setting, Integer... columnIndices) {
        add(byIndex(columnIndices), setting);
        return this;
    }

    public DataFrameSettings addByValueType(ColumnSetting setting, Class<?>... types) {
        add(byValueType(types), setting);
        return this;
    }

    public DataFrameSettings addByColumnType(ColumnSetting setting, Class<? extends DataFrameColumn<?, ?>>... types) {
        add(byColumnType(types), setting);
        return this;
    }
*/
    public DataFrameSettings addColumnSettings(ColumnMatcher matcher, ColumnSetting... settings) {
        for (ColumnSetting setting : settings) {
            addColumnSetting(matcher, setting);
        }
        return this;
    }

    public DataFrameSettings addColumnSetting(ColumnMatcher matcher, ColumnSetting setting) {
        columnSettings.compute(setting, (s, matchers) -> {
            if (matchers == null) {
                matchers = new ArrayList<>();
            }
            matchers.add(matcher);
            return matchers;
        });
        return this;
    }

    public boolean remove(ColumnSetting setting) {
        return columnSettings.remove(setting) != null;
    }

    public static DataFrameSettingsBuilder create() {
        return new DataFrameSettingsBuilder();
    }

    public static final class DataFrameSettingsBuilder {
        private final Map<ColumnSetting, List<ColumnMatcher>> columnSettings = new HashMap<>();

        private DataFrameSettingsBuilder() {
        }

        public DataFrameSettingsBuilder addColumnSettings(ColumnMatcher matcher, ColumnSetting... settings) {
            for (ColumnSetting setting : settings) {
                addColumnSetting(matcher, setting);
            }
            return this;
        }

        public DataFrameSettingsBuilder addColumnSetting(ColumnMatcher matcher, ColumnSetting setting) {
            columnSettings.compute(setting, (s, matchers) -> {
                if (matchers == null) {
                    matchers = new ArrayList<>();
                }
                matchers.add(matcher);
                return matchers;
            });
            return this;
        }

        public DataFrameSettings build() {
            return new DataFrameSettings(columnSettings);
        }
    }
}
