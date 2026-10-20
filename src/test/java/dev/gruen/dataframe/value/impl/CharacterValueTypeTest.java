package dev.gruen.dataframe.value.impl;

import dev.gruen.dataframe.type.impl.CharacterType;

public class CharacterValueTypeTest extends AbstractValueTypeTest<Character, CharacterType> {
    @Override
    public CharacterType getValueType() {
        return new CharacterType();
    }
}
