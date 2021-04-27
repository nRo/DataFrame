package de.unknownreality.dataframe.value.impl;

import de.unknownreality.dataframe.type.impl.CharacterType;

public class CharacterValueTypeTest extends AbstractValueTypeTest<Character, CharacterType> {
    @Override
    public CharacterType getValueType() {
        return new CharacterType();
    }
}
