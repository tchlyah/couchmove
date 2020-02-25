package com.github.couchmove.pojo;

import com.github.couchmove.exception.CouchmoveException;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class TypeTest {

    @Test
    public void should_get_type_from_extension() {
        for (Type type : Type.values()) {
            assertThat(Type.fromExtension(type.getExtension())).isEqualTo(type);
        }
    }

    @Test
    public void should_throw_exception_when_unknown_extension() {
        assertThrows(CouchmoveException.class, () -> Type.fromExtension("toto"));
    }
}
