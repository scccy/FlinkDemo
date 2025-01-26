package base;

import com.alibaba.fastjson2.JSON;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;

public class Fastjson2Deserializer<T> implements DeserializationSchema<T> {
    private final Class<T> targetClass;

    public Fastjson2Deserializer(Class<T> targetClass) {
        this.targetClass = targetClass;
    }

    @Override
    public T deserialize(byte[] message) throws IOException {
        // 使用 Fastjson2 将字节数组反序列化为目标类型
        return JSON.parseObject(message, targetClass);
    }

    @Override
    public boolean isEndOfStream(T nextElement) {
        // 如果消息是流结束的标志，返回 true；否则返回 false
        return false;
    }

    @Override
    public TypeInformation<T> getProducedType() {
        // 返回反序列化后的数据类型
        return TypeInformation.of(targetClass);
    }
}