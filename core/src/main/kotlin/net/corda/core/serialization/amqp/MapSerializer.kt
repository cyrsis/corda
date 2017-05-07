package net.corda.core.serialization.amqp

import net.corda.core.checkNotUnorderedHashMap
import org.apache.qpid.proton.codec.Data
import java.io.NotSerializableException
import java.lang.reflect.ParameterizedType
import java.lang.reflect.Type
import java.util.*
import kotlin.collections.Map
import kotlin.collections.iterator
import kotlin.collections.map

/**
 * Serialization / deserialization of certain supported [Map] types.
 */
class MapSerializer(val declaredType: ParameterizedType) : AMQPSerializer {
    override val type: Type = declaredType as? DeserializedParameterizedType ?: DeserializedParameterizedType.make(declaredType.toString())
    private val typeName = declaredType.toString()
    override val typeDescriptor = "$DESCRIPTOR_DOMAIN:${fingerprintForType(type)}"

    companion object {
        private val supportedTypes: Map<Class<out Map<*, *>>, (Map<*, *>) -> Map<*, *>> = mapOf(
                Map::class.java to { map -> Collections.unmodifiableMap(map) },
                SortedMap::class.java to { map -> Collections.unmodifiableSortedMap(TreeMap(map)) },
                NavigableMap::class.java to { map -> Collections.unmodifiableNavigableMap(TreeMap(map)) }
        )
    }

    private val concreteBuilder: (Map<*, *>) -> Map<*, *> = findConcreteType(declaredType.rawType as Class<*>)

    private fun findConcreteType(clazz: Class<*>): (Map<*, *>) -> Map<*, *> {
        return supportedTypes[clazz] ?: throw NotSerializableException("Unsupported map type $clazz.")
    }

    private val typeNotation: TypeNotation = RestrictedType(typeName, null, emptyList(), "map", Descriptor(typeDescriptor, null), emptyList())

    override fun writeClassInfo(output: SerializationOutput) {
        if (output.writeTypeNotations(typeNotation)) {
            output.requireSerializer(declaredType.actualTypeArguments[0])
            output.requireSerializer(declaredType.actualTypeArguments[1])
        }
    }

    override fun writeObject(obj: Any, data: Data, type: Type, output: SerializationOutput) {
        obj.javaClass.checkNotUnorderedHashMap()
        // Write described
        data.withDescribed(typeNotation.descriptor) {
            // Write map
            data.putMap()
            data.enter()
            for (entry in obj as Map<*, *>) {
                output.writeObjectOrNull(entry.key, data, declaredType.actualTypeArguments[0])
                output.writeObjectOrNull(entry.value, data, declaredType.actualTypeArguments[1])
            }
            data.exit() // exit map
        }
    }

    override fun readObject(obj: Any, envelope: Envelope, input: DeserializationInput): Any {
        // TODO: General generics question. Do we need to validate that entries in Maps and Collections match the generic type?  Is it a security hole?
        val entries: Iterable<Pair<Any?, Any?>> = (obj as Map<*, *>).map { readEntry(envelope, input, it) }
        return concreteBuilder(entries.toMap())
    }

    private fun readEntry(envelope: Envelope, input: DeserializationInput, entry: Map.Entry<Any?, Any?>) = input.readObjectOrNull(entry.key, envelope, declaredType.actualTypeArguments[0]) to input.readObjectOrNull(entry.value, envelope, declaredType.actualTypeArguments[1])
}