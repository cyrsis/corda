package net.corda.core.serialization.amqp

import com.google.common.primitives.Primitives
import net.corda.core.checkNotUnorderedHashMap
import net.corda.core.serialization.AllWhitelist
import net.corda.core.serialization.ClassWhitelist
import net.corda.core.serialization.CordaSerializable
import org.apache.qpid.proton.amqp.*
import java.io.NotSerializableException
import java.lang.reflect.GenericArrayType
import java.lang.reflect.ParameterizedType
import java.lang.reflect.Type
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import javax.annotation.concurrent.ThreadSafe

/**
 * Factory of serializers designed to be shared across threads and invocations.
 */
// TODO: object references
// TODO: class references? (e.g. cheat with repeated descriptors using a long encoding, like object ref proposal)
// TODO: Inner classes etc
// TODO: support for custom serialisation of core types (of e.g. PublicKey, Throwables)
// TODO: exclude schemas for core types that don't need custom serializers that everyone already knows the schema for.
// TODO: support for intern-ing of deserialized objects for some core types (e.g. PublicKey) for memory efficiency
// TODO: maybe support for caching of serialized form of some core types for performance
// TODO: profile for performance in general
// TODO: use guava caches etc so not unbounded
// TODO: do we need to support a transient annotation to exclude certain properties?
// TODO: incorporate the class carpenter for classes not on the classpath.
// TODO: apply class loader logic and an "app context" throughout this code.
// TODO: schema evolution solution when the fingerprints do not line up.
@ThreadSafe
class SerializerFactory(val whitelist: ClassWhitelist = AllWhitelist) {
    private val serializersByType = ConcurrentHashMap<Type, AMQPSerializer>()
    private val serializersByDescriptor = ConcurrentHashMap<Any, AMQPSerializer>()

    /**
     * Look up, and manufacture if necessary, a serializer for the given type.
     *
     * @param actualType Will be null if there isn't an actual object instance available (e.g. for
     * restricted type processing).
     */
    @Throws(NotSerializableException::class)
    fun get(actualType: Class<*>?, declaredType: Type): AMQPSerializer {
        if (declaredType is ParameterizedType) {
            return serializersByType.computeIfAbsent(declaredType) {
                // We allow only Collection and Map.
                val rawType = declaredType.rawType
                if (rawType is Class<*>) {
                    checkParameterisedTypesConcrete(declaredType.actualTypeArguments)
                    if (Collection::class.java.isAssignableFrom(rawType)) {
                        CollectionSerializer(declaredType)
                    } else if (Map::class.java.isAssignableFrom(rawType)) {
                        makeMapSerializer(declaredType)
                    } else {
                        throw NotSerializableException("Declared types of $declaredType are not supported.")
                    }
                } else {
                    throw NotSerializableException("Declared types of $declaredType are not supported.")
                }
            }
        } else if (declaredType is Class<*>) {
            // Simple classes allowed
            if (Collection::class.java.isAssignableFrom(declaredType)) {
                return serializersByType.computeIfAbsent(declaredType) { CollectionSerializer(DeserializedParameterizedType(declaredType, arrayOf(AnyType), null)) }
            } else if (Map::class.java.isAssignableFrom(declaredType)) {
                return serializersByType.computeIfAbsent(declaredType) { makeMapSerializer(DeserializedParameterizedType(declaredType, arrayOf(AnyType, AnyType), null)) }
            } else {
                return makeClassSerializer(actualType ?: declaredType)
            }
        } else if (declaredType is GenericArrayType) {
            return serializersByType.computeIfAbsent(declaredType) { ArraySerializer(declaredType) }
        } else {
            throw NotSerializableException("Declared types of $declaredType are not supported.")
        }
    }

    @Throws(NotSerializableException::class)
    fun get(typeDescriptor: Any, envelope: Envelope): AMQPSerializer {
        return serializersByDescriptor[typeDescriptor] ?: {
            processSchema(envelope.schema)
            serializersByDescriptor[typeDescriptor] ?: throw NotSerializableException("Could not find type matching descriptor $typeDescriptor.")
        }()
    }

    private fun processSchema(schema: Schema) {
        for (typeNotation in schema.types) {
            processSchemaEntry(typeNotation)
        }
    }

    private fun processSchemaEntry(typeNotation: TypeNotation) {
        when (typeNotation) {
            is CompositeType -> processCompositeType(typeNotation) // java.lang.Class (whether a class or interface)
            is RestrictedType -> processRestrictedType(typeNotation) // Collection / Map, possibly with generics
        }
    }

    private fun restrictedTypeForName(name: String): Type {
        return if (name.endsWith("[]")) {
            DeserializedGenericArrayType(restrictedTypeForName(name.substring(0, name.lastIndex - 1)))
        } else {
            DeserializedParameterizedType.make(name)
        }
    }

    private fun processRestrictedType(typeNotation: RestrictedType) {
        serializersByDescriptor.computeIfAbsent(typeNotation.descriptor.name!!) {
            // TODO: class loader logic, and compare the schema.
            val type = restrictedTypeForName(typeNotation.name)
            get(null, type)
        }
    }

    private fun processCompositeType(typeNotation: CompositeType) {
        serializersByDescriptor.computeIfAbsent(typeNotation.descriptor.name!!) {
            // TODO: class loader logic, and compare the schema.
            val clazz = Class.forName(typeNotation.name)
            get(clazz, clazz)
        }
    }

    private fun checkParameterisedTypesConcrete(actualTypeArguments: Array<out Type>) {
        for (type in actualTypeArguments) {
            // Needs to be another parameterised type or a class, or any type.
            if (type !is Class<*>) {
                if (type is ParameterizedType) {
                    checkParameterisedTypesConcrete(type.actualTypeArguments)
                } else if (type != AnyType) {
                    throw NotSerializableException("Declared parameterised types containing $type as a parameter are not supported.")
                }
            }
        }
    }

    private fun makeClassSerializer(clazz: Class<*>): AMQPSerializer {
        return serializersByType.computeIfAbsent(clazz) {
            if (clazz.isArray) {
                whitelisted(clazz.componentType)
                ArraySerializer(clazz)
            } else if (isPrimitive(clazz)) {
                AMQPPrimitiveSerializer(clazz)
            } else {
                whitelisted(clazz)
                ObjectSerializer(clazz)
            }
        }
    }

    private fun whitelisted(clazz: Class<*>): Boolean {
        if (whitelist.hasListed(clazz) || clazz.isAnnotationPresent(CordaSerializable::class.java)) {
            return true
        } else {
            throw NotSerializableException("Class $clazz is not on the whitelist or annotated with @CordaSerializable.")
        }
    }

    private fun makeMapSerializer(declaredType: ParameterizedType): AMQPSerializer {
        val rawType = declaredType.rawType as Class<*>
        rawType.checkNotUnorderedHashMap()
        return MapSerializer(declaredType)
    }

    companion object {
        fun isPrimitive(type: Type): Boolean = type is Class<*> && Primitives.wrap(type) in primitiveTypeNames

        fun primitiveTypeName(type: Type): String? = primitiveTypeNames[type as? Class<*>]

        private val primitiveTypeNames: Map<Class<*>, String> = mapOf(
                Boolean::class.java to "boolean",
                Byte::class.java to "byte",
                UnsignedByte::class.java to "ubyte",
                Short::class.java to "short",
                UnsignedShort::class.java to "ushort",
                Integer::class.java to "int",
                UnsignedInteger::class.java to "uint",
                Long::class.java to "long",
                UnsignedLong::class.java to "ulong",
                Float::class.java to "float",
                Double::class.java to "double",
                Decimal32::class.java to "decimal32",
                Decimal64::class.java to "decimal62",
                Decimal128::class.java to "decimal128",
                Char::class.java to "char",
                Date::class.java to "timestamp",
                UUID::class.java to "uuid",
                ByteArray::class.java to "binary",
                String::class.java to "string",
                Symbol::class.java to "symbol")
    }

    object AnyType : Type {
        override fun toString(): String = "*"
    }
}
