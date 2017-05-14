package net.corda.kotlin.rpc

import com.typesafe.config.*
import net.corda.core.crypto.commonName
import net.corda.core.identity.Party
import net.corda.nodeapi.User

class NodeConfig(
    val party: Party,
    val p2pPort: Int,
    val rpcPort: Int,
    val webPort: Int,
    val extraServices: List<String>,
    val users: List<User>,
    var networkMap: NodeConfig? = null
) {
    companion object {
        val renderOptions: ConfigRenderOptions = ConfigRenderOptions.defaults().setOriginComments(false)
    }

    val commonName: String = party.name.commonName

    /*
     * The configuration object depends upon the networkMap,
     * which is mutable.
     */
    fun toFileConfig(): Config = ConfigFactory.empty()
            .withValue("myLegalName", valueFor(party.name.toString()))
            .withValue("p2pAddress", addressValueFor(p2pPort))
            .withValue("extraAdvertisedServiceIds", valueFor(extraServices))
            .withFallback(optional("networkMapService", networkMap, { c, n ->
                c.withValue("address", addressValueFor(n.p2pPort))
                    .withValue("legalName", valueFor(n.party.name.toString()))
            }))
            .withValue("webAddress", addressValueFor(webPort))
            .withValue("rpcAddress", addressValueFor(rpcPort))
            .withValue("rpcUsers", valueFor(users.map(User::toMap).toList()))
            .withValue("useTestClock", valueFor(true))

    fun toText(): String = toFileConfig().root().render(renderOptions)

    private fun <T> valueFor(any: T): ConfigValue? = ConfigValueFactory.fromAnyRef(any)
    private fun addressValueFor(port: Int) = valueFor("localhost:$port")
    private inline fun <T> optional(path: String, obj: T?, body: (Config, T) -> Config): Config {
        val config = ConfigFactory.empty()
        return if (obj == null) config else body(config, obj).atPath(path)
    }
}

private fun User.toMap(): Map<String, Any> = mapOf(
    "username" to username,
    "password" to password,
    "permissions" to permissions
)
