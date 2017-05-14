package net.corda.node

import com.google.common.base.Stopwatch
import net.corda.node.driver.FalseNetworkMap
import net.corda.node.driver.driver
import org.junit.Ignore
import org.junit.Test
import java.util.concurrent.TimeUnit

@Ignore("Only use locally")
class NodeStartupPerformanceTests {

    // Measure the startup time of nodes. Note that this includes an RPC roundtrip, which causes e.g. Kryo initialisation.
    @Test
    fun `single node startup time`() {
        driver(networkMapStrategy = FalseNetworkMap) {
            startNetworkMapService().get()
            val times = ArrayList<Long>()
            for (i in 1 .. 10) {
                val time = Stopwatch.createStarted().apply {
                    startNode().get()
                }.stop().elapsed(TimeUnit.MICROSECONDS)
                times.add(time)
            }
            println(times.map { it / 1_000_000.0 })
        }
    }
}
