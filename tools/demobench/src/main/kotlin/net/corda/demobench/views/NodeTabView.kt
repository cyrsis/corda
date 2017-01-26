package net.corda.demobench.views

import java.text.DecimalFormat
import javafx.util.converter.NumberStringConverter
import net.corda.demobench.model.NodeController
import net.corda.demobench.model.NodeDataModel
import net.corda.demobench.ui.CloseableTab
import tornadofx.*

class NodeTabView : Fragment() {
    override val root = stackpane {}

    private val INTEGER_FORMAT = DecimalFormat()
    private val NOT_NUMBER = Regex("[^\\d]")

    private val model = NodeDataModel()
    private val controller by inject<NodeController>()

    private val nodeTerminalView = find<NodeTerminalView>()
    private val nodeConfigView = pane {
        form {
            fieldset("Configuration") {
                field("Node Name") {
                    textfield(model.legalName) {
                        minWidth = 200.0
                        maxWidth = 200.0
                        validator {
                            if (it.isNullOrBlank()) {
                                error("Node name is required")
                            } else {
                                null
                            }
                        }
                    }
                }
                field("P2P Port") {
                    textfield(model.p2pPort, NumberStringConverter(INTEGER_FORMAT)) {
                        minWidth = 100.0
                        maxWidth = 100.0
                        validator {
                            if ((it == null) || it.isEmpty()) {
                                error("Port number required")
                            } else if (it.contains(NOT_NUMBER)) {
                                error("Invalid port number")
                            } else {
                                null
                            }
                        }
                    }
                }
                field("Artemis Port") {
                    textfield(model.artemisPort, NumberStringConverter(INTEGER_FORMAT)) {
                        minWidth = 100.0
                        maxWidth = 100.0
                        validator {
                            if ((it == null) || it.isEmpty()) {
                                error("Port number required")
                            } else if (it.contains(NOT_NUMBER)) {
                                error("Invalid port number")
                            } else {
                                null
                            }
                        }
                    }
                }
                field("Web Port") {
                    textfield(model.webPort, NumberStringConverter(INTEGER_FORMAT)) {
                        minWidth = 100.0
                        maxWidth = 100.0
                        validator {
                            if ((it == null) || it.isEmpty()) {
                                error("Port number required")
                            } else if (it.contains(NOT_NUMBER)) {
                                error("Invalid port number")
                            } else {
                                null
                            }
                        }
                    }
                }
            }

            fieldset("Plugins") {

            }

            button("Create Node") {
                setOnAction() {
                    launch()
                }
            }
        }
    }

    val nodeTab = CloseableTab("New Node", root)

    fun launch() {
        model.commit()
        val config = controller.validate(model.item)
        if (config != null) {
            nodeConfigView.isVisible = false
            nodeTab.text = config.name
            nodeTerminalView.open(config)
        }
    }

    init {
        INTEGER_FORMAT.isGroupingUsed = false

        // Ensure that we close the terminal along with the tab.
        nodeTab.setOnCloseRequest {
            nodeTerminalView.close()
        }

        root.add(nodeConfigView)
        root.add(nodeTerminalView)

        model.p2pPort.value = controller.nextPort
        model.artemisPort.value = controller.nextPort
        model.webPort.value = controller.nextPort
    }
}