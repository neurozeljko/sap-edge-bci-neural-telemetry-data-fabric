package com.sap.cpi.neuro

import com.sap.gateway.ip.core.customdev.util.Message
import groovy.json.JsonSlurper
import spock.lang.Specification
import spock.lang.Unroll

class EdgeNeuralDspProcessorSpec extends Specification {

    EdgeNeuralDspProcessor dspProcessor = new EdgeNeuralDspProcessor()
    Message message = Mock(Message)

    @Unroll
    def "Edge DSP & HIPAA Privacy Verification: #scenario => #expectedRisk"() {
        given: "A raw hospital BCI implant telemetry packet containing patient MRN"
        def rawInput = """
        {
            "patientMrn": "MRN-CONFIDENTIAL-7712",
            "implantId": "NEURO-ARRAY-X9-04",
            "samplingRateHz": 1000,
            "rawMicrovolts": ${samples}
        }
        """
        message.getBody(String.class) >> rawInput

        when: "SAP Edge Integration Cell executes DSP denoising and anonymization"
        dspProcessor.processData(message)

        then: "PII is stripped, pseudonymized ID is assigned, and clinical risk is classified"
        1 * message.setHeader("X-BCI-Subject-Pseudo-ID", { it.startsWith("SUBJ-") && it.length() == 21 })
        1 * message.setHeader("X-BCI-RMS-Microvolts", _ as String)
        1 * message.setHeader("X-BCI-Clinical-Risk", expectedRisk)
        1 * message.setHeader("X-HIPAA-PII-Stripped", "true")
        1 * message.setHeader("CamelHttpResponseCode", 200)
        1 * message.setBody(_ as String) >> { args ->
            assert !args[0].contains("MRN-CONFIDENTIAL-7712")
            def parsed = new JsonSlurper().parseText(args[0])
            assert parsed.neuralTelemetryFabric.edgeDspMetrics.clinicalRiskClassification == expectedRisk
        }

        where:
        scenario                     | samples                                           | expectedRisk
        "Normal resting alpha wave"  | "[18.2, 21.5, 19.8, 22.1, 20.4, 19.1, 21.0, 20.2]" | "NORMAL_BASELINE_RHYTHM"
        "High-gamma pre-ictal spike" | "[88.0, 98.2, 112.4, 105.8, 94.1, 118.6, 109.3]"  | "CRITICAL_PRE_ICTAL_ALERT"
    }

    def "Should reject empty neural telemetry payloads"() {
        given: "An empty payload"
        message.getBody(String.class) >> ""

        when: "The processor runs"
        dspProcessor.processData(message)

        then: "An IllegalArgumentException is thrown"
        thrown(IllegalArgumentException)
    }
}
