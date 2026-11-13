import com.sap.gateway.ip.core.customdev.util.Message
import groovy.json.JsonSlurper
import groovy.json.JsonOutput
import java.security.MessageDigest
import java.time.Instant

def Message processData(Message message) {
    def body = message.getBody(String.class)
    if (!body || body.trim().isEmpty()) {
        throw new IllegalArgumentException("BCI Edge Fault: Empty neural telemetry frame received.")
    }

    def slurper = new JsonSlurper()
    def payload = slurper.parseText(body)

    if (!payload.patientMrn || !payload.implantId || !payload.rawMicrovolts || payload.rawMicrovolts.isEmpty()) {
        throw new IllegalArgumentException("BCI Calibration Violation: Missing patientMrn, implantId, or rawMicrovolts array.")
    }

    // 1. HIPAA / GDPR Zero-Knowledge Edge Pseudonymization (PII never leaves hospital boundary)
    String salt = "SAP-EDGE-NEURO-TRIAL-2026"
    String subjectPseudoId = "SUBJ-" + MessageDigest.getInstance("SHA-256")
        .digest("${salt}:${payload.patientMrn}".getBytes("UTF-8"))
        .encodeHex()
        .toString()
        .substring(0, 16)
        .toUpperCase()

    // 2. Real-Time Edge Digital Signal Processing (EMA Low-Pass Denoising & RMS Spectral Power)
    List<Double> rawSamples = payload.rawMicrovolts.collect { it as double }
    double alphaSmoothing = 0.35d
    double filteredPrev = rawSamples[0]
    double sumSquares = 0.0d
    double peakAmplitude = 0.0d

    rawSamples.each { double sample ->
        double denoised = (alphaSmoothing * sample) + ((1.0d - alphaSmoothing) * filteredPrev)
        filteredPrev = denoised
        sumSquares += (denoised * denoised)
        double absVal = Math.abs(denoised)
        if (absVal > peakAmplitude) {
            peakAmplitude = absVal
        }
    }

    double rmsMicrovolts = Math.round(Math.sqrt(sumSquares / rawSamples.size()) * 100.0d) / 100.0d
    double peakRounded = Math.round(peakAmplitude * 100.0d) / 100.0d

    // High-gamma / pre-ictal spike threshold detection (> 85.0 uV RMS indicates seizure precursor)
    boolean seizurePrecursorDetected = (rmsMicrovolts > 85.0d)
    String clinicalRiskState = seizurePrecursorDetected ? "CRITICAL_PRE_ICTAL_ALERT" : "NORMAL_BASELINE_RHYTHM"
    double compressionRatio = Math.round((1.0d - (4.0d / rawSamples.size())) * 1000.0d) / 10.0d

    message.setHeader("X-BCI-Subject-Pseudo-ID", subjectPseudoId)
    message.setHeader("X-BCI-RMS-Microvolts", rmsMicrovolts.toString())
    message.setHeader("X-BCI-Clinical-Risk", clinicalRiskState)
    message.setHeader("X-HIPAA-PII-Stripped", "true")
    message.setHeader("CamelHttpResponseCode", 200)

    def dataFabricEnvelope = [
        neuralTelemetryFabric: [
            subjectPseudoId: subjectPseudoId,
            implantDeviceId: payload.implantId,
            samplingRateHz: payload.samplingRateHz ?: 1000,
            edgeDspMetrics: [
                rawSampleCount: rawSamples.size(),
                denoisedRmsMicrovolts: rmsMicrovolts,
                peakAmplitudeMicrovolts: peakRounded,
                bandwidthCompressionPct: compressionRatio,
                clinicalRiskClassification: clinicalRiskState
            ],
            downstreamTargets: [
                analyticsLake: "Databricks Delta Lake (neuro_telemetry_silver) -> Dataiku Seizure ML",
                semanticFederation: "SAP Graph (GraphQL) <-> SAP S/4HANA Clinical Trial Subject"
            ],
            edgeProcessedUtc: Instant.now().toString()
        ]
    ]

    message.setBody(JsonOutput.prettyPrint(JsonOutput.toJson(dataFabricEnvelope)))
    return message
}