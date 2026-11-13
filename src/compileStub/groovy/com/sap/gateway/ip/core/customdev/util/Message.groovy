package com.sap.gateway.ip.core.customdev.util

interface Message {
    Object getBody()
    def <T> T getBody(Class<T> type)
    void setBody(Object exchangeBody)
    Map<String, Object> getHeaders()
    void setHeaders(Map<String, Object> exchangeHeaders)
    void setHeader(String name, Object value)
    Map<String, Object> getProperties()
    void setProperties(Map<String, Object> exchangeProperties)
    void setProperty(String name, Object value)
}
