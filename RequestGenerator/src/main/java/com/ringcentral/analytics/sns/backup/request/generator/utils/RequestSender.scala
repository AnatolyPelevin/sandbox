package com.ringcentral.analytics.sns.backup.request.generator.utils

import com.ringcentral.analytics.sns.backup.request.generator.model.Message
import com.ringcentral.analytics.sns.backup.request.generator.GeneratorOptions
import org.apache.http.auth.UsernamePasswordCredentials
import org.apache.http.client.config.RequestConfig
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.StringEntity
import org.apache.http.impl.auth.BasicScheme
import org.apache.http.impl.client.HttpClientBuilder

class RequestSender(options: GeneratorOptions) {

    private val path = options.snsControllerPath
    private val url = "http://" + options.snsHost + ":" + options.snsPort + path
    private val user = options.snsUserName
    private val password = options.snsUserPassword
    private val connectionTimeout = options.connectionTimeout

    def send(message: Message): Int = {
        val client = HttpClientBuilder
            .create
            .setDefaultRequestConfig(RequestConfig.custom.setConnectTimeout(connectionTimeout).build)
            .build

        val httpPost = new HttpPost(url)

        httpPost.setHeader(new BasicScheme().authenticate(new UsernamePasswordCredentials(user, password), httpPost, null))
        httpPost.setHeader("Accept", "application/json")
        httpPost.setHeader("Content-type", "application/json")
        httpPost.setEntity(new StringEntity(message.toString))

        val response = client.execute(httpPost)
        client.close()
        response.getStatusLine.getStatusCode
    }
}
