package com.ringcentral.analytics.sns.request.generator.utils

import com.ringcentral.analytics.sns.request.generator.model.Message
import com.ringcentral.analytics.sns.request.generator.GeneratorOptions
import org.apache.http.auth.UsernamePasswordCredentials
import org.apache.http.client.config.RequestConfig
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.{ContentType, StringEntity}
import org.apache.http.impl.auth.BasicScheme
import org.apache.http.impl.client.HttpClientBuilder
import org.apache.http.HttpResponse

class RequestSender(options: GeneratorOptions) {

    private val path = options.snsControllerPath
    private val url = "http://" + options.snsHost + ":" + options.snsPort + path
    private val user = options.snsUserName
    private val password = options.snsUserPassword
    private val connectionTimeout = options.connectionTimeout

    def send(message: Message): HttpResponse = {
        val client = HttpClientBuilder
            .create
            .setDefaultRequestConfig(RequestConfig.custom.setConnectTimeout(connectionTimeout).build)
            .build

        val httpPost = new HttpPost(url)

        httpPost.setHeader(new BasicScheme().authenticate(new UsernamePasswordCredentials(user, password), httpPost, null))
        httpPost.setHeader("Accept", ContentType.APPLICATION_JSON.toString)
        httpPost.setHeader("Content-type", ContentType.APPLICATION_JSON.toString)
        httpPost.setEntity(new StringEntity(message.toString))

        val response = client.execute(httpPost)
        client.close()
        response
    }
}
