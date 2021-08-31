package com.example.demo

import com.jcraft.jsch.ChannelSftp
import org.springframework.beans.factory.BeanFactory
import org.springframework.beans.factory.config.ConfigurableBeanFactory
import org.springframework.integration.dsl.IntegrationFlow
import org.springframework.integration.dsl.IntegrationFlows
import org.springframework.integration.dsl.MessageChannels
import org.springframework.integration.dsl.Pollers
import org.springframework.integration.dsl.SourcePollingChannelAdapterSpec
import org.springframework.integration.dsl.context.IntegrationFlowContext
import org.springframework.integration.file.filters.AcceptAllFileListFilter
import org.springframework.integration.file.remote.session.CachingSessionFactory
import org.springframework.integration.file.remote.session.SessionFactory
import org.springframework.integration.sftp.dsl.Sftp
import org.springframework.integration.sftp.outbound.SftpMessageHandler
import org.springframework.integration.sftp.session.DefaultSftpSessionFactory
import org.springframework.integration.sftp.session.SftpRemoteFileTemplate
import org.springframework.integration.transformer.StreamTransformer
import org.springframework.messaging.Message
import org.springframework.messaging.MessageHandler
import org.springframework.stereotype.Component
import java.util.concurrent.atomic.AtomicInteger
import javax.annotation.PostConstruct


@Component
class FlowRegister(val flowContext: IntegrationFlowContext) {

    @PostConstruct
    fun register() {
        val inbound = sftpInboundFlow()!!
        val outbound = sftpOutboundFlow()!!

        val first = flowContext.registration(outbound).autoStartup(true).register()
        val second = flowContext.registration(inbound).autoStartup(true).register()
        //r.destroy()
    }

    fun myFlow(): IntegrationFlow? {
        return IntegrationFlows.fromSupplier(
            { AtomicInteger().getAndIncrement() }
        ) { c: SourcePollingChannelAdapterSpec ->
            c.poller(
                Pollers.fixedRate(600)
            )
        }
            .channel("inputChannel")
            .filter { p: Int -> p > 0 }
            .transform { obj: Any -> obj.toString() }
            .handle { it:String -> println(it) }
            .channel(MessageChannels.queue())
            .get()
    }

    fun sftpSessionFactory(): SessionFactory<ChannelSftp.LsEntry?>? {
        val factory = DefaultSftpSessionFactory(true)
        factory.setHost("localhost")
        factory.setUser("dkovalskyi")
        factory.setPassword("")
        factory.setAllowUnknownKeys(true)
        return CachingSessionFactory(factory)
    }

    fun template(): SftpRemoteFileTemplate? {
        return SftpRemoteFileTemplate(sftpSessionFactory())
    }

    fun sftpInboundFlow(): IntegrationFlow? {
        return IntegrationFlows
            .from(
                Sftp.inboundStreamingAdapter(template())
                    .filter(AcceptAllFileListFilter())
                    .remoteDirectory("test")
                    .regexFilter(".*\\.txt$")
            ) { e ->
                e.id("sftpInboundAdapter")
                    .autoStartup(true)
                    .poller(Pollers.fixedDelay(500))
            }
            .transform(StreamTransformer("UTF-8"))
            //.handle(MessageHandler { m: Message<*> -> println(m) })
            .channel("testChannel")
            .get()
    }

    fun sftpOutboundFlow(): IntegrationFlow? {
        return IntegrationFlows
            .from("testChannel", true)
            .handle(MessageHandler { m: Message<*> -> println(m) })
            .get()
    }
}