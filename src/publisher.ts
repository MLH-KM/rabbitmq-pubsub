import * as amqp from "amqplib";
import { IRabbitMqConnectionFactory } from "./connectionFactory";
import * as Logger from "bunyan";
import * as Promise from "bluebird";
import { IQueueNameConfig, asPubSubQueueNameConfig } from "./common";
import { createChildLogger } from "./childLogger";

export class RabbitMqPublisher {
    channelMap = {};

    constructor(private logger: Logger, private connectionFactory: IRabbitMqConnectionFactory) {
        this.logger = createChildLogger(logger, "RabbitMqPublisher");
    }

    publish<T>(queue: string | IQueueNameConfig, message: T): Promise<void> {
        const queueConfig = asPubSubQueueNameConfig(queue);
        const settings = this.getSettings();
        this.logger.debug(queueConfig.name);        
        if (!this.channelMap[queueConfig.name]) {
            this.channelMap[queueConfig.name] =
                this.connectionFactory.create().then(connection => {
                    return connection.createChannel().then(channel => {
                        return this.setupChannel<T>(channel, queueConfig).then(() => channel)
                    });
                })
        }
        return this.channelMap[queueConfig.name].then((channel) => {
            return Promise.resolve(channel.publish(queueConfig.dlx, '', this.getMessageBuffer(message))).then(() => {
                this.logger.trace("message sent to exchange '%s' (%j)", queueConfig.dlx, message)
            });
        }).catch(() => {
                this.logger.error("unable to send message to exchange '%j' {%j}", queueConfig.dlx, message)
            return Promise.reject(new Error("Unable to send message"))
        })
    }

    private setupChannel<T>(channel: amqp.Channel, queueConfig: IQueueNameConfig) {
        this.logger.trace("setup '%j'", queueConfig);
        return Promise.all(this.getChannelSetup(channel, queueConfig));
    }

    protected getMessageBuffer<T>(message: T) {
        return new Buffer(JSON.stringify(message), 'utf8');
    }

    protected getChannelSetup(channel: amqp.Channel, queueConfig: IQueueNameConfig) {
        return [
            channel.assertExchange(queueConfig.dlx, 'fanout', this.getSettings()),
        ]
    }

    protected getSettings(): amqp.Options.AssertQueue {
        return {
            durable: false,
        }
    }
}


