module.exports =
/******/ (function(modules) { // webpackBootstrap
/******/ 	// The module cache
/******/ 	var installedModules = {};
/******/
/******/ 	// The require function
/******/ 	function __webpack_require__(moduleId) {
/******/
/******/ 		// Check if module is in cache
/******/ 		if(installedModules[moduleId]) {
/******/ 			return installedModules[moduleId].exports;
/******/ 		}
/******/ 		// Create a new module (and put it into the cache)
/******/ 		var module = installedModules[moduleId] = {
/******/ 			i: moduleId,
/******/ 			l: false,
/******/ 			exports: {}
/******/ 		};
/******/
/******/ 		// Execute the module function
/******/ 		modules[moduleId].call(module.exports, module, module.exports, __webpack_require__);
/******/
/******/ 		// Flag the module as loaded
/******/ 		module.l = true;
/******/
/******/ 		// Return the exports of the module
/******/ 		return module.exports;
/******/ 	}
/******/
/******/
/******/ 	// expose the modules object (__webpack_modules__)
/******/ 	__webpack_require__.m = modules;
/******/
/******/ 	// expose the module cache
/******/ 	__webpack_require__.c = installedModules;
/******/
/******/ 	// identity function for calling harmony imports with the correct context
/******/ 	__webpack_require__.i = function(value) { return value; };
/******/
/******/ 	// define getter function for harmony exports
/******/ 	__webpack_require__.d = function(exports, name, getter) {
/******/ 		if(!__webpack_require__.o(exports, name)) {
/******/ 			Object.defineProperty(exports, name, {
/******/ 				configurable: false,
/******/ 				enumerable: true,
/******/ 				get: getter
/******/ 			});
/******/ 		}
/******/ 	};
/******/
/******/ 	// getDefaultExport function for compatibility with non-harmony modules
/******/ 	__webpack_require__.n = function(module) {
/******/ 		var getter = module && module.__esModule ?
/******/ 			function getDefault() { return module['default']; } :
/******/ 			function getModuleExports() { return module; };
/******/ 		__webpack_require__.d(getter, 'a', getter);
/******/ 		return getter;
/******/ 	};
/******/
/******/ 	// Object.prototype.hasOwnProperty.call
/******/ 	__webpack_require__.o = function(object, property) { return Object.prototype.hasOwnProperty.call(object, property); };
/******/
/******/ 	// __webpack_public_path__
/******/ 	__webpack_require__.p = "";
/******/
/******/ 	// Load entry module and return exports
/******/ 	return __webpack_require__(__webpack_require__.s = 8);
/******/ })
/************************************************************************/
/******/ ([
/* 0 */
/***/ (function(module, exports, __webpack_require__) {

"use strict";

Object.defineProperty(exports, "__esModule", { value: true });
function createChildLogger(logger, className) {
    return logger.child({ child: "rabbitmq-pub-sub", "class": className }, true);
}
exports.createChildLogger = createChildLogger;


/***/ }),
/* 1 */
/***/ (function(module, exports) {

module.exports = require("bluebird");

/***/ }),
/* 2 */
/***/ (function(module, exports, __webpack_require__) {

"use strict";

Object.defineProperty(exports, "__esModule", { value: true });
var DefaultQueueNameConfig = /** @class */ (function () {
    function DefaultQueueNameConfig(name) {
        this.name = name;
        this.dlq = name + ".DLQ";
        this.dlx = this.dlq + ".Exchange";
    }
    return DefaultQueueNameConfig;
}());
exports.DefaultQueueNameConfig = DefaultQueueNameConfig;
var DefaultPubSubQueueConfig = /** @class */ (function () {
    function DefaultPubSubQueueConfig(name) {
        this.name = name;
        this.dlq = '';
        this.dlx = name + ".DLQ.Exchange";
    }
    return DefaultPubSubQueueConfig;
}());
exports.DefaultPubSubQueueConfig = DefaultPubSubQueueConfig;
function asQueueNameConfig(config) {
    return isQueueNameConfig(config) ? config : new DefaultQueueNameConfig(config);
}
exports.asQueueNameConfig = asQueueNameConfig;
function asPubSubQueueNameConfig(config) {
    return isQueueNameConfig(config) ? config : new DefaultPubSubQueueConfig(config);
}
exports.asPubSubQueueNameConfig = asPubSubQueueNameConfig;
function isQueueNameConfig(config) {
    if (config.name && config.dlq && config.dlx) {
        return true;
    }
}


/***/ }),
/* 3 */
/***/ (function(module, exports, __webpack_require__) {

"use strict";

Object.defineProperty(exports, "__esModule", { value: true });
var amqp = __webpack_require__(9);
var Promise = __webpack_require__(1);
var childLogger_1 = __webpack_require__(0);
function isConnectionConfig(config) {
    if (config.host && config.port) {
        return true;
    }
}
var RabbitMqConnectionFactory = /** @class */ (function () {
    function RabbitMqConnectionFactory(logger, config) {
        this.logger = logger;
        this.connection = isConnectionConfig(config) ? "amqp://" + config.host + ":" + config.port : config;
        this.logger = childLogger_1.createChildLogger(logger, "RabbitMqConnectionFactory");
    }
    RabbitMqConnectionFactory.prototype.create = function () {
        var _this = this;
        this.logger.debug("connecting to %s", this.connection);
        return Promise.resolve(amqp.connect(this.connection)).catch(function (err) {
            _this.logger.error("failed to create connection '%s'", _this.connection);
            return Promise.reject(err);
        });
    };
    return RabbitMqConnectionFactory;
}());
exports.RabbitMqConnectionFactory = RabbitMqConnectionFactory;
var RabbitMqSingletonConnectionFactory = /** @class */ (function () {
    function RabbitMqSingletonConnectionFactory(logger, config) {
        this.logger = logger;
        this.connection = isConnectionConfig(config) ? "amqp://" + config.host + ":" + config.port : config;
    }
    RabbitMqSingletonConnectionFactory.prototype.create = function () {
        if (this.promise) {
            this.logger.trace("reusing connection to %s", this.connection);
            return this.promise;
        }
        this.logger.debug("creating connection to %s", this.connection);
        return this.promise = Promise.resolve(amqp.connect(this.connection));
    };
    return RabbitMqSingletonConnectionFactory;
}());
exports.RabbitMqSingletonConnectionFactory = RabbitMqSingletonConnectionFactory;


/***/ }),
/* 4 */
/***/ (function(module, exports, __webpack_require__) {

"use strict";

Object.defineProperty(exports, "__esModule", { value: true });
var Promise = __webpack_require__(1);
var common_1 = __webpack_require__(2);
var childLogger_1 = __webpack_require__(0);
var RabbitMqConsumer = /** @class */ (function () {
    function RabbitMqConsumer(logger, connectionFactory) {
        this.logger = logger;
        this.connectionFactory = connectionFactory;
        this.logger = childLogger_1.createChildLogger(logger, "RabbitMqConsumer");
    }
    RabbitMqConsumer.prototype.subscribe = function (queue, action) {
        var _this = this;
        var queueConfig = common_1.asQueueNameConfig(queue);
        return this.connectionFactory.create()
            .then(function (connection) { return connection.createChannel(); })
            .then(function (channel) {
            _this.logger.trace("got channel for queue '%s'", queueConfig.name);
            return _this.setupChannel(channel, queueConfig)
                .then(function () { return _this.subscribeToChannel(channel, queueConfig, action); });
        });
    };
    RabbitMqConsumer.prototype.setupChannel = function (channel, queueConfig) {
        this.logger.trace("setup '%j'", queueConfig);
        return Promise.all(this.getChannelSetup(channel, queueConfig));
    };
    RabbitMqConsumer.prototype.subscribeToChannel = function (channel, queueConfig, action) {
        var _this = this;
        this.logger.trace("subscribing to queue '%s'", queueConfig.name);
        return channel.consume(queueConfig.name, function (message) {
            var msg;
            Promise.try(function () {
                msg = _this.getMessageObject(message);
                _this.logger.trace("message arrived from queue '%s' (%j)", queueConfig.name, msg);
                return action(msg);
            }).then(function () {
                _this.logger.trace("message processed from queue '%s' (%j)", queueConfig.name, msg);
                channel.ack(message);
            }).catch(function (err) {
                _this.logger.error(err, "message processing failed from queue '%j' (%j)", queueConfig, msg);
                channel.nack(message, false, false);
            });
        }).then(function (opts) {
            _this.logger.trace("subscribed to queue '%s' (%s)", queueConfig.name, opts.consumerTag);
            return (function () {
                _this.logger.trace("disposing subscriber to queue '%s' (%s)", queueConfig.name, opts.consumerTag);
                return Promise.resolve(channel.cancel(opts.consumerTag)).return();
            });
        });
    };
    RabbitMqConsumer.prototype.getMessageObject = function (message) {
        return JSON.parse(message.content.toString('utf8'));
    };
    RabbitMqConsumer.prototype.getChannelSetup = function (channel, queueConfig) {
        return [
            channel.assertQueue(queueConfig.name, this.getQueueSettings(queueConfig.dlx)),
            channel.assertQueue(queueConfig.dlq, this.getDLSettings()),
            channel.assertExchange(queueConfig.dlx, 'fanout', this.getDLSettings()),
            channel.bindQueue(queueConfig.dlq, queueConfig.dlx, '*')
        ];
    };
    RabbitMqConsumer.prototype.getQueueSettings = function (deadletterExchangeName) {
        var settings = this.getDLSettings();
        settings.arguments = {
            'x-dead-letter-exchange': deadletterExchangeName
        };
        return settings;
    };
    RabbitMqConsumer.prototype.getDLSettings = function () {
        return {
            durable: true,
            autoDelete: false
        };
    };
    return RabbitMqConsumer;
}());
exports.RabbitMqConsumer = RabbitMqConsumer;


/***/ }),
/* 5 */
/***/ (function(module, exports, __webpack_require__) {

"use strict";

Object.defineProperty(exports, "__esModule", { value: true });
var Promise = __webpack_require__(1);
var common_1 = __webpack_require__(2);
var childLogger_1 = __webpack_require__(0);
var RabbitMqProducer = /** @class */ (function () {
    function RabbitMqProducer(logger, connectionFactory) {
        this.logger = logger;
        this.connectionFactory = connectionFactory;
        this.logger = childLogger_1.createChildLogger(logger, "RabbitMqProducer");
    }
    RabbitMqProducer.prototype.publish = function (queue, message) {
        var _this = this;
        var queueConfig = common_1.asQueueNameConfig(queue);
        var settings = this.getQueueSettings(queueConfig.dlx);
        return this.connectionFactory.create()
            .then(function (connection) { return connection.createChannel(); })
            .then(function (channel) {
            return Promise.resolve(channel.assertQueue(queueConfig.name, settings)).then(function () {
                if (!channel.sendToQueue(queueConfig.name, _this.getMessageBuffer(message), { persistent: true })) {
                    _this.logger.error("unable to send message to queue '%j' {%j}", queueConfig, message);
                    return Promise.reject(new Error("Unable to send message"));
                }
                _this.logger.trace("message sent to queue '%s' (%j)", queueConfig.name, message);
            });
        });
    };
    RabbitMqProducer.prototype.getMessageBuffer = function (message) {
        return new Buffer(JSON.stringify(message), 'utf8');
    };
    RabbitMqProducer.prototype.getQueueSettings = function (deadletterExchangeName) {
        return {
            durable: true,
            autoDelete: false,
            arguments: {
                'x-dead-letter-exchange': deadletterExchangeName
            }
        };
    };
    return RabbitMqProducer;
}());
exports.RabbitMqProducer = RabbitMqProducer;


/***/ }),
/* 6 */
/***/ (function(module, exports, __webpack_require__) {

"use strict";

Object.defineProperty(exports, "__esModule", { value: true });
var Promise = __webpack_require__(1);
var common_1 = __webpack_require__(2);
var childLogger_1 = __webpack_require__(0);
var RabbitMqPublisher = /** @class */ (function () {
    function RabbitMqPublisher(logger, connectionFactory) {
        this.logger = logger;
        this.connectionFactory = connectionFactory;
        this.channelMap = {};
        this.logger = childLogger_1.createChildLogger(logger, "RabbitMqPublisher");
    }
    RabbitMqPublisher.prototype.publish = function (queue, message) {
        var _this = this;
        var queueConfig = common_1.asPubSubQueueNameConfig(queue);
        var settings = this.getSettings();
        this.logger.debug(queueConfig.name);
        if (!this.channelMap[queueConfig.name]) {
            this.channelMap[queueConfig.name] =
                this.connectionFactory.create().then(function (connection) {
                    return connection.createChannel().then(function (channel) {
                        return _this.setupChannel(channel, queueConfig).then(function () { return channel; });
                    });
                });
        }
        return this.channelMap[queueConfig.name].then(function (channel) {
            return Promise.resolve(channel.publish(queueConfig.dlx, '', _this.getMessageBuffer(message))).then(function () {
                _this.logger.trace("message sent to exchange '%s' (%j)", queueConfig.dlx, message);
            });
        }).catch(function () {
            _this.logger.error("unable to send message to exchange '%j' {%j}", queueConfig.dlx, message);
            return Promise.reject(new Error("Unable to send message"));
        });
    };
    RabbitMqPublisher.prototype.setupChannel = function (channel, queueConfig) {
        this.logger.trace("setup '%j'", queueConfig);
        return Promise.all(this.getChannelSetup(channel, queueConfig));
    };
    RabbitMqPublisher.prototype.getMessageBuffer = function (message) {
        return new Buffer(JSON.stringify(message), 'utf8');
    };
    RabbitMqPublisher.prototype.getChannelSetup = function (channel, queueConfig) {
        return [
            channel.assertExchange(queueConfig.dlx, 'fanout', this.getSettings()),
        ];
    };
    RabbitMqPublisher.prototype.getSettings = function () {
        return {
            durable: false,
        };
    };
    return RabbitMqPublisher;
}());
exports.RabbitMqPublisher = RabbitMqPublisher;


/***/ }),
/* 7 */
/***/ (function(module, exports, __webpack_require__) {

"use strict";

var __assign = (this && this.__assign) || Object.assign || function(t) {
    for (var s, i = 1, n = arguments.length; i < n; i++) {
        s = arguments[i];
        for (var p in s) if (Object.prototype.hasOwnProperty.call(s, p))
            t[p] = s[p];
    }
    return t;
};
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : new P(function (resolve) { resolve(result.value); }).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
var __generator = (this && this.__generator) || function (thisArg, body) {
    var _ = { label: 0, sent: function() { if (t[0] & 1) throw t[1]; return t[1]; }, trys: [], ops: [] }, f, y, t, g;
    return g = { next: verb(0), "throw": verb(1), "return": verb(2) }, typeof Symbol === "function" && (g[Symbol.iterator] = function() { return this; }), g;
    function verb(n) { return function (v) { return step([n, v]); }; }
    function step(op) {
        if (f) throw new TypeError("Generator is already executing.");
        while (_) try {
            if (f = 1, y && (t = op[0] & 2 ? y["return"] : op[0] ? y["throw"] || ((t = y["return"]) && t.call(y), 0) : y.next) && !(t = t.call(y, op[1])).done) return t;
            if (y = 0, t) op = [op[0] & 2, t.value];
            switch (op[0]) {
                case 0: case 1: t = op; break;
                case 4: _.label++; return { value: op[1], done: false };
                case 5: _.label++; y = op[1]; op = [0]; continue;
                case 7: op = _.ops.pop(); _.trys.pop(); continue;
                default:
                    if (!(t = _.trys, t = t.length > 0 && t[t.length - 1]) && (op[0] === 6 || op[0] === 2)) { _ = 0; continue; }
                    if (op[0] === 3 && (!t || (op[1] > t[0] && op[1] < t[3]))) { _.label = op[1]; break; }
                    if (op[0] === 6 && _.label < t[1]) { _.label = t[1]; t = op; break; }
                    if (t && _.label < t[2]) { _.label = t[2]; _.ops.push(op); break; }
                    if (t[2]) _.ops.pop();
                    _.trys.pop(); continue;
            }
            op = body.call(thisArg, _);
        } catch (e) { op = [6, e]; y = 0; } finally { f = t = 0; }
        if (op[0] & 5) throw op[1]; return { value: op[0] ? op[1] : void 0, done: true };
    }
};
Object.defineProperty(exports, "__esModule", { value: true });
var Promisefy = __webpack_require__(1);
var common_1 = __webpack_require__(2);
var childLogger_1 = __webpack_require__(0);
var RabbitMqSubscriber = /** @class */ (function () {
    function RabbitMqSubscriber(logger, connectionFactory) {
        this.logger = logger;
        this.connectionFactory = connectionFactory;
        this.logger = childLogger_1.createChildLogger(logger, "RabbitMqConsumer");
    }
    RabbitMqSubscriber.prototype.subscribe = function (queue, action) {
        var _this = this;
        var queueConfig = common_1.asPubSubQueueNameConfig(queue);
        return this.connectionFactory.create()
            .then(function (connection) { return connection.createChannel(); })
            .then(function (channel) {
            _this.logger.trace("got channel for queue '%s'", queueConfig.name);
            return _this.setupChannel(channel, queueConfig)
                .then(function (queueName) {
                _this.logger.debug("queue name generated for subscription queue '(%s)' is '(%s)'", queueConfig.name, queueName);
                var queConfig = __assign({}, queueConfig, { dlq: queueName });
                return _this.subscribeToChannel(channel, queueConfig, action);
            });
        });
    };
    RabbitMqSubscriber.prototype.setupChannel = function (channel, queueConfig) {
        this.logger.trace("setup '%j'", queueConfig);
        return this.getChannelSetup(channel, queueConfig);
    };
    RabbitMqSubscriber.prototype.subscribeToChannel = function (channel, queueConfig, action) {
        var _this = this;
        this.logger.trace("subscribing to queue '%s'", queueConfig.name);
        return channel.consume(queueConfig.dlq, function (message) {
            var msg;
            Promisefy.try(function () {
                msg = _this.getMessageObject(message);
                _this.logger.trace("message arrived from queue '%s' (%j)", queueConfig.name, msg);
                return action(msg);
            }).then(function () {
                _this.logger.trace("message processed from queue '%s' (%j)", queueConfig.name, msg);
                channel.ack(message);
            }).catch(function (err) {
                _this.logger.error(err, "message processing failed from queue '%j' (%j)", queueConfig, msg);
                channel.nack(message, false, false);
            });
        }).then(function (opts) {
            _this.logger.trace("subscribed to queue '%s' (%s)", queueConfig.name, opts.consumerTag);
            return (function () {
                _this.logger.trace("disposing subscriber to queue '%s' (%s)", queueConfig.name, opts.consumerTag);
                return Promisefy.resolve(channel.cancel(opts.consumerTag).then(function () { return channel.close(); })).return();
            });
        });
    };
    RabbitMqSubscriber.prototype.getMessageObject = function (message) {
        return JSON.parse(message.content.toString('utf8'));
    };
    RabbitMqSubscriber.prototype.getChannelSetup = function (channel, queueConfig) {
        return __awaiter(this, void 0, void 0, function () {
            var result;
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0: return [4 /*yield*/, channel.assertExchange(queueConfig.dlx, 'fanout', this.getDLSettings())];
                    case 1:
                        _a.sent();
                        return [4 /*yield*/, channel.assertQueue(queueConfig.dlq, this.getQueueSettings(queueConfig.dlx))];
                    case 2:
                        result = _a.sent();
                        return [4 /*yield*/, channel.bindQueue(result.queue, queueConfig.dlx, '')];
                    case 3:
                        _a.sent();
                        return [2 /*return*/, result.queue];
                }
            });
        });
    };
    RabbitMqSubscriber.prototype.getQueueSettings = function (deadletterExchangeName) {
        return {
            exclusive: true
        };
    };
    RabbitMqSubscriber.prototype.getDLSettings = function () {
        return {
            durable: false
        };
    };
    return RabbitMqSubscriber;
}());
exports.RabbitMqSubscriber = RabbitMqSubscriber;


/***/ }),
/* 8 */
/***/ (function(module, exports, __webpack_require__) {

"use strict";

Object.defineProperty(exports, "__esModule", { value: true });
var connectionFactory_1 = __webpack_require__(3);
exports.RabbitMqConnectionFactory = connectionFactory_1.RabbitMqConnectionFactory;
exports.RabbitMqSingletonConnectionFactory = connectionFactory_1.RabbitMqSingletonConnectionFactory;
var consumer_1 = __webpack_require__(4);
exports.RabbitMqConsumer = consumer_1.RabbitMqConsumer;
var producer_1 = __webpack_require__(5);
exports.RabbitMqProducer = producer_1.RabbitMqProducer;
var publisher_1 = __webpack_require__(6);
exports.RabbitMqPublisher = publisher_1.RabbitMqPublisher;
var subscriber_1 = __webpack_require__(7);
exports.RabbitMqSubscriber = subscriber_1.RabbitMqSubscriber;


/***/ }),
/* 9 */
/***/ (function(module, exports) {

module.exports = require("amqplib");

/***/ })
/******/ ]);
//# sourceMappingURL=main.js.map