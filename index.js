module.exports = function (RED) {
    "use strict";
    const { ServiceBroker } = require('moleculer')
    let brokers = {}

    RED.events.on('nodes-stopped', async (event) => {
        for (let prop in brokers) {
            await brokers[prop]['broker'].stop()
        }
    })

    RED.events.on('nodes-started', async (event) => {
        if (brokers !== {}) {
            for (let i in brokers) {
                for (let j in brokers[i]['services']) {
                    let service = {
                        name: brokers[i]['services'][j]['name']
                    }
                    if (brokers[i]['services'][j]['version'] !== "") {
                        service['version'] = brokers[i]['services'][j]['version']
                    }
                    if (brokers[i]['services'][j]['events'] !== undefined) {
                        service['events'] = brokers[i]['services'][j]['events']
                    }
                    if (brokers[i]['services'][j]['actions'] !== undefined) {
                        service['actions'] = brokers[i]['services'][j]['actions']
                    }
                    if (brokers[i]['services'][j]['mixins'] !== undefined) {
                        service['mixins'] = brokers[i]['services'][j]['mixins']
                    }
                    if (brokers[i]['services'][j]['settings'] !== undefined) {
                        service['settings'] = brokers[i]['services'][j]['settings']
                    }
                    let svc = brokers[i]['broker'].createService(service);

                    if (brokers[i]['services'][j]['path'] !== undefined && svc.express !== undefined) {
                        RED.httpNode.use(brokers[i]['services'][j]['path'], svc.express());
                    }
                }
                await brokers[i]['broker'].start()
            }
        }
    })


    function MoleculerConfigNode(n) {
        RED.nodes.createNode(this, n);
        this.name = n.name;
        this.options = n.options;
        this.optionsType = n.optionsType;
        let node = this
        let options = {}
        try {
            if (node.optionsType) {
                options = RED.util.evaluateNodeProperty(node.options, node.optionsType, node)
            } else {
                options = JSON.parse(node.options)
            }
        } catch (e) {
            options = {}
        }
        brokers[node.name] = { broker: null, services: {}, options }
        brokers[node.name]['broker'] = new ServiceBroker(brokers[node.name]['options']);
        node.on('close', async (done) => {
            await brokers[node.name]['broker'].stop()
            done()
        })
    }
    RED.nodes.registerType("moleculer-config", MoleculerConfigNode);


    function MoleculerServiceConfigNode(n) {
        RED.nodes.createNode(this, n);
        this.name = n.name;
        this.version = n.version;
        this.settingsType = n.settingsType;
        this.settings = n.settings;

        let node = this
        node.on('close', (done) => {
            done()
        })
    }
    RED.nodes.registerType("moleculer-service-config", MoleculerServiceConfigNode);

    function eventNode(n) {
        RED.nodes.createNode(this, n);
        this.broker = RED.nodes.getNode(n.broker);
        this.service = RED.nodes.getNode(n.service);
        this.name = n.name;
        this.topic = n.topic;
        this.group = n.group;
        let node = this
        createEvent(node)
    }
    RED.nodes.registerType("moleculer-event", eventNode);

    function emitNode(n) {
        RED.nodes.createNode(this, n);
        this.broker = RED.nodes.getNode(n.broker);
        this.name = n.name;
        this.topic = n.topic;
        this.broadcast = n.broadcast;
        this.group = n.group;
        var node = this
        createEmit(node);
    }
    RED.nodes.registerType("moleculer-emit", emitNode);

    function callNode(n) {
        RED.nodes.createNode(this, n);
        this.broker = RED.nodes.getNode(n.broker);
        this.name = n.name;
        this.topic = n.topic;
        this.options = n.options;
        this.optionsType = n.optionsType;
        var node = this
        createCall(node);
    }
    RED.nodes.registerType("moleculer-call", callNode);

    function reqActionNode(n) {
        RED.nodes.createNode(this, n);
        this.broker = RED.nodes.getNode(n.broker);
        this.service = RED.nodes.getNode(n.service);
        this.name = n.name;
        this.topic = n.topic;
        var node = this
        createAction(node);
    }
    RED.nodes.registerType("moleculer-request-action", reqActionNode);

    function resActionNode(n) {
        RED.nodes.createNode(this, n);
        this.name = n.name;
        this.topic = n.topic;
        var node = this
        responseAction(node);
    }
    RED.nodes.registerType("moleculer-response-action", resActionNode);


    function apigwNode(n) {
        RED.nodes.createNode(this, n);
        this.broker = RED.nodes.getNode(n.broker);
        this.service = RED.nodes.getNode(n.service);
        this.name = n.name;
        this.path = n.path;
        var node = this;
        createApigw(node);
    }
    RED.nodes.registerType("moleculer-apigw", apigwNode);

    function getBroker(config) {
        if (config && config['name'] !== undefined) {
            if (brokers[config.name] !== undefined) {
                return brokers[config.name]
            } else {
                brokers[config.name] = { broker: null, services: {}, options: RED.util.evaluateNodeProperty(config.options, config.optionsType, config) }
                return brokers[config.name]
            }
        } else {
            throw new Error('Missing Broker Config')
        }
    }

    function getService(node) {
        if (node) {
            return node.version + '.' + node.name
        } else {
            throw new Error('Missing Service Config')
        }
    }

    function createEvent(node) {
        let broker = getBroker(node.broker)
        let serviceName = getService(node.service)
        if (!broker['services'].hasOwnProperty(serviceName)) {
            broker['services'][serviceName] = {}
        }
        broker['services'][serviceName]['name'] = node.service.name;
        broker['services'][serviceName]['version'] = node.service.version;
        try {
            if (node.service.settingsType) {
                broker['services'][serviceName]['settings'] = RED.util.evaluateNodeProperty(node.service.settings, node.service.settingsType, node)
            } else {
                broker['services'][serviceName]['settings'] = JSON.parse(node.service.settings)
            }
        } catch (e) {
            broker['services'][serviceName]['settings'] = {}
        }
        if (!broker['services'][serviceName].hasOwnProperty('events')) {
            broker['services'][serviceName]['events'] = {}
        }
        broker['services'][serviceName]['events'][node.topic] = {
            handler: (payload, sender, event) => {
                let msg = { topic: node.topic, payload, sender, event }
                node.status({ fill: 'blue', shape: 'dot', text: 'receiving event...' })
                setTimeout(() => { node.status({}) }, 500)
                node.send(msg)
            }
        }
        if (node.group !== "") {
            broker['services'][serviceName]['events'][node.topic]['group'] = node.group.split(',')
        }
    }

    function createEmit(node) {
        let broker = getBroker(node.broker)
        node.on('input', (msg) => {

            let topic = msg.topic || node.topic || null
            let group = msg.group || node.group
            let bcast = msg.broadcast || node.broadcast

            let func = 'emit'
            let status = 'emitting...'
            if (bcast) {
                func = 'broadcast'
                status = 'broadcasting...'
            }
            let groups = null
            if (group !== "") {
                groups = group.split(',')
            }
            if (topic) {
                node.status({ fill: 'blue', shape: 'dot', text: status })
                broker['broker'][func](topic, msg.payload, groups)
                setTimeout(() => { node.status({}) }, 500)
            } else {
                node.error('Missing topic, please send topic on msg.topic or Node Topic.', msg)
            }
        })
    }

    function createCall(node) {
        let broker = getBroker(node.broker)
        node.on('input', async (msg) => {
            try {

                let action = msg.action || node.topic
                let options = msg.options || {}
                if (options === {} && node.optionsType) {
                    options = RED.util.evaluateNodeProperty(node.options, node.optionsType, node, msg)
                } else {
                    if (options === {}) {
                        try {
                            options = JSON.parse(node.options)
                        } catch (e) {
                            options = {}
                        }
                    }
                }
                if (action !== "") {
                    node.status({ fill: 'blue', shape: 'dot', text: 'requesting...' })
                    let res = await broker['broker'].call(action, msg.payload, options)
                    msg.payload = res
                    setTimeout(() => { node.status({}) }, 500)
                    node.send(msg)
                } else {
                    node.error('Missing action, please send action on msg.action or Node Action.', msg)
                }
            } catch (e) {
                node.status({ fill: 'red', shape: 'ring', text: 'error' })
                node.error(e, msg)
                setTimeout(() => { node.status({}) }, 500)
            }
        })
    }

    function createAction(node) {
        let broker = getBroker(node.broker)
        let serviceName = getService(node.service)
        if (!broker['services'].hasOwnProperty(serviceName)) {
            broker['services'][serviceName] = {}
        }
        broker['services'][serviceName]['name'] = node.service.name;
        broker['services'][serviceName]['version'] = node.service.version;
        try {
            if (node.service.settingsType) {
                broker['services'][serviceName]['settings'] = RED.util.evaluateNodeProperty(node.service.settings, node.service.settingsType, node)
            } else {
                broker['services'][serviceName]['settings'] = JSON.parse(node.service.settings)
            }
        } catch (e) {
            broker['services'][serviceName]['settings'] = {}
        }

        if (!broker['services'][serviceName].hasOwnProperty('actions')) {
            broker['services'][serviceName]['actions'] = {}
        }
        broker['services'][serviceName]['actions'][node.topic] = (ctx) => {
            return new Promise((resolve, reject) => {
                node.status({ fill: 'blue', shape: 'dot', text: 'receiving request...' })
                setTimeout(() => { node.status({}) }, 500)
                let msg = {
                    action: node.topic,
                    payload: ctx.params,
                    _res: { resolve, reject },
                    emit: ctx.emit.bind(ctx),
                    broadcast: ctx.broadcast.bind(ctx),
                    call: ctx.call.bind(ctx)
                }
                node.send(msg)
            })
        }
    }

    function responseAction(node) {
        node.on('input', (msg) => {
            node.status({ fill: 'blue', shape: 'dot', text: 'sending response...' })
            setTimeout(() => { node.status({}) }, 500)
            if (msg._res !== undefined) {
                msg._res.resolve(msg.payload)
            } else {
                node.error('Request Action required', msg)
            }
        })
    }

    function createApigw(node) {
        let ApiGatewayService = require("moleculer-web")
        let broker = getBroker(node.broker)
        let serviceName = getService(node.service)
        if (!broker['services'].hasOwnProperty(serviceName)) {
            broker['services'][serviceName] = {}
        }
        broker['services'][serviceName]['name'] = node.service.name
        broker['services'][serviceName]['version'] = node.service.version
        try {
            if (node.service.settingsType) {
                broker['services'][serviceName]['settings'] = RED.util.evaluateNodeProperty(node.service.settings, node.service.settingsType, node)
            } else {
                broker['services'][serviceName]['settings'] = JSON.parse(node.service.settings)
            }
        } catch (e) {
            broker['services'][serviceName]['settings'] = {}
        }
        broker['services'][serviceName]['mixins'] = ApiGatewayService
        broker['services'][serviceName]['settings']['server'] = false
        broker['services'][serviceName]['path'] = node.path || '/'
    }

};
