module.exports = function (RED) {
    "use strict";
    const { ServiceBroker } = require('moleculer')
    let brokers = {}
    //console.log(RED);
    /*
        'type-registered': [Function],
          'node-status': [Function: handleStatusEvent],
          'runtime-event': [Function: handleRuntimeEvent],
          comms: [Function: handleCommsEvent],
          'event-log'
    */
    // RED.events.on('node-status', console.log.bind(null, 'status'))
    // RED.events.on('runtime-event', console.log.bind(null, 'runtime'))
    // RED.events.on('event-log', console.log.bind(null, 'log'))
    // RED.events.on('type-registered', console.log.bind(null, 'reg'))
    // RED.events.on('nodes-started', console.log.bind(null, 'nodes'))
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
                        name: brokers[i]['services'][j]['name'],
                        events: brokers[i]['services'][j]['events'],
                        actions: brokers[i]['services'][j]['actions']
                    }
                    if (brokers[i]['services'][j]['version'] !== "") {
                        service['version'] = brokers[i]['services'][j]['version']
                    }
                    brokers[i]['broker'].createService(service);
                }
                await brokers[i]['broker'].start()
            }
        }
    })


    function MoleculerConfig(n) {
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
        node.on('close', async(done) => {
            await brokers[node.name]['broker'].stop()
            done()
        })
    }
    RED.nodes.registerType("moleculer-config", MoleculerConfig);


    function MoleculerServiceConfig(n) {
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
    RED.nodes.registerType("moleculer-service-config", MoleculerServiceConfig);

    function event(n) {
        RED.nodes.createNode(this, n);
        this.broker = RED.nodes.getNode(n.broker);
        this.service = RED.nodes.getNode(n.service);
        this.name = n.name;
        this.topic = n.topic;
        this.group = n.group;
        let node = this
        createEvent(node)
    }
    RED.nodes.registerType("moleculer-event", event);

    function emit(n) {
        RED.nodes.createNode(this, n);
        this.broker = RED.nodes.getNode(n.broker);
        this.name = n.name;
        this.topic = n.topic;
        this.broadcast = n.broadcast;
        this.group = n.group;
        var node = this
        createEmit(node);
    }
    RED.nodes.registerType("moleculer-emit", emit);

    function call(n) {
        RED.nodes.createNode(this, n);
        this.broker = RED.nodes.getNode(n.broker);
        this.name = n.name;
        this.topic = n.topic;
        this.options = n.options;
        this.optionsType = n.optionsType;
        var node = this
        createCall(node);
    }
    RED.nodes.registerType("moleculer-call", call);

    function reqAction(n) {
        RED.nodes.createNode(this, n);
        this.broker = RED.nodes.getNode(n.broker);
        this.service = RED.nodes.getNode(n.service);
        this.name = n.name;
        this.topic = n.topic;
        var node = this
        createAction(node);
    }
    RED.nodes.registerType("moleculer-request-action", reqAction);

    function resAction(n) {
        RED.nodes.createNode(this, n);
        this.name = n.name;
        this.topic = n.topic;
        var node = this
        responseAction(node);
    }
    RED.nodes.registerType("moleculer-response-action", resAction);

    function getBroker(config) {
        if (brokers[config.name] !== undefined) {
            return brokers[config.name]
        } else {
            brokers[config.name] = { broker: null, services: {}, options: RED.util.evaluateNodeProperty(config.options, config.optionsType, config) }
            return brokers[config.name]
        }
    }

    function createEvent(node) {
        let broker = getBroker(node.broker)
        let serviceName = node.service.version + '.' + node.service.name
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
        if(node.group !== ""){
            broker['services'][serviceName]['events'][node.topic]['group'] = node.group.split(',')
        }
    }

    function createEmit(node) {
        let broker = getBroker(node.broker)
        node.on('input', (msg) => {

            let topic = msg.topic || node.topic
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

            node.status({ fill: 'blue', shape: 'dot', text: status })
            broker['broker'][func](topic, msg.payload, groups)
            setTimeout(() => { node.status({}) }, 500)
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
                node.status({ fill: 'blue', shape: 'dot', text: 'requesting...' })
                let res = await broker['broker'].call(action, msg.payload, options)
                msg.payload = res
                setTimeout(() => { node.status({}) }, 500)
                node.send(msg)
            } catch (e) {
                node.status({ fill: 'red', shape: 'ring', text: 'error' })
                node.error(e)
                setTimeout(() => { node.status({}) }, 500)
            }
        })
    }

    function createAction(node) {
        let broker = getBroker(node.broker)
        let serviceName = node.service.version + '.' + node.service.name
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
                let msg = { action: node.topic, payload: ctx.params, _res: { resolve, reject } }
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

};
