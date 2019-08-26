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
            brokers[prop]['broker'].stop()
        }
    })

    RED.events.on('nodes-started', async (event) => {
        if (brokers !== {}) {
            for (let i in brokers) {
                brokers[i]['broker'] = new ServiceBroker(JSON.parse(brokers[i]['options']))
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
        node.on('close', (done) => {
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
            brokers[config.name] = { broker: null, services: {}, options: RED.util.evaluateNodeProperty(config.options,config.optionsType, config) }
            return brokers[config.name]
        }
    }


    function createEvent(node) {
        let broker = getBroker(node.broker)
        let serviceName = node.service.version + '.' + node.service.name
        if (!broker['services'].hasOwnProperty(serviceName)) {
            broker['services'][serviceName] = { 
                name: node.service.name, 
                version: node.service.version, 
                settings: RED.util.evaluateNodeProperty(node.service.settings,node.service.settingsType, node) 
            }
        }
        if (!broker['services'][serviceName].hasOwnProperty('events')) {
            broker['services'][serviceName]['events'] = {}
        }
        broker['services'][serviceName]['events'][node.topic] = {
            handler: (payload, sender, event) => {
                let msg = { topic: node.topic, payload, sender, event }
                node.send(msg)
            }
        }
    }

    function createEmit(node) {
        let broker = getBroker(node.broker)
        node.on('input', (msg) => {
            broker['broker'].emit(node.topic, msg.payload, node.group.split(','))
        })
    }

    function createCall(node) {
        let broker = getBroker(node.broker)
        node.on('input', async (msg) => {
            try {
                node.status({ fill: 'blue', shape: 'dot', text: 'Requesting...' })
                let res = await broker['broker'].call(node.topic, msg.payload, RED.util.evaluateNodeProperty(node.options,node.optionsType, node, msg))
                msg.payload = res
                node.status({})
                node.send(msg)
            } catch (e) {
                node.status({ fill: 'red', shape: 'ring', text: 'Error' })
                node.error(e)
                setTimeout(()=>{node.status({})},2000)
            }
        })
    }

    function createAction(node) {
        let broker = getBroker(node.broker)
        let serviceName = node.service.version + '.' + node.service.name
        if (!broker['services'].hasOwnProperty(serviceName)) {
            broker['services'][serviceName] = { 
                name: node.service.name, 
                version: node.service.version, 
                settings: RED.util.evaluateNodeProperty(node.service.settings,node.service.settingsType, node) 
            }
        }
        if (!broker['services'][serviceName].hasOwnProperty('actions')) {
            broker['services'][serviceName]['actions'] = {}
        }
        broker['services'][serviceName]['actions'][node.topic] = (ctx) => {
            return new Promise((resolve, reject) => {
                console.log('call')
                let msg = { topic: node.topic, payload: ctx.params, _res: { resolve, reject } }
                node.send(msg)
            })
        }
    }

    function responseAction(node) {
        node.on('input', (msg) => {
            if (msg._res !== undefined) {
                msg._res.resolve(msg.payload)
            } else {
                node.error('Request Action required')
            }
        })
    }

};
