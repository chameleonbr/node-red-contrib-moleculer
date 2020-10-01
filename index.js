module.exports = function (RED) {
  "use strict";
  const { ServiceBroker } = require("moleculer");
  let brokers = {};
  let dynamicMiddleware = require("express-dynamic-middleware").create();

  RED.httpNode.use(dynamicMiddleware.handle());

  RED.events.on("nodes-stopped", async (event) => {
    dynamicMiddleware.clean();
    for (let prop in brokers) {
      await brokers[prop]["broker"].stop();
    }
  });

  RED.events.on("nodes-started", async (event) => {
    if (brokers !== {}) {
      for (let i in brokers) {
        for (let j in brokers[i]["services"]) {
          let service = {
            name: brokers[i]["services"][j]["name"],
          };
          if (brokers[i]["services"][j]["version"] !== "") {
            service["version"] = brokers[i]["services"][j]["version"];
          }
          if (brokers[i]["services"][j]["events"] !== undefined) {
            service["events"] = brokers[i]["services"][j]["events"];
          }
          if (brokers[i]["services"][j]["actions"] !== undefined) {
            service["actions"] = brokers[i]["services"][j]["actions"];
          }
          if (brokers[i]["services"][j]["mixins"] !== undefined) {
            service["mixins"] = brokers[i]["services"][j]["mixins"];
          }
          if (brokers[i]["services"][j]["settings"] !== undefined) {
            service["settings"] = brokers[i]["services"][j]["settings"];
          }

          service["settings"]['onError'] = (req, res, err) => {
              res.status(err.code || 500);
              res.send(err);
              res.end();
          };

          let svc = brokers[i]["broker"].createService(service);
          if (
            brokers[i]["services"][j]["apigw"] !== undefined &&
            svc.express !== undefined
          ) {
            dynamicMiddleware.use(svc.express());
          }
        }
        await brokers[i]["broker"].start();
      }
    }
  });

  function MoleculerConfigNode(n) {
    RED.nodes.createNode(this, n);
    this.name = n.name;
    this.options = n.options;
    this.optionsType = n.optionsType;
    let node = this;
    let options = {};
    try {
      if (node.optionsType) {
        options = RED.util.evaluateNodeProperty(
          node.options,
          node.optionsType,
          node
        );
      } else {
        options = JSON.parse(node.options);
      }
    } catch (e) {
      options = {};
    }
    brokers[node.name] = { broker: null, services: {}, options };
    brokers[node.name]["broker"] = new ServiceBroker(
      brokers[node.name]["options"]
    );
    node.on("close", async (done) => {
      await brokers[node.name]["broker"].stop();
      done();
    });
  }
  RED.nodes.registerType("moleculer-config", MoleculerConfigNode);

  function MoleculerServiceConfigNode(n) {
    RED.nodes.createNode(this, n);
    this.name = n.name;
    this.version = n.version;
    this.rest = n.rest;
    this.settingsType = n.settingsType;
    this.settings = n.settings;

    let node = this;
    node.on("close", (done) => {
      done();
    });
  }
  RED.nodes.registerType(
    "moleculer-service-config",
    MoleculerServiceConfigNode
  );

  function eventNode(n) {
    RED.nodes.createNode(this, n);
    this.broker = RED.nodes.getNode(n.broker);
    this.service = RED.nodes.getNode(n.service);
    this.name = n.name;
    this.topic = n.topic;
    this.group = n.group;
    let node = this;
    createEvent(node);
  }
  RED.nodes.registerType("moleculer-event", eventNode);

  function emitNode(n) {
    RED.nodes.createNode(this, n);
    this.broker = RED.nodes.getNode(n.broker);
    this.name = n.name;
    this.topic = n.topic;
    this.broadcast = n.broadcast;
    this.group = n.group;
    var node = this;
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
    var node = this;
    createCall(node);
  }
  RED.nodes.registerType("moleculer-call", callNode);

  function reqActionNode(n) {
    RED.nodes.createNode(this, n);
    this.broker = RED.nodes.getNode(n.broker);
    this.service = RED.nodes.getNode(n.service);
    this.name = n.name;
    this.topic = n.topic;
    this.rest = n.rest;
    this.restType = n.restType;
    this.params = n.params;
    var node = this;
    createAction(node);
  }
  RED.nodes.registerType("moleculer-request-action", reqActionNode);

  function resActionNode(n) {
    RED.nodes.createNode(this, n);
    this.name = n.name;
    this.topic = n.topic;
    var node = this;
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

  function MoleculerInstance(n) {
    RED.nodes.createNode(this, n);
    this.broker = RED.nodes.getNode(n.broker);
    this.location = n.location;
    this.name = n.name;
    this.topic = n.topic;
    var node = this;
    let client = getBroker(node.broker)['broker'];
    this.context()[node.location].set(node.topic, client);

    node.on("close", function (done) {
      node.status({});
      this.context()[node.location].set(node.topic, null);
      done();
    });
  }
  RED.nodes.registerType("moleculer-instance", MoleculerInstance);

  function getBroker(config) {
    if (config && config["name"] !== undefined) {
      if (brokers[config.name] !== undefined) {
        return brokers[config.name];
      } else {
        brokers[config.name] = {
          broker: null,
          services: {},
          options: RED.util.evaluateNodeProperty(
            config.options,
            config.optionsType,
            config
          ),
        };
        return brokers[config.name];
      }
    } else {
      throw new Error("Missing Broker Config");
    }
  }

  function getService(node) {
    if (node) {
      return node.version + "." + node.name;
    } else {
      throw new Error("Missing Service Config");
    }
  }

  function createEvent(node) {
    try {
      let broker = getBroker(node.broker);
      let serviceName = getService(node.service);
      if (!broker["services"].hasOwnProperty(serviceName)) {
        broker["services"][serviceName] = {};
      }
      broker["services"][serviceName]["name"] = node.service.name;
      broker["services"][serviceName]["version"] = node.service.version;
      try {
        if (node.service.settingsType) {
          broker["services"][serviceName][
            "settings"
          ] = RED.util.evaluateNodeProperty(
            node.service.settings,
            node.service.settingsType,
            node
          );
        } else {
          broker["services"][serviceName]["settings"] = JSON.parse(
            node.service.settings
          );
        }
      } catch (e) {
        broker["services"][serviceName]["settings"] = {};
      }
      if (!broker["services"][serviceName].hasOwnProperty("events")) {
        broker["services"][serviceName]["events"] = {};
      }
      broker["services"][serviceName]["events"][node.topic] = {
        handler: (payload, sender, event) => {
          let msg = { topic: node.topic, payload, sender, event };
          node.status({
            fill: "blue",
            shape: "dot",
            text: "receiving event...",
          });
          setTimeout(() => {
            node.status({});
          }, 200);
          node.send(msg);
        },
      };
      if (node.group !== "") {
        broker["services"][serviceName]["events"][node.topic][
          "group"
        ] = node.group.split(",");
      }
    } catch (err) {
      node.error(err);
    }
  }

  function createEmit(node) {
    try {
      let broker = getBroker(node.broker);
      node.on("input", (msg, send, done) => {
        send = send || function() { node.send.apply(node,arguments) }
        done = done || function(e) { if(e){node.error(e, msg)}; }
        
        let topic = msg.topic || node.topic || null;
        let group = msg.group || node.group;
        let bcast = msg.broadcast || node.broadcast;

        let func = "emit";
        let status = "emitting...";
        if (bcast) {
          func = "broadcast";
          status = "broadcasting...";
        }
        let groups = null;
        if (group !== "") {
          groups = group.split(",");
        }
        if (topic) {
          node.status({ fill: "blue", shape: "dot", text: status });
          broker["broker"][func](topic, msg.payload, groups);
          setTimeout(() => {
            node.status({});
          }, 200);
          done();
        } else {
          done(new Error("Missing topic, please send topic on msg.topic or Node Topic."));
        }
      });
    } catch (err) {
      node.error(err);
    }
  }

  function createCall(node) {
    try {
      let broker = getBroker(node.broker);
      node.on("input", async (msg, send, done)=>{
        send = send || function() { node.send.apply(node,arguments) }
        done = done || function(e) { if(e){node.error(e, msg)}; }

        try {
          let action = msg.action || node.topic;
          let options = msg.options || {};
          if (options === {} && node.optionsType) {
            options = RED.util.evaluateNodeProperty(
              node.options,
              node.optionsType,
              node,
              msg
            );
          } else {
            if (options === {}) {
              try {
                options = JSON.parse(node.options);
              } catch (e) {
                options = {};
              }
            }
          }
          if (action !== "") {
            node.status({ fill: "blue", shape: "dot", text: "requesting..." });
            let res = await broker["broker"].call(action, msg.payload, options);
            msg.payload = res;
            setTimeout(() => {
              node.status({});
            }, 200);
            send(msg);
            done();
          } else {
            done(new Error("Missing action, please send action on msg.action or Node Action."));
          }
        } catch (e) {
          node.status({ fill: "red", shape: "ring", text: "error" });
          msg.payload = e.message;
          msg.data = e.data;
          done(e);
          setTimeout(() => {
            node.status({});
          }, 200);
        }
      });
    } catch (err) {
      node.error(err);
    }
  }

  function createAction(node) {
    try {
      let broker = getBroker(node.broker);
      let serviceName = getService(node.service);
      if (!broker["services"].hasOwnProperty(serviceName)) {
        broker["services"][serviceName] = {};
      }
      broker["services"][serviceName]["name"] = node.service.name;
      broker["services"][serviceName]["version"] = node.service.version;
      try {
        if (node.service.settingsType) {
          broker["services"][serviceName][
            "settings"
          ] = RED.util.evaluateNodeProperty(
            node.service.settings,
            node.service.settingsType,
            node
          );
        } else {
          broker["services"][serviceName]["settings"] = JSON.parse(
            node.service.settings
          );
        }
      } catch (e) {
        broker["services"][serviceName]["settings"] = {};
      }
      if(node.service.rest){
        broker["services"][serviceName]["settings"]['rest'] = node.service.rest;
      }

      if (!broker["services"][serviceName].hasOwnProperty("actions")) {
        broker["services"][serviceName]["actions"] = {};
      }

      broker["services"][serviceName]["actions"][node.topic] = {};

      if (node.rest && node.rest !== "{}" && node.rest !== "") {
        let rest = null;
        try {
          rest = RED.util.evaluateNodeProperty(
            node.rest,
            node.restType,
            node
          );
        } catch (e) {
          rest = null;
        }

        if (rest) {
          broker["services"][serviceName]["actions"][node.topic][
            "rest"
          ] = rest;
        }
      }

      if (node.params && node.params !== "{}") {
        let params = null;
        try {
          params = JSON.parse(node.params);
        } catch (e) {
          params = null;
        }

        if (params) {
          broker["services"][serviceName]["actions"][node.topic][
            "params"
          ] = params;
        }
      }

      broker["services"][serviceName]["actions"][node.topic][
        "handler"
      ] = async function (ctx) {
        return new Promise((resolve, reject) => {
          node.status({
            fill: "blue",
            shape: "dot",
            text: "receiving request...",
          });
          setTimeout(() => {
            node.status({});
          }, 200);
          let msg = {
            action: node.topic,
            payload: ctx.params,
            _res: { resolve, reject },
            emit: ctx.emit.bind(ctx),
            broadcast: ctx.broadcast.bind(ctx),
            call: ctx.call.bind(ctx),
            meta: ctx.meta || null,
          };
          node.send(msg);
        });
      };
    } catch (err) {
      node.error(err);
    }
  }

  function responseAction(node) {
    node.on("input", (msg, send, done) => {
      send = send || function() { node.send.apply(node,arguments) }
      done = done || function(e) { if(e){node.error(e, msg)}; }
      node.status({ fill: "blue", shape: "dot", text: "sending response..." });
      setTimeout(() => {
        node.status({});
      }, 200);
      if (msg._res !== undefined) {
        msg._res.resolve(msg.payload);
        done();
      } else {
        done(new Error("Missing action, please send action on msg.action or Node Action."));
      }
    });
  }

  function createApigw(node) {
    try {
      let ApiGatewayService = require("moleculer-web");
      let broker = getBroker(node.broker);
      let serviceName = getService(node.service);
      if (!broker["services"].hasOwnProperty(serviceName)) {
        broker["services"][serviceName] = {};
      }
      broker["services"][serviceName]["name"] = node.service.name;
      broker["services"][serviceName]["version"] = node.service.version;
      try {
        if (node.service.settingsType) {
          broker["services"][serviceName][
            "settings"
          ] = RED.util.evaluateNodeProperty(
            node.service.settings,
            node.service.settingsType,
            node
          );
        } else {
          broker["services"][serviceName]["settings"] = JSON.parse(
            node.service.settings
          );
        }
      } catch (e) {
        broker["services"][serviceName]["settings"] = {};
      }
      broker["services"][serviceName]["mixins"] = ApiGatewayService;
      broker["services"][serviceName]["settings"]["server"] = false;
      broker["services"][serviceName]["settings"]["middleware"] = true;
      broker["services"][serviceName]["apigw"] = true;
    } catch (err) {
      node.error(err);
    }
  }
};
