const axios = require('axios').default;
const { URLSearchParams } = require('url');

class SolrInstance {
  #client

  constructor(baseURL) {
    if (process.env.SOCK5_PROXY) {
      const [host, port] = process.env.SOCK5_PROXY.split(':')
      const SocksAgent = require('axios-socks5-agent');
      const { httpAgent, httpsAgent } = SocksAgent({ agentOptions: { keepAlive: true, }, host, port: Number(port) });
      this.client = axios.create({ baseURL, httpAgent, httpsAgent });
    } else {
      this.client = axios.create({ baseURL });
    }
  }

  async deleteDeadReplicas(collection, shard) {
    if (!shard) shard = 'shard1';
    const state = await this.#request('CLUSTERSTATUS', { collection });
    const collectionState = state.cluster.collections[collection];
    const replicas = collectionState.shards[shard].replicas;
    for (const replica in replicas) {
      if (replicas[replica].state == 'down') {
        console.info(`Deleting dead replica ${collectipon}.${shard}.${replica}`);
        await this.#request('DELETEREPLICA', { collection, shard, replica });
      }
    }
  }

  async addReplicas(collection, shard) {
    if (!shard) shard = 'shard1';
    const state = await this.#request('CLUSTERSTATUS', { collection });
    const collectionState = state.cluster.collections[collection];
    const replicas = collectionState.shards[shard].replicas;
    const desiredCount = Number(collectionState.replicationFactor);
    const liveNodeCount = state.cluster.live_nodes.length;
    const toAdd = Math.min(desiredCount, liveNodeCount) - Object.keys(replicas).length;
    console.info(`Adding ${toAdd} replicas to ${collection}.${shard}`);
    for (let i = 1; i <= toAdd; i++) {
      await this.#request('ADDREPLICA', { collection, shard });
    }
  }

  async redistributeShard(collection, shard) {
    await this.deleteDeadReplicas(collection, shard);
    await this.addReplicas(collection, shard);
  }

  async #request(action, params) {
    const query = new URLSearchParams(params).toString();
    const response = await this.client.get(`/admin/collections?action=${action}&${query}`);
    return response.data;
  }
}

module.exports = SolrInstance;