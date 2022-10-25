import {default as net} from 'net'
import { Socket } from 'net'
// import * as dt from './fancydb'
import * as ss from './fancydb/stateset'
import * as causalGraph from './fancydb/causal-graph'
import handle from './jsonlines'
import { LV, Primitive, RawVersion, ROOT_LV, VersionSummary } from './types'
import { createAgent } from './utils'
import { finished } from 'stream'
import repl from 'node:repl'

let agent = createAgent()

let db = ss.create()

// if (dt.get(db).time == null) {
//   console.log('Setting time = 0')
//   const serverAgent = createAgent()
//   dt.localMapInsert(db, serverAgent(), ROOT_LV, 'time', {type: 'primitive', val: 0})
// }


// const port = +(process.env.PORT ?? '8008')

type Msg = [
  type: 'known version',
  vs: VersionSummary
] | [
  type: 'delta',
  delta: ss.RemoteStateDelta
] | [
  type: 'ack',
  v: RawVersion[]
]

const dbListeners = new Set<() => void>()
const triggerListeners = () => {
  console.log('BROADCAST')
  console.dir(db.values, {depth:null})
  for (const l of dbListeners) l()
}

const runProtocol = (sock: Socket) => {
  let remoteVersion: LV[] | null = null

  const sendDelta = (v: LV[] | null = remoteVersion) => {
    if (v == null) return
    console.log('sending delta to', sock.remoteAddress, sock.remotePort, 'since', v)
    const delta = ss.deltaSince(db, v)
    handler.write(['delta', delta])
    remoteVersion = db.cg.version.slice()
  }

  const sendChanges = () => {
    console.log('sendChanges')

    if (remoteVersion != null && !causalGraph.lvEq(remoteVersion, db.cg.version)) {
      sendDelta()
    }
  }

  finished(sock, () => {
    console.log('finished')
    dbListeners.delete(sendChanges)
  })

  const handler = handle<Msg, Msg>(sock, msg => {
    console.log('msg from', sock.remoteAddress, sock.remotePort)
    console.dir(msg, {depth:null})

    const type = msg[0]
    switch (type) {
      case 'known version': {
        // When we get the known version, we always send a delta so the remote
        // knows they're up to date (even if they were already anyway).
        const summary = msg[1]
        const sv = causalGraph.intersectWithSummary(db.cg, summary)
        console.log('known version', sv)
        sendDelta(sv)
        dbListeners.add(sendChanges) // Only matters the first time.
        break
      }
      case 'delta': {
        if (remoteVersion == null) throw Error('Invalid state - missing known version')

        const delta = msg[1]
        // console.log('got delta')
        // console.dir(delta, {depth:null})
        const updated = ss.mergeDelta(db, delta)
        // console.log('Merged data')
        // console.dir(db.values, {depth:null})

        // console.log('== v', remoteVersion, delta.cg)
        remoteVersion = causalGraph.advanceVersionFromSerialized(
          db.cg, delta.cg, remoteVersion
        )
        // TODO: Ideally, this shouldn't be necessary!
        // console.log('1> v', remoteVersion)
        remoteVersion = causalGraph.findDominators(db.cg, remoteVersion)
        // console.log('-> v', remoteVersion)

        if (updated[0] !== updated[1]) triggerListeners()
        // TODO: Send ack??
        break
      }
      default: console.warn('Unknown message type:', type)
    }
  })

  handler.write(['known version', causalGraph.summarizeVersion(db.cg)])
}

const serverOnPort = (port: number) => {
  const server = net.createServer(async sock => {
    console.log('got server socket connection', sock.remoteAddress, sock.remotePort)
    runProtocol(sock)
    // handler.write({oh: 'hai'})
  })

  server.listen(port, () => {
    console.log(`Server listening on port ${port}`)
  })
}

const connect = (host: string, port: number) => {
  const sock = net.connect({port, host}, () => {
    console.log('connected!')
    runProtocol(sock)
  })
}

for (let i = 2; i < process.argv.length; i++) {
  const command = process.argv[i]
  switch (command) {
    case '-l': {
      const port = +process.argv[i+1]
      if (port === 0 || isNaN(port)) throw Error('Invalid port (usage -l <PORT>)')

      serverOnPort(port)
      break
    }

    case '-c': {
      const host = process.argv[i+1]
      if (host == null) throw Error('Missing host to connect to! (usage -c <HOST> <PORT>')
      const port = +process.argv[i+2]
      if (port === 0 || isNaN(port)) throw Error('Invalid port (usage -c <HOST> <PORT>)')

      connect(host, port)
      console.log('connect', host, port)
    }
  }
  // console.log(process.argv[i])
}

const r = repl.start({
  prompt: '> ',
  useColors: true,
  terminal: true,
  // completer: true,

})
r.context.db = db
r.context.ss = ss

r.context.i = (val: Primitive) => {
  const version = agent()
  const lv = ss.localInsert(db, version, val)
  console.log(`Inserted ${version[0]}/${version[1]} (LV ${lv})`, val)

  triggerListeners()
}

r.context.s = (key: LV, val: Primitive) => {
  const version = agent()
  const lv = ss.localSet(db, version, key, val)
  console.log(`Set ${version[0]}/${version[1]} (LV ${lv})`, val)

  triggerListeners()
}

r.once('exit', () => {
  process.exit(0)
})