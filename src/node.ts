import {default as net} from 'net'
import { Socket } from 'net'
// import * as dt from './fancydb'
import * as ss from './fancydb/stateset'
import * as causalGraph from './fancydb/causal-graph'
import handle from './jsonlines'
import { LV, Primitive, RawVersion, ROOT_LV, VersionSummary } from './types'
import { createAgent, rateLimit } from './utils'
import { finished } from 'stream'
import repl from 'node:repl'
import fs from 'node:fs'

let agent = createAgent()

let db = ss.create()

let filename: string | null = null
const loadFromFile = (f: string) => {
  try {
    const dataStr = fs.readFileSync(f, 'utf-8')
    const data = JSON.parse(dataStr)
    ss.mergeDelta(db, data)
    console.log('Loaded from', f)
  } catch (e: any) {
    if (e.code !== 'ENOENT') throw e

    console.log('Using new database file')
  }

  filename = f
}

const save = rateLimit(100, () => {
  if (filename != null) {
    console.log('Saving to', filename)
    const data = ss.deltaSince(db, [])
    const dataStr = JSON.stringify(data) + '\n'
    fs.writeFileSync(filename, dataStr)
  }
})

process.on('exit', () => {
  save.flushSync()
})

process.on('SIGINT', () => {
  // Catching this to make sure we save!
  // console.log('SIGINT!')
  process.exit(1)
})

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
const dbDidChange = () => {
  console.log('BROADCAST')
  console.dir(db.values, {depth:null})
  for (const l of dbListeners) l()
  save()
}

const runProtocol = (sock: Socket) => {
  type ProtocolState = {state: 'waitingForVersion'}
    | {
      state: 'established',
      remoteVersion: LV[],
      unknownVersions: VersionSummary | null
    }

  let state: ProtocolState = {state: 'waitingForVersion'}

  const sendDelta = (sinceVersion: LV[]) => {
    console.log('sending delta to', sock.remoteAddress, sock.remotePort, 'since', sinceVersion)
    const delta = ss.deltaSince(db, sinceVersion)
    handler.write(['delta', delta])
    // remoteVersion = db.cg.version.slice()
  }

  const onVersionChanged = () => {
    console.log('onVersionChanged')
    if (state.state !== 'established') throw Error('Unexpected connection state')

    if (state.unknownVersions != null) {
      // The db might now include part of the remainder. Doing this works
      // around a bug where connecting to 2 computers will result in
      // re-sending known changes back to them.
      // console.log('unknown', state.unknownVersions)
      ;[state.remoteVersion, state.unknownVersions] = causalGraph.intersectWithSummary(
        db.cg, state.unknownVersions, state.remoteVersion
      )
      // console.log('->known', state.unknownVersions)
    }

    if (!causalGraph.lvEq(state.remoteVersion, db.cg.version)) {
      sendDelta(state.remoteVersion)
      // The version will always (& only) advance forward.
      state.remoteVersion = db.cg.version.slice()
    }
  }

  finished(sock, () => {
    console.log('Socket closed', sock.remoteAddress, sock.remotePort)
    dbListeners.delete(onVersionChanged)
  })

  const handler = handle<Msg, Msg>(sock, msg => {
    console.log('msg from', sock.remoteAddress, sock.remotePort)
    console.dir(msg, {depth:null})

    const type = msg[0]
    switch (type) {
      case 'known version': {
        if (state.state !== 'waitingForVersion') throw Error('Unexpected connection state')

        // When we get the known version, we always send a delta so the remote
        // knows they're up to date (even if they were already anyway).
        const summary = msg[1]
        const [sv, remainder] = causalGraph.intersectWithSummary(db.cg, summary)
        console.log('known version', sv)
        if (!causalGraph.lvEq(sv, db.cg.version)) {
          // We could always send the delta here to let the remote peer know they're
          // up to date, but they can figure that out by looking at the known version
          // we send on first connect.
          sendDelta(sv)
        }

        state = {
          state: 'established',
          remoteVersion: sv,
          unknownVersions: remainder
        }

        dbListeners.add(onVersionChanged) // Only matters the first time.
        break
      }

      case 'delta': {
        if (state.state !== 'established') throw Error('Invalid state')

        const delta = msg[1]
        // console.log('got delta')
        // console.dir(delta, {depth:null})
        const updated = ss.mergeDelta(db, delta)
        // console.log('Merged data')
        // console.dir(db.values, {depth:null})

        // console.log('== v', remoteVersion, delta.cg)
        state.remoteVersion = causalGraph.advanceVersionFromSerialized(
          db.cg, delta.cg, state.remoteVersion
        )
        // TODO: Ideally, this shouldn't be necessary! But it is because the remoteVersion
        // gets updated as a result of versions *we* send.
        state.remoteVersion = causalGraph.findDominators(db.cg, state.remoteVersion)

        // Presumably the remote peer has just sent us all the data it has that we were
        // missing. I could call intersectWithSummary2 here, but this should be
        // sufficient.
        state.unknownVersions = null

        if (updated[0] !== updated[1]) dbDidChange()
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

// ***** Command line argument passing
for (let i = 2; i < process.argv.length; i++) {
  const command = process.argv[i]
  switch (command) {
    case '-l': {
      const port = +process.argv[++i]
      if (port === 0 || isNaN(port)) throw Error('Invalid port (usage -l <PORT>)')

      serverOnPort(port)
      break
    }

    case '-c': {
      const host = process.argv[++i]
      if (host == null) throw Error('Missing host to connect to! (usage -c <HOST> <PORT>')
      const port = +process.argv[++i]
      if (port === 0 || isNaN(port)) throw Error('Invalid port (usage -c <HOST> <PORT>)')

      connect(host, port)
      console.log('connect', host, port)
      break
    }

    case '-f': {
      const f = process.argv[++i]
      loadFromFile(f)

      break
    }

    default: {
      throw Error(`Unknown command line argument '${command}'`)
    }
  }
  // console.log(process.argv[i])
}

// ***** REPL
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

  dbDidChange()
}

r.context.s = (key: LV, val: Primitive) => {
  const version = agent()
  const lv = ss.localSet(db, version, key, val)
  console.log(`Set ${version[0]}/${version[1]} (LV ${lv})`, val)

  dbDidChange()
}

r.once('exit', () => {
  process.exit(0)
})