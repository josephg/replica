import {default as net} from 'net'
import { Socket } from 'net'
import * as dt from './fancydb'
import * as ss from './fancydb/stateset'
import * as causalGraph from './fancydb/causal-graph'
import handle from './jsonlines'
import { LV, Primitive, RawVersion, ROOT_LV, VersionSummary } from './types'
import { AgentGenerator, createAgent, rateLimit } from './utils'
import { finished } from 'stream'
import repl from 'node:repl'
import fs from 'node:fs'


type InboxEntry = {
  v: RawVersion[],
  // type: string,
}

let inboxAgent = createAgent()

const inbox = ss.create<InboxEntry>()
const docs = new Map<LV, {
  agent: AgentGenerator,
  doc: dt.FancyDB
}>()

type FileData = {
  inbox: ss.RemoteStateDelta,
  docs: [LV, dt.SerializedFancyDBv1][]
}

let filename: string | null = null
const loadFromFile = (f: string) => {
  try {
    const dataStr = fs.readFileSync(f, 'utf-8')
    const data = JSON.parse(dataStr) as FileData

    ss.mergeDelta(inbox, data.inbox)
    for (const [key, snap] of data.docs) {
      docs.set(key, {
        agent: createAgent(),
        doc: dt.fromSerialized(snap)
      })
    }

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
    const data: FileData = {
      inbox: ss.deltaSince(inbox, []),
      docs: Array.from(docs.entries()).map(([lv, {doc}]) => [lv, dt.serialize(doc)])
    }
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
  type: 'known idx version',
  vs: VersionSummary
] | [
  type: 'idx delta',
  delta: ss.RemoteStateDelta
] | [
  // Get the changes to a document since the named version.
  type: 'get doc',
  k: RawVersion,
  since: VersionSummary, // OPT: Could probably just send the version here most of the time.
] | [
  type: 'doc delta',
  k: RawVersion,
  delta: dt.PSerializedFancyDBv1
] | [
  // Unused!
  type: 'ack',
  v: RawVersion[]
]

const dbListeners = new Set<() => void>()
const indexDidChange = () => {
  console.log('BROADCAST')
  console.dir(inbox.values, {depth:null})
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
    const delta = ss.deltaSince(inbox, sinceVersion)
    handler.write(['idx delta', delta])
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
        inbox.cg, state.unknownVersions, state.remoteVersion
      )
      // console.log('->known', state.unknownVersions)
    }

    if (!causalGraph.lvEq(state.remoteVersion, inbox.cg.version)) {
      sendDelta(state.remoteVersion)
      // The version will always (& only) advance forward.
      state.remoteVersion = inbox.cg.version.slice()
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
      case 'known idx version': {
        if (state.state !== 'waitingForVersion') throw Error('Unexpected connection state')

        // When we get the known idx version, we always send a delta so the remote
        // knows they're up to date (even if they were already anyway).
        const summary = msg[1]
        const [sv, remainder] = causalGraph.intersectWithSummary(inbox.cg, summary)
        console.log('known idx version', sv)
        if (!causalGraph.lvEq(sv, inbox.cg.version)) {
          // We could always send the delta here to let the remote peer know they're
          // up to date, but they can figure that out by looking at the known idx version
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

      case 'idx delta': {
        if (state.state !== 'established') throw Error('Invalid state')

        const delta = msg[1]
        // console.log('got delta')
        // console.dir(delta, {depth:null})
        const updated = ss.mergeDelta(inbox, delta)
        // console.log('Merged data')
        // console.dir(db.values, {depth:null})

        // console.log('== v', remoteVersion, delta.cg)
        state.remoteVersion = causalGraph.advanceVersionFromSerialized(
          inbox.cg, delta.cg, state.remoteVersion
        )
        // TODO: Ideally, this shouldn't be necessary! But it is because the remoteVersion
        // gets updated as a result of versions *we* send.
        state.remoteVersion = causalGraph.findDominators(inbox.cg, state.remoteVersion)

        // Presumably the remote peer has just sent us all the data it has that we were
        // missing. I could call intersectWithSummary2 here, but this should be
        // sufficient.
        state.unknownVersions = null

        if (updated[0] !== updated[1]) {
          const keys = ss.modifiedKeysSince(inbox, updated[0])
          for (const k of keys) {
            // NOTE: Version here might have duplicate entries!
            // const version = ss.get(inbox, k).flatMap(data => data.v)

            const doc = docs.get(k)
            const kRaw = causalGraph.lvToRaw(inbox.cg, k)
            const vs = doc ? causalGraph.summarizeVersion(doc.doc.cg) : {}
            console.log('Requesting updated info on doc', k, kRaw, 'since', vs)
            handler.write(['get doc', kRaw, vs])
          }

          // Uhhh should this wait until we've got the requested changes?
          indexDidChange()
        }
        // TODO: Send ack??
        break
      }

      case 'get doc': {
        // Get the changes to the given document at some point in time
        const kRaw = msg[1]
        const vs = msg[2]

        const k = causalGraph.rawToLV2(inbox.cg, kRaw)
        const doc = docs.get(k)
        if (doc == null) throw Error('Requested unknown doc??')

        const commonVersion = causalGraph.intersectWithSummary(doc.doc.cg, vs)[0]
        const partial = dt.serializePartialSince(doc.doc, commonVersion)

        handler.write(['doc delta', kRaw, partial])

        break
      }

      case 'doc delta': {
        // Merge the changes to the specified doc
        const kRaw = msg[1]
        const partial = msg[2]

        const k = causalGraph.rawToLV2(inbox.cg, kRaw)

        let doc = docs.get(k)
        if (doc == null) {
          doc = {
            agent: createAgent(),
            doc: dt.createDb()
          }
          docs.set(k, doc)
        }

        dt.mergePartialSerialized(doc.doc, partial)
        console.log('doc', k, 'now has value', dt.get(doc.doc))
      }

      default: console.warn('Unknown message type:', type)
    }
  })

  handler.write(['known idx version', causalGraph.summarizeVersion(inbox.cg)])
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
r.context.inbox = inbox
r.context.docs = docs
r.context.ss = ss
r.context.dt = dt

r.context.getDoc = (key: LV) => {
  const doc = docs.get(key)
  if (doc == null) throw Error('Missing doc')
  return dt.get(doc.doc)
}

// r.context.i = (val: Primitive) => {
//   const version = agent()
//   const lv = ss.localInsert(inbox, version, val)
//   console.log(`Inserted ${version[0]}/${version[1]} (LV ${lv})`, val)

//   dbDidChange()
// }

r.context.i = (data: Primitive) => {
  // We'll reuse the version for the document name. It shows up as a key in
  // the inbox as well.
  const docKey = inboxAgent()

  const doc = dt.createDb()
  const docAgent = createAgent()
  dt.recursivelySetRoot(doc, docAgent, {
    type: 'unknown',
    data,
  })

  const lv = ss.localInsert(inbox, docKey, {
    v: causalGraph.getRawVersion(doc.cg)
  })

  docs.set(lv, {
    agent: docAgent,
    doc
  })

  // console.dir(doc, {depth:null})

  console.log(`Inserted ${docKey[0]}/${docKey[1]} (LV ${lv})`, data)

  indexDidChange()
}

r.context.s = (docKey: LV, val: Primitive) => {
  const doc = docs.get(docKey)
  if (doc == null) throw Error('Missing or invalid key')

  dt.recursivelySetRoot(doc.doc, doc.agent, {data: val})
  console.log(dt.get(doc.doc))

  const version = inboxAgent()
  const lv = ss.localSet(inbox, version, docKey, {
    v: causalGraph.getRawVersion(doc.doc.cg)
  })

  console.log(`Set ${docKey} data`, val)
  indexDidChange()
}

r.context.get = (docKey: LV) => dt.get(docs.get(docKey)!.doc)

r.context.print = () => {
  for (const [k, doc] of docs.entries()) {
    console.log(k, ':', dt.get(doc.doc))
  }
}

r.once('exit', () => {
  process.exit(0)
})

// r.context.i({oh: 'hai'})