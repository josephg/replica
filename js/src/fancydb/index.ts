import assert from "assert/strict"
import Map2 from 'map2'
import { AtLeast1, CreateValue, DBSnapshot, DBValue, LV, LVRange, Operation, Primitive, RawVersion, ROOT, ROOT_LV, SnapCRDTInfo, SnapRegisterValue } from '../types'
import { AgentGenerator } from "../utils"
import * as causalGraph from './causal-graph.js'
import { CausalGraph } from "./causal-graph.js"

type RegisterValue = {type: 'primitive', val: Primitive}
  | {type: 'crdt', id: LV}

type MVRegister = AtLeast1<[LV, RegisterValue]>


type CRDTMapInfo = { type: 'map', registers: {[k: string]: MVRegister} }
type CRDTSetInfo = { type: 'set', values: Map<LV, RegisterValue> }
type CRDTRegisterInfo = { type: 'register', value: MVRegister }

type CRDTInfo = CRDTMapInfo | CRDTSetInfo | CRDTRegisterInfo
// } | {
//   type: 'stateset',
//   values: Map<LV, Primitive>
// }

export interface FancyDB {
  crdts: Map<LV, CRDTInfo>,
  cg: CausalGraph,
  onop?: (db: FancyDB, op: Operation) => void
}

export function createDb(): FancyDB {
  const db: FancyDB = {
    crdts: new Map(),
    cg: causalGraph.create(),
  }

  db.crdts.set(ROOT_LV, {
    type: "map",
    registers: {}
  })

  return db
}


function removeRecursive(db: FancyDB, value: RegisterValue) {
  if (value.type !== 'crdt') return

  const crdt = db.crdts.get(value.id)
  if (crdt == null) return

  switch (crdt.type) {
    case 'map':
      for (const k in crdt.registers) {
        const reg = crdt.registers[k]
        for (const [version, value] of reg) {
          removeRecursive(db, value)
        }
      }
      break
    case 'register':
      for (const [version, value] of crdt.value) {
        removeRecursive(db, value)
      }
      break
    case 'set':
      for (const [id, value] of crdt.values) {
        removeRecursive(db, value)
      }
      break
    // case 'stateset':
    //   throw Error('Cannot remove from a stateset')
      
    default: throw Error('Unknown CRDT type!?')
  }

  db.crdts.delete(value.id)
}

const errExpr = (str: string): never => { throw Error(str) }

function createCRDT(db: FancyDB, id: LV, type: 'map' | 'set' | 'register' | 'stateset') {
  if (db.crdts.has(id)) {
    throw Error('CRDT already exists !?')
  }

  const crdtInfo: CRDTInfo = type === 'map' ? {
    type: "map",
    registers: {},
  } : type === 'register' ? {
    type: 'register',
    // Registers default to NULL when created.
    value: [[id, {type: 'primitive', val: null}]],
  } : type === 'set' ? {
    type: 'set',
    values: new Map,
  // } : type === 'stateset' ? {
  //   type: 'stateset',
  //   values: new Map,
  } : errExpr('Invalid CRDT type')

  db.crdts.set(id, crdtInfo)
}

function mergeRegister(db: FancyDB, globalParents: LV[], oldPairs: MVRegister, localParents: LV[], newVersion: LV, newVal: CreateValue): MVRegister {
  let newValue: RegisterValue
  if (newVal.type === 'primitive') {
    newValue = newVal
  } else {
    // Create it.
    createCRDT(db, newVersion, newVal.crdtKind)
    newValue = {type: "crdt", id: newVersion}
  }

  const newPairs: MVRegister = [[newVersion, newValue]]
  for (const [version, value] of oldPairs) {
    // Each item is either retained or removed.
    if (localParents.some(v2 => version === v2)) {
      // The item was named in parents. Remove it.
      // console.log('removing', value)
      removeRecursive(db, value)
    } else {
      // We're intending to retain this operation because its not explicitly
      // named, but that only makes sense if the retained version is concurrent
      // with the new version.
      if (causalGraph.versionContainsTime(db.cg, globalParents, version)) {
        throw Error('Invalid local parents in operation')
      }

      newPairs.push([version, value])
    }
  }

  // Note we're sorting by *local version* here. This doesn't sort by LWW
  // priority. Could do - currently I'm figuring out the priority in the
  // get() method.
  newPairs.sort(([v1], [v2]) => v1 - v2)

  return newPairs
}

export function applyRemoteOp(db: FancyDB, op: Operation): LV {
  const newVersion = causalGraph.addRaw(db.cg, op.id, 1, op.globalParents)
  if (newVersion < 0) {
    // The operation is already known.
    console.warn('Operation already applied', op.id)
    return newVersion
  }

  const globalParents = causalGraph.rawToLVList(db.cg, op.globalParents)

  const crdtLV = causalGraph.rawToLV2(db.cg, op.crdtId)

  const crdt = db.crdts.get(crdtLV)
  if (crdt == null) {
    console.warn('CRDT has been deleted..')
    return newVersion
  }

  // Every register operation creates a new value, and removes 0-n other values.
  switch (op.action.type) {
    case 'registerSet': {
      if (crdt.type !== 'register') throw Error('Invalid operation type for target')
      const localParents = causalGraph.rawToLVList(db.cg, op.action.localParents)
      const newPairs = mergeRegister(db, globalParents, crdt.value, localParents, newVersion, op.action.val)

      crdt.value = newPairs
      break
    }
    case 'map': {
      if (crdt.type !== 'map') throw Error('Invalid operation type for target')

      const oldPairs = crdt.registers[op.action.key] ?? []
      const localParents = causalGraph.rawToLVList(db.cg, op.action.localParents)

      const newPairs = mergeRegister(db, globalParents, oldPairs, localParents, newVersion, op.action.val)

      crdt.registers[op.action.key] = newPairs
      break
    }
    case 'setInsert': case 'setDelete': { // Sets!
      if (crdt.type !== 'set') throw Error('Invalid operation type for target')

      // Set operations are comparatively much simpler, because insert
      // operations cannot be concurrent and multiple overlapping delete
      // operations are ignored.
      if (op.action.type == 'setInsert') {
        if (op.action.val.type === 'primitive') {
          crdt.values.set(newVersion, op.action.val)
        } else {
          createCRDT(db, newVersion, op.action.val.crdtKind)
          crdt.values.set(newVersion, {type: "crdt", id: newVersion})
        }
      } else {
        // Delete!
        const target = causalGraph.rawToLV2(db.cg, op.action.target)
        let oldVal = crdt.values.get(target)
        if (oldVal != null) {
          removeRecursive(db, oldVal)
          crdt.values.delete(target)
        }
      }

      break
    }

    default: throw Error('Invalid action type')
  }

  db.onop?.(db, op)
  return newVersion
}


const getMap = (db: FancyDB, mapId: LV): CRDTMapInfo => {
  const crdt = db.crdts.get(mapId)
  if (crdt == null || crdt.type !== 'map') throw Error('Invalid CRDT')
  return crdt
}

export function localMapInsert(db: FancyDB, id: RawVersion, mapId: LV, key: string, val: CreateValue): [Operation, LV] {
  const crdt = getMap(db, mapId)

  const crdtId = causalGraph.lvToRaw(db.cg, mapId)

  const localParentsLV = (crdt.registers[key] ?? []).map(([version]) => version)
  const localParents = causalGraph.lvToRawList(db.cg, localParentsLV)
  const op: Operation = {
    id,
    crdtId,
    globalParents: causalGraph.lvToRawList(db.cg, db.cg.version),
    action: { type: 'map', localParents, key, val }
  }

  // TODO: Could easily inline this - which would mean more code but higher performance.
  const v = applyRemoteOp(db, op)
  return [op, v]
}




/** Recursively set / insert values into the map to make the map resemble the input */
export function recursivelySetMap(db: FancyDB, agent: AgentGenerator, mapId: LV, val: Record<string, Primitive>) {
  // The root value already exists. Recursively insert / replace child items.
  const crdt = getMap(db, mapId)

  for (const k in val) {
    const v = val[k]
    // console.log(k, v)
    if (v === null || typeof v !== 'object') {
      // Set primitive into register.
      // This is a bit inefficient - it re-queries the CRDT and whatnot.
      // console.log('localMapInsert', v)
      localMapInsert(db, agent(), mapId, k, {type: 'primitive', val: v})
    } else {
      if (Array.isArray(v)) throw Error('Arrays not supported') // Could just move this up for now.

      // Or we have a recursive object merge.
      const inner = crdt.registers[k]

      // Force the inner item to become a map. Rawr.
      let innerMapId
      const setToMap = () => (
        localMapInsert(db, agent(), mapId, k, {type: "crdt", crdtKind: 'map'})[1]
      )

      if (inner == null) innerMapId = setToMap()
      else {
        const activePair = causalGraph.tieBreakRegisters(db.cg, inner)

        if (activePair.type !== 'crdt') {
          innerMapId = setToMap()
        } else {
          const innerId = activePair.id
          const innerInfo = db.crdts.get(innerId)!
          if (innerInfo.type !== 'map') innerMapId = setToMap()
          else innerMapId = innerId
        }
      }

      // console.log('recursivelySetMap', innerMapId, v)
      recursivelySetMap(db, agent, innerMapId, v)
    }
  }
}

export function recursivelySetRoot(db: FancyDB, agent: AgentGenerator, val: Record<string, Primitive>) {
  // The root value already exists. Recursively insert / replace child items.
  recursivelySetMap(db, agent, ROOT_LV, val)
}




const registerToVal = (db: FancyDB, r: RegisterValue): DBValue => (
  (r.type === 'primitive')
    ? r.val
    : get(db, r.id) // Recurse!
)

export function get(db: FancyDB): {[k: string]: DBValue};
export function get(db: FancyDB, crdtId: LV): DBValue;
export function get(db: FancyDB, crdtId: LV = ROOT_LV): DBValue {
  const crdt = db.crdts.get(crdtId)
  if (crdt == null) { return null }

  switch (crdt.type) {
    case 'register': {
      // When there's a tie, the active value is based on the order in pairs.
      const activePair = causalGraph.tieBreakRegisters(db.cg, crdt.value)
      return registerToVal(db, activePair)
    }
    case 'map': {
      const result: {[k: string]: DBValue} = {}
      for (const k in crdt.registers) {
        const activePair = causalGraph.tieBreakRegisters(db.cg, crdt.registers[k])
        result[k] = registerToVal(db, activePair)
      }
      return result
    }
    case 'set': {
      const result = new Map2<string, number, DBValue>()

      for (const [version, value] of crdt.values) {
        const rawVersion = causalGraph.lvToRaw(db.cg, version)
        result.set(rawVersion[0], rawVersion[1], registerToVal(db, value))
      }

      return result
    }
    default: throw Error('Invalid CRDT type in DB')
  }
}

const isObj = (x: Primitive): x is Record<string, Primitive> => x != null && typeof x === 'object'
export function getPath(db: FancyDB, path: (string | number)[], base: LV = ROOT_LV): RegisterValue {
  let idx = 0

  let container: RegisterValue = {type: 'crdt', id: base}

  while (idx < path.length) {
    // ... Though if the value was an object, we really could!
    const p = path[idx]

    // if (container.type === 'primitive' && container.val != null && typeof container.val === 'object') {
    if (container.type === 'primitive') {
      if (isObj(container.val) && typeof p === 'string') {
        container = {
          type: 'primitive',
          val: container.val[p]
        }
      } else {
        throw Error('Cannot descend into primitive object')
      }
    } else {
      const crdt = db.crdts.get(container.id)
      if (crdt == null) throw Error('Cannot descend into CRDT')

      if (crdt.type === 'map' && typeof p === 'string') {
        const register = crdt.registers[p]
        container = causalGraph.tieBreakRegisters(db.cg, register)
        idx++
      } else if (crdt.type === 'set' && typeof p === 'number') {
        container = crdt.values.get(p) ?? errExpr('Missing set value')
        idx++
      } else if (crdt.type === 'register') {
        container = causalGraph.tieBreakRegisters(db.cg, crdt.value)
      } else {
        throw Error(`Cannot descend into ${crdt.type} with path '${p}'`)
      }
    }
  }

  return container
}

// *** Snapshot methods ***
const registerValToJSON = (db: FancyDB, val: RegisterValue): SnapRegisterValue => (
  val.type === 'crdt' ? {
    type: 'crdt',
    id: causalGraph.lvToRaw(db.cg, val.id)
  } : val
)

const mvRegisterToJSON = (db: FancyDB, val: MVRegister): [RawVersion, SnapRegisterValue][] => (
  val.map(([lv, val]) => {
    const v = causalGraph.lvToRaw(db.cg, lv)
    const mappedVal: SnapRegisterValue = registerValToJSON(db, val)
    return [v, mappedVal]
  })
)

/** Used for interoperability with SimpleDB */
export function toSnapshot(db: FancyDB): DBSnapshot {
  return {
    version: causalGraph.lvToRawList(db.cg, db.cg.version),
    crdts: Array.from(db.crdts.entries()).map(([lv, rawInfo]) => {
      const [agent, seq] = causalGraph.lvToRaw(db.cg, lv)
      const mappedInfo: SnapCRDTInfo = rawInfo.type === 'set' ? {
        type: rawInfo.type,
        values: Array.from(rawInfo.values).map(([lv, val]) => {
          const [agent, seq] = causalGraph.lvToRaw(db.cg, lv)
          return [agent, seq, registerValToJSON(db, val)]
        })
      } : rawInfo.type === 'map' ? {
        type: rawInfo.type,
        registers: Object.fromEntries(Object.entries(rawInfo.registers)
          .map(([k, val]) => ([k, mvRegisterToJSON(db, val)])))
      } : rawInfo.type === 'register' ? {
        type: rawInfo.type,
        value: mvRegisterToJSON(db, rawInfo.value)
      } : errExpr('Unknown CRDT type') // Never.
      return [agent, seq, mappedInfo]
    })
  }
}

// *** Serialization ***

type SerializedRegisterValue = [type: 'primitive', val: Primitive]
  | [type: 'crdt', id: LV]

type SerializedMVRegister = [LV, SerializedRegisterValue][]

type SerializedCRDTInfo = [
  type: 'map',
  registers: [k: string, reg: SerializedMVRegister][],
] | [
  type: 'set',
  values: [LV, SerializedRegisterValue][],
] | [
  type: 'register',
  value: SerializedMVRegister,
]

export interface SerializedFancyDBv1 {
  crdts: [LV, SerializedCRDTInfo][]
  cg: causalGraph.SerializedCausalGraphV1,
}


const serializeRegisterValue = (r: RegisterValue): SerializedRegisterValue => (
  r.type === 'crdt' ? [r.type, r.id]
    : [r.type, r.val]
)
const serializeMV = (r: MVRegister): SerializedMVRegister => (
  r.map(([v, r]) => [v, serializeRegisterValue(r)])
)

const serializeCRDTInfo = (info: CRDTInfo): SerializedCRDTInfo => (
  info.type === 'map' ? [
    'map', Object.entries(info.registers).map(([k, v]) => ([k, serializeMV(v)]))
  ] : info.type === 'set' ? [
    'set', Array.from(info.values).map(([id, v]) => [id, serializeRegisterValue(v)])
  ] : info.type === 'register' ? [
    'register', serializeMV(info.value)
  ] : errExpr('Unknown CRDT type')
)

export function serialize(db: FancyDB): SerializedFancyDBv1 {
  return {
    cg: causalGraph.serialize(db.cg),
    crdts: Array.from(db.crdts).map(([lv, info]) => ([
      lv, serializeCRDTInfo(info)
    ]))
  }
}



const deserializeRegisterValue = (data: SerializedRegisterValue): RegisterValue => (
  data[0] === 'crdt' ? {type: 'crdt', id: data[1]}
    : {type: 'primitive', val: data[1]}
)
const deserializeMV = (r: SerializedMVRegister): MVRegister => {
  const result: [LV, RegisterValue][] = r.map(([v, r]) => [v, deserializeRegisterValue(r)])
  if (result.length === 0) throw Error('Invalid MV register')
  return result as MVRegister
}

const deserializeCRDTInfo = (data: SerializedCRDTInfo): CRDTInfo => {
  const type = data[0]
  return type === 'map' ? {
    type: 'map',
    registers: Object.fromEntries(data[1].map(([k, r]) => ([k, deserializeMV(r)])))
  } : type === 'register' ? {
    type: 'register',
    value: deserializeMV(data[1])
  } : type === 'set' ? {
    type: 'set',
    values: new Map(data[1].map(([k, v]) => ([k, deserializeRegisterValue(v)])))
  } : errExpr('Invalid or unknown type: ' + type)
}

export function fromSerialized(data: SerializedFancyDBv1): FancyDB {
  return {
    cg: causalGraph.fromSerialized(data.cg),
    crdts: new Map(data.crdts
      .map(([id, crdtData]) => [id, deserializeCRDTInfo(crdtData)]))
  }
}

/** Partial serialization */

type PSerializedRegisterValue = [type: 'primitive', val: Primitive]
  | [type: 'crdt', agent: string, seq: number]

type PSerializedMVRegister = [agent: string, seq: number, val: PSerializedRegisterValue][]

type PSerializedCRDTInfo = [
  type: 'map',
  registers: [k: string, reg: PSerializedMVRegister][],
] | [
  type: 'set',
  values: [agent: string, seq: number, val: PSerializedRegisterValue][],
] | [
  type: 'register',
  value: PSerializedMVRegister,
]

export interface PSerializedFancyDBv1 {
  cg: causalGraph.PartialSerializedCGV1,
  crdts: [agent: string, seq: number, info: PSerializedCRDTInfo][]
}


const deserializePRegisterValue = (data: PSerializedRegisterValue, cg: CausalGraph): RegisterValue => (
  data[0] === 'crdt' ? {type: 'crdt', id: causalGraph.rawToLV(cg, data[1], data[2])}
    : {type: 'primitive', val: data[1]}
)

const serializePRegisterValue = (data: RegisterValue, cg: CausalGraph): PSerializedRegisterValue => {
  if (data.type === 'crdt') {
    const rv = causalGraph.lvToRaw(cg, data.id)
    return ['crdt', rv[0], rv[1]]
  } else {
    return ['primitive', data.val]
  }
}

const serializePMVRegisterValue = (v: LV, val: RegisterValue, cg: CausalGraph): [agent: string, seq: number, val: PSerializedRegisterValue] => {
  const rv = causalGraph.lvToRaw(cg, v)
  return [rv[0], rv[1], serializePRegisterValue(val, cg)]
}

const mergePartialRegister = (reg: MVRegister, givenRawPairs: PSerializedMVRegister, cg: CausalGraph) => {
  // This function mirrors mergeSet in stateset code.
  const oldVersions = reg.map(([v]) => v)
  const newVersions = givenRawPairs.map(([agent, seq]) => causalGraph.rawToLV(cg, agent, seq))

  // Throw them in a blender...

  let needsSort = false
  causalGraph.findDominators2(cg, [...oldVersions, ...newVersions], (v, isDominator) => {
    // 3 cases: v is in old, v is in new, or v is in both.
    if (isDominator && !oldVersions.includes(v)) {
      // Its in the new data set only. Copy it in.
      const idx = newVersions.indexOf(v)
      if (idx < 0) throw Error('Invalid state')
      reg.push([v, deserializePRegisterValue(givenRawPairs[idx][2], cg)])

      needsSort = true
    } else if (!isDominator && !newVersions.includes(v)) {
      // The item is in old only, and its been superceded. Remove it!
      const idx = reg.findIndex(([v2]) => v2 == v)
      if (idx < 0) throw Error('Invalid state')
      reg.splice(idx, 1)
    }
  })

  // Matching the sort in mergeRegister. Not sure if this is necessary.
  if (needsSort && reg.length > 1) reg.sort(([v1], [v2]) => v1 - v2)
}

export function mergePartialSerialized(db: FancyDB, data: PSerializedFancyDBv1): LVRange {
  const updated = causalGraph.mergePartialVersions(db.cg, data.cg)
  for (const [agent, seq, newInfo] of data.crdts) {
    const id = causalGraph.rawToLV(db.cg, agent, seq)

    const type = newInfo[0]

    let existingInfo = db.crdts.get(id)
    if (existingInfo == null) {
      existingInfo = type === 'map' ? { type, registers: {} }
        : type === 'set' ? { type, values: new Map() }
        : { type, value: [] as any } // shhhhh don't worry about it. We'll fix it below.

      db.crdts.set(id, existingInfo)
    }

    if (existingInfo.type !== type) throw Error('Unexpected CRDT type in data')

    switch (type) {
      case 'map': {
        for (const [k, regInfo] of newInfo[1]) {
          const r = (existingInfo as CRDTMapInfo).registers[k]
          if (r == null) {
            // Uhhh we could call mergePartial but it hurts.
            (existingInfo as CRDTMapInfo).registers[k] = regInfo.map(
              ([a, s, v]) => [causalGraph.rawToLV(db.cg, a, s), deserializePRegisterValue(v, db.cg)]
            ) as AtLeast1<[LV, RegisterValue]>
          } else {
            mergePartialRegister(r, regInfo, db.cg)
          }
        }
        break
      }
      case 'set': {
        const values = (existingInfo as CRDTSetInfo).values
        for (const [agent, seq, value] of newInfo[1]) {
          const k = causalGraph.rawToLV(db.cg, agent, seq)
          // Set values are immutable, so if it exists, we've got it.
          if (!values.has(k)) {
            values.set(k, deserializePRegisterValue(value, db.cg))
          }
        }
        break
      }
      case 'register': {
        mergePartialRegister((existingInfo as CRDTRegisterInfo).value, newInfo[1], db.cg)
        break
      }
    }
  }

  return updated
}

export function serializePartialSince(db: FancyDB, v: LV[]): PSerializedFancyDBv1 {
  const cgData = causalGraph.serializeFromVersion(db.cg, v)
  const crdts: [agent: string, seq: number, info: PSerializedCRDTInfo][] = []

  const diff = causalGraph.diff(db.cg, v, db.cg.version).bOnly

  const shouldIncludeV = (v: LV): boolean => (
    // This could be implemented using a binary search, but given the sizes involved this is fine.
    diff.find(([start, end]) => (start <= v) && (v < end)) != null
  )

  const encodeMVRegister = (reg: MVRegister, includeAll: boolean): null | PSerializedMVRegister => {
    // I'll do this in an imperative way because its called so much.
    let result: null | PSerializedMVRegister = null
    for (const [v, val] of reg) {
      if (includeAll || shouldIncludeV(v)) {
        result ??= []
        result.push(serializePMVRegisterValue(v, val, db.cg))
      }
    }
    return result
  }

  // So this is SLOOOW for big documents. A better implementation would store
  // operations and do a whole thing sending partial operation logs.
  for (const [id, info] of db.crdts.entries()) {
    // If the CRDT was created recently, just include all of it.
    const includeAll = shouldIncludeV(id)

    let infoOut: PSerializedCRDTInfo | null = null
    switch (info.type) {
      case 'map': {
        let result: null | [k: string, reg: PSerializedMVRegister][] = null
        for (let k in info.registers) {
          const v = info.registers[k]

          const valHere = encodeMVRegister(v, includeAll)
          // console.log('valHere', valHere)
          if (valHere != null) {
            result ??= []
            result.push([k, valHere])
          }
        }
        if (result != null) infoOut = ['map', result]
        break
      }
      case 'register': {
        const result = encodeMVRegister(info.value, includeAll)
        if (result != null) infoOut = ['register', result]
        // if (result != null) {
          // const rv = causalGraph.lvToRaw(db.cg, id)
          // crdts.push([rv[0], rv[1], ['register', result]])
        // }

        break
      }

      case 'set': {
        // TODO: Weird - this looks almost identical to the register code!
        let result: null | [agent: string, seq: number, val: PSerializedRegisterValue][] = null
        for (const [k, val] of info.values.entries()) {
          if (includeAll || shouldIncludeV(k)) {
            result ??= []
            result.push(serializePMVRegisterValue(k, val, db.cg))
          }
        }
        if (result != null) infoOut = ['set', result]
        // if (result != null) {
        //   const rv = causalGraph.lvToRaw(db.cg, id)
        //   crdts.push([rv[0], rv[1], ['set', result]])
        // }
        break
      }
    }

    if (infoOut != null) {
      const rv = causalGraph.lvToRaw(db.cg, id)
      crdts.push([rv[0], rv[1], infoOut])
    }
  }

  return {
    cg: cgData,
    crdts
  }
}

;(() => {
  let db = createDb()

  localMapInsert(db, ['seph', 0], ROOT_LV, 'yo', {type: 'primitive', val: 123})
  assert.deepEqual(get(db), {yo: 123})

  // ****
  db = createDb()
  // concurrent changes
  applyRemoteOp(db, {
    id: ['mike', 0],
    globalParents: [],
    crdtId: ROOT,
    action: {type: 'map', localParents: [], key: 'c', val: {type: 'primitive', val: 'mike'}},
  })
  applyRemoteOp(db, {
    id: ['seph', 1],
    globalParents: [],
    crdtId: ROOT,
    action: {type: 'map', localParents: [], key: 'c', val: {type: 'primitive', val: 'seph'}},
  })

  assert.deepEqual(get(db), {c: 'seph'})

  applyRemoteOp(db, {
    id: ['mike', 1],
    // globalParents: [['mike', 0]],
    globalParents: [['mike', 0], ['seph', 1]],
    crdtId: ROOT,
    // action: {type: 'map', localParents: [['mike', 0]], key: 'yo', val: {type: 'primitive', val: 'both'}},
    action: {type: 'map', localParents: [['mike', 0], ['seph', 1]], key: 'c', val: {type: 'primitive', val: 'both'}},
  })
  // console.dir(db, {depth: null})
  assert.deepEqual(get(db), {c: 'both'})

  // ****
  db = createDb()
  // Set a value in an inner map
  const [_, inner] = localMapInsert(db, ['seph', 1], ROOT_LV, 'stuff', {type: 'crdt', crdtKind: 'map'})
  localMapInsert(db, ['seph', 2], inner, 'cool', {type: 'primitive', val: 'definitely'})
  assert.deepEqual(get(db), {stuff: {cool: 'definitely'}})



  const serialized = JSON.stringify(serialize(db))
  const deser = fromSerialized(JSON.parse(serialized))
  assert.deepEqual(db, deser)

  // console.dir(, {depth: null})



  // // Insert a set
  // const innerSet = localMapInsert(db, ['seph', 2], ROOT, 'a set', {type: 'crdt', crdtKind: 'set'})
  // localSetInsert(db, ['seph', 3], innerSet.id, {type: 'primitive', val: 'whoa'})
  // localSetInsert(db, ['seph', 4], innerSet.id, {type: 'crdt', crdtKind: 'map'})
  
  // console.log('db', get(db))
  // console.log('db', db)
  
  
  // assert.deepEqual(db, fromJSON(toJSON(db)))
})()


;(() => {
  let db = createDb()
  // console.log(serializePartialSince(db, []))

  localMapInsert(db, ['seph', 0], ROOT_LV, 'yo', {type: 'primitive', val: 123})
  assert.deepEqual(get(db), {yo: 123})

  const serializedA = serializePartialSince(db, [])
  // console.dir(serializePartialSince(db, []), {depth:null})

  // ****
  db = createDb()
  // concurrent changes
  applyRemoteOp(db, {
    id: ['mike', 0],
    globalParents: [],
    crdtId: ROOT,
    action: {type: 'map', localParents: [], key: 'c', val: {type: 'primitive', val: 'mike'}},
  })
  applyRemoteOp(db, {
    id: ['seph', 1],
    globalParents: [],
    crdtId: ROOT,
    action: {type: 'map', localParents: [], key: 'c', val: {type: 'primitive', val: 'seph'}},
  })

  assert.deepEqual(get(db), {c: 'seph'})

  // const serializedB = serializePartialSince(db, [])
  const serializedB1 = serializePartialSince(db, [0])
  const serializedB2 = serializePartialSince(db, [1])
  // console.dir(serializedB, {depth:null})

  const db2 = createDb()
  mergePartialSerialized(db2, serializedB2)
  mergePartialSerialized(db2, serializedB1)
  // console.dir(db, {depth:null})
  // console.dir(db2, {depth:null})
  assert.deepEqual(db, db2)
  assert.deepEqual(get(db), {c: 'seph'})


  // applyRemoteOp(db, {
  //   id: ['mike', 1],
  //   // globalParents: [['mike', 0]],
  //   globalParents: [['mike', 0], ['seph', 1]],
  //   crdtId: ROOT,
  //   // action: {type: 'map', localParents: [['mike', 0]], key: 'yo', val: {type: 'primitive', val: 'both'}},
  //   action: {type: 'map', localParents: [['mike', 0], ['seph', 1]], key: 'c', val: {type: 'primitive', val: 'both'}},
  // })
  // // console.dir(db, {depth: null})
  // assert.deepEqual(get(db), {c: 'both'})

  // // ****
  // db = createDb()
  // // Set a value in an inner map
  // const [_, inner] = localMapInsert(db, ['seph', 1], ROOT_LV, 'stuff', {type: 'crdt', crdtKind: 'map'})
  // localMapInsert(db, ['seph', 2], inner, 'cool', {type: 'primitive', val: 'definitely'})
  // assert.deepEqual(get(db), {stuff: {cool: 'definitely'}})



  // const serialized = JSON.stringify(serialize(db))
  // const deser = fromSerialized(JSON.parse(serialized))
  // assert.deepEqual(db, deser)

})()