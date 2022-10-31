import { AtLeast1, LV, LVRange, Primitive, RawVersion } from "../types"
import { CausalGraph } from "./causal-graph.js"
import * as causalGraph from './causal-graph.js'
import bs from 'binary-search'
import assert from 'assert/strict'
import { assertSorted, createAgent } from "../utils"

type Pair<T=Primitive> = [LV, T]
type RawPair<T=Primitive> = [RawVersion, T]

export interface StateSet<T=Primitive> {
  // ID -> [current value, current version] pairs.
  // NOTE: This is a MV register which only (always) stores primitive values.
  values: Map<LV, AtLeast1<Pair<T>>>,

  // This is an index to quickly find the items to send when syncing.
  // Each value exists in this list once for each version it has.
  index: {
    v: LV
    key: LV,
  }[],

  cg: CausalGraph,
}

export function create<T=Primitive>(): StateSet<T> {
  return {
    values: new Map(),
    index: [],
    cg: causalGraph.create(),
  }
}

function rawLookup<T>(crdt: StateSet<T>, v: LV): number {
  return bs(crdt.index, v, (entry, needle) => entry.v - needle)
}

function removeIndex(crdt: StateSet, v: LV) {
  const idx = rawLookup(crdt, v)
  if (idx < 0) throw Error('Missing old version in index')

  // Splice the entry out. The entry will usually be near the end, so this is
  // not crazy slow.
  crdt.index.splice(idx, 1)
}

function addIndex(crdt: StateSet, v: LV, key: LV) {
  const entry = {v, key}

  if (crdt.index.length == 0 || crdt.index[crdt.index.length - 1].v < v) {
    // Normal, fast case.
    crdt.index.push(entry)
  } else {
    const idx = rawLookup(crdt, v)
    if (idx >= 0) return // Already indexed.
    const insIdx = -idx - 1
    crdt.index.splice(insIdx, 0, entry)
  }
}

export function localSet<T>(crdt: StateSet<T>, version: RawVersion, key: LV | -1, value: T): LV {
  const lv = causalGraph.addRaw(crdt.cg, version)
  if (key == -1) key = lv

  const oldPairs = crdt.values.get(key)
  crdt.values.set(key, [[lv, value]])

  if (oldPairs != null) {
    for (const [v, oldValue] of oldPairs) {
      // Remove from index
      const idx = rawLookup(crdt, v)
      if (idx < 0) throw Error('Missing old version in index')

      // Splice the entry out. The entry will usually be near the end, so this is
      // not crazy slow.
      crdt.index.splice(idx, 1)
    }
  }

  crdt.index.push({v: lv, key})

  return lv
}

export function localInsert<T>(crdt: StateSet<T>, version: RawVersion, value: T): LV {
  return localSet(crdt, version, -1, value)
}

/** Get a list of the keys which have been modified in the range of [since..] */
export function modifiedKeysSince<T>(crdt: StateSet<T>, since: LV): LV[] {
  let idx = rawLookup(crdt, since)
  if (idx < 0) idx = -idx - 1

  const result = new Set<LV>() // To uniq() the results.
  for (; idx < crdt.index.length; idx++) {
    const {key} = crdt.index[idx]
    result.add(key)
  }
  return Array.from(result)
}

// *** Remote state ***
export type RemoteStateDelta = {
  cg: causalGraph.PartialSerializedCGV1,

  ops: [
    key: RawVersion,
    pairs: AtLeast1<RawPair>, // This could be optimized by referencing cg below.
  ][],
}

function mergeSet(crdt: StateSet, keyRaw: RawVersion, givenRawPairs: AtLeast1<RawPair>) {
  // const lv = causalGraph.addRaw(crdt.cg, version, 1, parents)

  const key = causalGraph.rawToLV2(crdt.cg, keyRaw)
  const pairs: Pair[] = crdt.values.get(key) ?? []

  const oldVersions = pairs.map(([v]) => v)
  const newVersions = givenRawPairs.map(([rv]) => causalGraph.rawToLV2(crdt.cg, rv))

  causalGraph.findDominators2(crdt.cg, [...oldVersions, ...newVersions], (v, isDominator) => {
    // There's 3 options here: Its in old, its in new, or its in both.
    if (isDominator && !oldVersions.includes(v)) {
      // Its in new only. Add it!
      addIndex(crdt, v, key)

      const idx = newVersions.indexOf(v)
      if (idx < 0) throw Error('Invalid state')
      pairs.push([v, givenRawPairs[idx][1]])

    } else if (!isDominator && !newVersions.includes(v)) {
      // The item is in old only, and its been superceded. Remove it!
      removeIndex(crdt, v)
      const idx = pairs.findIndex(([v2]) => v2 === v)
      if (idx < 0) throw Error('Invalid state')
      pairs.splice(idx, 1)
    }
  })

  if (pairs.length < 1) throw Error('Invalid pairs - all removed?')
  crdt.values.set(key, pairs as AtLeast1<Pair>)
}

export function mergeDelta(crdt: StateSet, delta: RemoteStateDelta): LVRange {
  // We need to merge the causal graph stuff first because the ops depend on it.
  const updated = causalGraph.mergePartialVersions(crdt.cg, delta.cg)

  for (const [key, pairs] of delta.ops) {
    mergeSet(crdt, key, pairs)
  }

  return updated
}

export function deltaSince(crdt: StateSet, v: LV[] = []): RemoteStateDelta {
  const cgDelta = causalGraph.serializeFromVersion(crdt.cg, v)

  // This calculation is duplicated in the serializeFromVersion call.
  const ranges = causalGraph.diff(crdt.cg, v, crdt.cg.version).bOnly

  const pairs = new Map<LV, Pair[]>()

  for (const [start, end] of ranges) {
    let idx = rawLookup(crdt, start)
    if (idx < 0) idx = -idx - 1

    for (; idx < crdt.index.length; idx++) {
      const {key, v} = crdt.index[idx]
      if (v >= end) break

      // I could just add the data to ops, but this way we make sure to
      // only include the pairs within the requested range.
      const pair = crdt.values.get(key)!.find(([v2]) => v2 === v)
      if (pair == null) throw Error('Invalid state!')

      let p = pairs.get(key) as Pair[] | undefined
      if (p == null) {
        p = []
        pairs.set(key, p)
      }
      p.push(pair)
    }
  }

  const ops: [
    key: RawVersion,
    pairs: AtLeast1<RawPair>, // This could be optimized by referencing cg below.
  ][] = Array.from(pairs.entries()).map(([key, p]) => [
    causalGraph.lvToRaw(crdt.cg, key),
    p.map(([v, val]): RawPair => [causalGraph.lvToRaw(crdt.cg, v), val]) as AtLeast1<RawPair>
  ])

  return { cg: cgDelta, ops }
}


function lookupIndex(crdt: StateSet, v: LV): LV | null {
  const result = rawLookup(crdt, v)

  return result < 0 ? null
    : crdt.index[result].key
}

function check(crdt: StateSet) {
  let expectedIdxSize = 0

  for (const [key, pairs] of crdt.values.entries()) {
    assert(pairs.length >= 1)

    if (pairs.length >= 2) {
      const version = pairs.map(([v]) => v)

      // Check that all the versions are concurrent with each other.
      const dominators = causalGraph.findDominators(crdt.cg, version)
      assert.equal(version.length, dominators.length)

      assertSorted(version)
    }

    expectedIdxSize += pairs.length

    // Each entry should show up in the index.
    for (const [vv] of pairs) {
      assert.equal(key, lookupIndex(crdt, vv))
    }
  }

  assert.equal(expectedIdxSize, crdt.index.length)
}

export function get<T>(crdt: StateSet<T>, key: LV): T[] {
  const pairs = (crdt.values.get(key) ?? []) as Pair<T>[]

  return pairs.map(([_, val]) => val)
}

// ;(() => {
//   const crdt = create()
//   check(crdt)

//   const agent = createAgent('seph')
//   const key1 = localInsert(crdt, agent(), "yooo")
//   console.log('key', key1)

//   const crdt2 = create()
//   mergeDelta(crdt2, deltaSince(crdt))
//   // console.log('----')
//   // console.dir(crdt2, {depth:null})
//   // console.log('----')

//   const key2 = localInsert(crdt, agent(), "hiii")
//   console.log('key', key2)

//   const t1 = localSet(crdt, agent(), key1, "blah")
//   console.log(t1)

//   console.log(crdt)

//   console.dir(deltaSince(crdt, [1]), {depth:null})

//   mergeDelta(crdt2, deltaSince(crdt, crdt2.cg.version))
//   console.log('----')
//   console.dir(crdt, {depth:null})
//   console.dir(crdt2, {depth:null})
//   console.log('----')
//   assert.deepEqual(crdt, crdt2)
// })()