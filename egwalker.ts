// import { genSvg } from "./dot"
import assert from 'node:assert/strict'
import PriorityQueue from "priorityqueuejs"

export type Id = [agent: string, seq: number] // GUIDs that compress

export type LV = number

type OpInner<T> = {
  type: 'ins',
  content: T,
  pos: number,
} | {
  type: 'del',
  pos: number,
}

export type Op<T> = OpInner<T> & {
  id: Id,
  parents: LV[],
}

type RemoteVersion = Record<string, number> // Last known seq number for every agent

export type OpLog<T> = {
  ops: Op<T>[]
  frontier: LV[],

  version: RemoteVersion,
}

function createOpLog<T>(): OpLog<T> {
  return {
    ops: [],
    frontier: [],
    version: {}
  }
}

function pushLocalOp<T>(oplog: OpLog<T>, agent: string, op: OpInner<T>) {
  const seq = (oplog.version[agent] ?? -1) + 1

  const lv = oplog.ops.length
  oplog.ops.push({
    ...op,
    id: [agent, seq],
    parents: oplog.frontier,
  })

  oplog.frontier = [lv]
  oplog.version[agent] = seq
}

function localInsert<T>(oplog: OpLog<T>, agent: string, pos: number, content: T[]) {
  for (const c of content) {
    pushLocalOp(oplog, agent, {
      type: 'ins',
      content: c,
      pos
    })
    pos++
  }
}

function localDelete<T>(oplog: OpLog<T>, agent: string, pos: number, delLen: number) {
  while (delLen > 0) {
    pushLocalOp(oplog, agent, {
      type: 'del',
      pos
    })
    delLen--
  }
}


const idEq = (a: Id, b: Id): boolean => (
  a == b || (a[0] === b[0] && a[1] === b[1])
)

function idToLV(oplog: OpLog<any>, id: Id): LV {
  const idx = oplog.ops.findIndex(op => idEq(op.id, id))
  if (idx < 0) throw Error('Could not find id in oplog')
  return idx
}

const sortLVs = (frontier: LV[]): LV[] => frontier.sort((a, b) => a - b)

function advanceFrontier(frontier: LV[], lv: LV, parents: LV[]): LV[] {
  const f = frontier.filter(v => !parents.includes(v))
  f.push(lv)
  return sortLVs(f)
}

function pushRemoteOp<T>(oplog: OpLog<T>, op: Op<T>, parentIds: Id[]) {
  const [agent, seq] = op.id
  const lastKnownSeq = oplog.version[agent] ?? -1
  if (lastKnownSeq >= seq) return // We already have the op.

  const lv = oplog.ops.length
  const parents = sortLVs(parentIds.map(id => idToLV(oplog, id)))

  oplog.ops.push({
    ...op,
    parents
  })

  oplog.frontier = advanceFrontier(oplog.frontier, lv, parents)
  if (seq !== lastKnownSeq + 1) throw Error('Seq numbers out of order')
  oplog.version[agent] = seq
}

function mergeInto<T>(dest: OpLog<T>, src: OpLog<T>) {
  for (const op of src.ops) {
    const parentIds = op.parents.map(lv => src.ops[lv].id)
    pushRemoteOp(dest, op, parentIds)
  }
}


/// Generate a document from oplog
type DiffResult = { aOnly: LV[], bOnly: LV[] }

function diff(oplog: OpLog<any>, a: LV[], b: LV[]): DiffResult {
  const enum DiffFlag { A, B, Shared }
  const flags = new Map<LV, DiffFlag>

  let numShared = 0

  const queue = new PriorityQueue<LV>()

  function enq(v: LV, flag: DiffFlag) {
    // Queue v, with the specified flag.
    const oldFlag = flags.get(v)
    if (oldFlag == null) {
      queue.enq(v)
      flags.set(v, flag)
      if (flag === DiffFlag.Shared) numShared++
    } else if (flag !== oldFlag && oldFlag !== DiffFlag.Shared) {
      flags.set(v, DiffFlag.Shared)
      numShared++
    }
  }

  for (const aa of a) enq(aa, DiffFlag.A)
  for (const bb of b) enq(bb, DiffFlag.B)

  const aOnly: LV[] = [], bOnly: LV[] = []

  while (queue.size() > numShared) {
    const lv = queue.deq()
    const flag = flags.get(lv)!

    if (flag === DiffFlag.Shared) numShared--
    else if (flag === DiffFlag.A) aOnly.push(lv)
    else if (flag === DiffFlag.B) bOnly.push(lv)

    const op = oplog.ops[lv]
    for (const p of op.parents) enq(p, flag)
  }

  // console.log('diff', a, b, aOnly, bOnly)

  // const oldResult = diffSlow(oplog, a, b)
  // assert.deepEqual(new Set(oldResult.aOnly), new Set(aOnly))
  // assert.deepEqual(new Set(oldResult.bOnly), new Set(bOnly))
  return { aOnly, bOnly }
}


const NOT_YET_INSERTED = -1
const INSERTED = 0
// DELETED(1) = 1, DELETED(2) = 2, ....

type CRDTItem = {
  lv: LV,
  originLeft: LV | -1,
  originRight: LV | -1,

  deleted: boolean,

  curState: number, // State variable
}

type CRDTDoc = {
  items: CRDTItem[],
  currentVersion: LV[],

  delTargets: LV[] // LV of a delete op
  itemsByLV: CRDTItem[] // Map from LV => CRDTItem.
  // itemsByLV: Map<LV, CRDTItem> // Map from LV => CRDTItem.
}

function retreat(doc: CRDTDoc, oplog: OpLog<any>, opLv: LV) {
  const op = oplog.ops[opLv]

  const targetLV = op.type === 'ins'
    ? opLv
    : doc.delTargets[opLv]

  const item = doc.itemsByLV[targetLV]
  item.curState--
}

function advance(doc: CRDTDoc, oplog: OpLog<any>, opLv: LV) {
  const op = oplog.ops[opLv]

  const targetLV = op.type === 'ins'
    ? opLv
    : doc.delTargets[opLv]

  const item = doc.itemsByLV[targetLV]
  item.curState++
}

function findItemIdxAtLV(items: CRDTItem[], lv: LV) {
  const idx = items.findIndex(item => item.lv === lv)
  if (idx < 0) throw Error('Could not find item')
  return idx
}

function integrate<T>(doc: CRDTDoc, oplog: OpLog<T>, newItem: CRDTItem, idx: number, endPos: number, snapshot: T[] | null) {
  let scanIdx = idx
  let scanEndPos = endPos

  // If originLeft is -1, that means it was inserted at the start of the document.
  // We'll pretend there was some item at position -1 which we were inserted to the
  // right of.
  let left = scanIdx - 1
  let right = newItem.originRight == -1
    ? doc.items.length
    : findItemIdxAtLV(doc.items, newItem.originRight)!

  let scanning = false

  // This loop scans forward from destIdx until it finds the right place to insert into
  // the list.
  while (scanIdx < right) {
    let other = doc.items[scanIdx]

    if (other.curState !== NOT_YET_INSERTED) break

    let oleft = other.originLeft === -1
      ? -1
      : findItemIdxAtLV(doc.items, other.originLeft)

    let oright = other.originRight === -1
      ? doc.items.length
      : findItemIdxAtLV(doc.items, other.originRight)

    // The logic below summarizes to:
    const newItemAgent = oplog.ops[newItem.lv].id[0]
    const otherAgent = oplog.ops[other.lv].id[0]

    if (oleft < left
      || (oleft === left && oright === right && newItemAgent < otherAgent)) {
      break
    }
    if (oleft === left) scanning = oright < right

    if (!other.deleted) scanEndPos++
    scanIdx++

    if (!scanning) {
      idx = scanIdx
      endPos = scanEndPos
    }
  }

  // We've found the position. Insert here.
  doc.items.splice(idx, 0, newItem)

  const op = oplog.ops[newItem.lv]
  if (op.type !== 'ins') throw Error('Cannot insert a delete')
  if (snapshot != null) snapshot.splice(endPos, 0, op.content)
}



function findByCurrentPos(items: CRDTItem[], targetPos: number): {idx: number, endPos: number} {
  let curPos = 0
  let endPos = 0
  let idx = 0

  for (; curPos < targetPos; idx++) {
    if (idx >= items.length) throw Error('Past end of items list')

    const item = items[idx]
    if (item.curState === INSERTED) curPos++
    if (!item.deleted) endPos++
  }

  return {idx, endPos}
}

function apply<T>(doc: CRDTDoc, oplog: OpLog<T>, snapshot: T[] | null, opLv: LV) {
  const op = oplog.ops[opLv]

  if (op.type === 'del') {
    // Delete!

    // find the item that will be deleted.
    let { idx, endPos } = findByCurrentPos(doc.items, op.pos)

    // Scan forward to find the actual item!
    while (doc.items[idx].curState !== INSERTED) {
      if (!doc.items[idx].deleted) endPos++
      idx++
    }

    // This is it
    const item = doc.items[idx]

    if (!item.deleted) {
      item.deleted = true
      if (snapshot != null) snapshot.splice(endPos, 1)
    }

    item.curState = 1

    doc.delTargets[opLv] = item.lv

  } else {
    // Insert
    const { idx, endPos } = findByCurrentPos(doc.items, op.pos)

    if (idx >= 1 && doc.items[idx - 1].curState !== INSERTED) {
      throw Error('Item to the left is not inserted! What!')
    }

    const originLeft = idx === 0 ? -1 : doc.items[idx - 1].lv

    // let originRight = doc.items[idx].lv
    let originRight = -1
    for (let i = idx; i < doc.items.length; i++) {
      const item2 = doc.items[i]
      if (item2.curState !== NOT_YET_INSERTED) {
        // Use this item as our "right" item.
        originRight = item2.lv
        break
      }
    }

    const item: CRDTItem = {
      lv: opLv,
      originLeft,
      originRight,
      deleted: false,
      curState: INSERTED,
    }
    doc.itemsByLV[opLv] = item

    // insert it into the document list
    integrate(doc, oplog, item, idx, endPos, snapshot)
  }
}

function checkout<T>(oplog: OpLog<T>): T[] {
  const doc: CRDTDoc = {
    items: [],
    currentVersion: [],
    delTargets: [],
    itemsByLV: []
  }

  const snapshot: T[] = []

  for (let lv = 0; lv < oplog.ops.length; lv++) {
    do1Operation(doc, oplog, lv, snapshot)
  }

  return snapshot
}

function do1Operation<T>(doc: CRDTDoc, oplog: OpLog<T>, lv: LV, snapshot: T[] | null) {
  const op = oplog.ops[lv]

  const { aOnly, bOnly } = diff(oplog, doc.currentVersion, op.parents)

  // retreat
  for (const i of aOnly) {
    retreat(doc, oplog, i)
  }
  // advance
  for (const i of bOnly) {
    advance(doc, oplog, i)
  }

  apply(doc, oplog, snapshot, lv)
  doc.currentVersion = [lv]
}


type OpsToVisit = {
  commonVersion: LV[],
  sharedOps: LV[],
  bOnlyOps: LV[],
}

function compareArrays(a: LV[], b: LV[]): number {
  for (let i = 0; i < a.length; i++) {
    if (b.length <= i) return 1

    const delta = a[i] - b[i]
    if (delta !== 0) return delta
  }

  // We've covered the case where a is longer than b above.
  // But we might not have iterated through all of b.
  if (a.length < b.length) return -1
  else return 0
}

function findOpsToVisit(oplog: OpLog<any>, a: LV[], b: LV[]): OpsToVisit {
  // if (a.length === 0 && b.length === 0) return { start: [], common: [], bOnly: [] }

  type MergePoint = {
    v: LV[], // Sorted in inverse order (highest to lowest)
    isInA: boolean,
  }

  const queue = new PriorityQueue<MergePoint>((a, b) => {
    // What about when they have different isInA flags? It shouldn't matter.
    return compareArrays(a.v, b.v)
  })

  const enq = (lv: LV[], isInA: boolean) => {
    const mergePoint = {
      v: lv.slice().sort((a, b) => b - a), // Sort in descending order.
      isInA
    }
    queue.enq(mergePoint)
  }

  enq(a, true)
  enq(b, false)

  let commonVersion: LV[]
  const sharedOps = [], bOnlyOps = []

  // console.log('a', a, 'b', b)
  while (true) {
    let { v, isInA } = queue.deq()
    // console.log('deq', v, isInA)
    if (v.length === 0) {
      // We've hit the root element.
      commonVersion = []
      break
    }

    while (!queue.isEmpty()) {
      // We might have multiple elements that have the same merge point.
      // Gross.
      //
      // Consume everything that matches.
      const { v: peekV, isInA: peekIsInA } = queue.peek()
      if (compareArrays(v, peekV) !== 0) break // Stop if the merge point is different.

      queue.deq()
      if (peekIsInA) isInA = true
    }

    // Finally we hit a common version!
    if (queue.isEmpty()) {
      commonVersion = v.reverse()
      break
    }

    if (v.length >= 2) {
      // Its a merger! Just add all the individual items.
      for (const vv of v) enq([vv], isInA)
    } else {
      const lv = v[0]
      // Length must be 1, since we checked for a length of 0 above.
      if (isInA) sharedOps.push(lv)
      else bOnlyOps.push(lv)

      // Add parents.
      const op = oplog.ops[lv]
      enq(op.parents, isInA)
    }
  }

  return {
    commonVersion,
    sharedOps: sharedOps.reverse(),
    bOnlyOps: bOnlyOps.reverse()
  }
}

export type Branch<T> = {
  snapshot: T[],
  frontier: LV[],
}

function createBranch<T>(): Branch<T> {
  return {
    snapshot: [],
    frontier: []
  }
}

function checkoutFancy<T>(oplog: OpLog<T>, branch: Branch<T>, mergeFrontier: LV[] = oplog.frontier) {
  const {
    commonVersion,
    sharedOps,
    bOnlyOps
  } = findOpsToVisit(oplog, branch.frontier, mergeFrontier)

  const doc: CRDTDoc = {
    items: [],
    currentVersion: commonVersion,
    delTargets: [],
    itemsByLV: []
  }

  const placeholderLength = Math.max(...branch.frontier) + 1
  for (let i = 0; i < placeholderLength; i++) {
    const item: CRDTItem = {
      lv: i + 1e12,
      curState: INSERTED,
      deleted: false,
      originLeft: -1,
      originRight: -1
    }
    doc.items.push(item)
    doc.itemsByLV[item.lv] = item
  }


  // 1. Go through all changes in snapshot version (frontier) (ignore snapshot)
  // 2. Add all other operations (modify the snapshot)
  for (const lv of sharedOps) {
    do1Operation(doc, oplog, lv, null)
  }

  for (const lv of bOnlyOps) {
    do1Operation(doc, oplog, lv, branch.snapshot)
    branch.frontier = advanceFrontier(branch.frontier, lv, oplog.ops[lv].parents)
  }

  // console.log('visited:', sharedOps.length + bOnlyOps.length, 'total:', oplog.ops.length)
}


export class CRDTDocument {
  oplog: OpLog<string>
  agent: string
  branch: Branch<string>

  constructor(agent: string) {
    this.oplog = createOpLog()
    this.agent = agent
    this.branch = createBranch()
  }

  check() {
    const actualDoc = checkout(this.oplog)
    if (actualDoc.join('') !== this.branch.snapshot.join('')) throw Error('Document out of sync')
  }

  ins(pos: number, text: string) {
    const inserted = [...text]
    localInsert(this.oplog, this.agent, pos, inserted)
    this.branch.snapshot.splice(pos, 0, ...inserted)
    this.branch.frontier = this.oplog.frontier.slice()
  }

  del(pos: number, delLen: number) {
    localDelete(this.oplog, this.agent, pos, delLen)
    // this.snapshot = checkout(this.oplog)
    this.branch.snapshot.splice(pos, delLen)
    this.branch.frontier = this.oplog.frontier.slice()
  }

  getString() {
    // return checkout(this.oplog).join('')
    return this.branch.snapshot.join('')
  }

  mergeFrom(other: CRDTDocument) {
    mergeInto(this.oplog, other.oplog)
    // this.snapshot = checkout(this.oplog)
    checkoutFancy(this.oplog, this.branch)
  }

  reset() {
    this.oplog = createOpLog()
    this.branch = createBranch()
  }
}




// const oplog1 = createOpLog<string>()
// localInsert(oplog1, 'seph', 0, [...'hi'])

// const oplog2 = createOpLog<string>()
// localInsert(oplog2, 'alice', 0, [...'yo'])

// mergeInto(oplog1, oplog2)
// mergeInto(oplog2, oplog1)

// localInsert(oplog2, 'alice', 4, [...'x'])

// // console.log(oplog1)
// // console.table(oplog2.ops)
// // console.table(oplog2.ops)

// const result = checkout(oplog2).join('')
// console.log('doc is', result)


// // genSvg(oplog2, 'blah.svg')
