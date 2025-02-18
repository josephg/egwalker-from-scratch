
export type Id = [agent: string, seq: number] // GUIDs that compress

type LV = number

type OpInner<T> = {
  type: 'ins',
  content: T,
  pos: number,
} | {
  type: 'del',
  pos: number,
}

type Op<T> = OpInner<T> & {
  id: Id,
  parents: LV[],
}

type RemoteVersion = Record<string, number> // Last known seq number for every agent

type OpLog<T> = {
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

function expandVersionToSet(oplog: OpLog<any>, frontier: LV[]): Set<LV> {
  const set = new Set<LV>
  const toExpand = frontier.slice()

  while (toExpand.length > 0) {
    const lv = toExpand.pop()!
    if (set.has(lv)) continue

    set.add(lv)
    const op = oplog.ops[lv]
    toExpand.push(...op.parents)
  }
  return set
}

type DiffResult = { aOnly: LV[], bOnly: LV[] }
function diff(oplog: OpLog<any>, a: LV[], b: LV[]): DiffResult {
  // bad (slow) implementation
  const aExpand = expandVersionToSet(oplog, a)
  const bExpand = expandVersionToSet(oplog, b)

  return {
    aOnly: [...aExpand.difference(bExpand)],
    bOnly: [...bExpand.difference(aExpand)],
  }
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

function integrate<T>(doc: CRDTDoc, oplog: OpLog<T>, newItem: CRDTItem, idx: number, endPos: number, snapshot: T[]) {
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
  snapshot.splice(endPos, 0, op.content)
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

function apply<T>(doc: CRDTDoc, oplog: OpLog<T>, snapshot: T[], opLv: LV) {
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
      snapshot.splice(endPos, 1)
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
    const op = oplog.ops[lv]

    const { aOnly, bOnly } = diff(oplog, doc.currentVersion, op.parents)

    // retreat
    for (const i of aOnly) {
      // console.log('retreat', i)
      retreat(doc, oplog, i)

      // console.table(doc.items)
    }
    // advance
    for (const i of bOnly) {
      // console.log('advance', i)
      advance(doc, oplog, i)

      // console.table(doc.items)
    }

    // apply
    // console.log('apply', lv) // Add items to items[]
    apply(doc, oplog, snapshot, lv)

    // console.table(doc.items)
    doc.currentVersion = [lv]
  }

  return snapshot
}


export class CRDTDocument {
  oplog: OpLog<string>
  agent: string
  // snapshot: string
  snapshot: string[]

  constructor(agent: string) {
    this.oplog = createOpLog()
    this.agent = agent
    this.snapshot = []
  }

  check() {
    const actualDoc = checkout(this.oplog)
    if (actualDoc.join('') !== this.snapshot.join('')) throw Error('Document out of sync')
  }

  ins(pos: number, text: string) {
    const inserted = [...text]
    localInsert(this.oplog, this.agent, pos, inserted)
    this.snapshot.splice(pos, 0, ...inserted)
  }

  del(pos: number, delLen: number) {
    localDelete(this.oplog, this.agent, pos, delLen)
    // this.snapshot = checkout(this.oplog)
    this.snapshot.splice(pos, delLen)
  }

  getString() {
    // return checkout(this.oplog).join('')
    return this.snapshot.join('')
  }

  mergeFrom(other: CRDTDocument) {
    mergeInto(this.oplog, other.oplog)
    this.snapshot = checkout(this.oplog)
  }

  reset() {
    this.oplog = createOpLog()
    this.snapshot = []
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
