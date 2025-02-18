import SeedRandom from "seed-random";
import assert from 'node:assert/strict'
import { CRDTDocument } from "./egwalker";

function fuzzer(seed: number) {
  const random = SeedRandom(`${seed}`)

  const randInt = (n: number) => Math.floor(random() * n)
  const randBool = (weight: number = 0.5) => random() < weight

  const alphabet = [...' abcdefghijklmnopqrstuvwxyz']
  const randChar = () => alphabet[randInt(alphabet.length)]

  const docs = [
    new CRDTDocument('a'),
    new CRDTDocument('b'),
    new CRDTDocument('c'),
  ]

  const randDoc = () => docs[randInt(3)]

  for (let i = 0; i < 100; i++) {
    for (let d = 0; d < 3; d++) {
      // 1. Pick a random document
      // 2. Make a random change to that document
      const doc = randDoc()
      const len = doc.snapshot.length

      const insertWeight = len < 100 ? 0.65 : 0.35

      if (len === 0 || randBool(insertWeight)) {
        // Insert
        const content = randChar()
        const pos = randInt(len + 1)
        doc.ins(pos, content)
      } else {
        // delete
        const pos = randInt(len)
        const delLen = randInt(Math.min(len - pos, 3))
        doc.del(pos, delLen)
      }

      doc.check()
    }

    // pick 2 documents and merge them
    const a = randDoc()
    const b = randDoc()

    if (a === b) continue

    a.mergeFrom(b)
    b.mergeFrom(a)
    assert.deepEqual(a.snapshot, b.snapshot)
  }

  // console.log(docs[0].getString())
}

// fuzzer(2)


for (let i = 0; i < 1000; i++) {
  console.log('seed', i)
  fuzzer(i)
}
