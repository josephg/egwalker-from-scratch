// Output an oplog as a DOT graph

import { Graphviz } from '@hpcc-js/wasm-graphviz'
import type { LV, OpLog } from "./egwalker.js"
import fs from 'node:fs'

const COLOR = {
  'Red': "red",
  'Green': "#98ea79",
  'Blue': "#036ffc",
  'Grey': "#eeeeee",
  'Black': "black",
}

const nodeKey = (lv: LV) => `N_${lv}`

export function toDotSrc(oplog: OpLog<any>): string {
  const lines = []
  lines.push("strict digraph {")
  lines.push("\trankdir=\"BT\"")
  lines.push("\tlabelloc=\"t\"")
  lines.push("\tnode [shape=box style=filled]")
  lines.push("\tedge [color=\"#333333\" dir=none]")

  lines.push(`\tROOT [fillcolor=${COLOR.Red} label=<ROOT>]`)

  const inGraph = new Set<string>()

  const getParentNode = (parents: LV[]): string => {
    if (parents.length === 0) return 'ROOT'
    else if (parents.length === 1) return nodeKey(parents[0])
    else {
      const key = 'MERGE_' + parents.map(p => `${p}`).join('_')
      if (!inGraph.has(key)) {
        // Create new merge node in the dot file.
        lines.push(`\t${key} [fillcolor="${COLOR.Blue}" label="" shape=point]`)
        for (const p of parents) {
          lines.push(`\t${key} -> ${nodeKey(p)} [label=${p} color="${COLOR.Blue}"]`)
        }
        inGraph.add(key)
      }
      return key
    }
  }

  // Go through all the ops, adding them to the graph (if they don't exist already)
  for (let lv = 0; lv < oplog.ops.length; lv++) {
    const op = oplog.ops[lv]
    const label = op.type === 'ins'
      ? `${lv} (INS '${op.content}' at ${op.pos})`
      : `${lv} (DEL ${op.pos})`
    lines.push(`\t${nodeKey(lv)} [label=<${label}>]`)
    lines.push(`\t${nodeKey(lv)} -> ${getParentNode(op.parents)}`)
  }

  // If the frontier is a merger, add that too.
  getParentNode(oplog.frontier)

  lines.push('}')
  return lines.join('\n')
}

export async function genSvg(oplog: OpLog<any>, filename: string) {
  const graphviz = await Graphviz.load()
  const dotSource = toDotSrc(oplog)
  const svg = graphviz.dot(dotSource)
  fs.writeFileSync(filename, svg)
}


// const graphviz = await Graphviz.load()

// /** Returns the SVG content */
// export function toDotGraph(oplog: OpLog<any>): string {
//   const dotSource = toDotSrc(oplog)
//   return graphviz.dot(dotSource)
// }

// // export function renderRaw(string: string, filename: string) {
// //   const outStream = fs.createWriteStream(filename);
// //   const result = spawnSync("dot", ["-Tsvg"], {
// //     input: string,
// //     encoding: 'utf8',
// //     stdio: ['pipe', 'pipe', 'pipe']
// //   })

// //   fs.writeFileSync(filename, result.stdout)

// //   console.log('Rendered DOT SVG to', filename)
// // }

// export function render(oplog: OpLog<any>, filename: string) {
//   // renderRaw(toDotGraph(oplog), filename)

//   fs.writeFileSync(filename, toDotGraph(oplog))
// }
