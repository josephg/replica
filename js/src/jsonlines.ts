// Simple network protocol using serialized lines of JSON.

import { Socket } from "net";

export type MsgHandler<Msg = any> = (msg: Msg, sock: Socket) => void

export default function handle<InMsg = any, OutMsg = any>(sock: Socket, onMsg: MsgHandler<InMsg>) {
  let closed = false

  ;(async () => {
    let buffer = ''
    for await (const _data of sock) {
      const data = _data as Buffer
      let s = data.toString('utf-8')
      // console.log('data', s)

      while (s.includes('\n')) {
        const idx = s.indexOf('\n')
        const before = s.slice(0, idx)
        s = s.slice(idx + 1)
        let msg = buffer + before

        try {
          let m = JSON.parse(msg) as InMsg
          onMsg(m, sock)
          if (closed) return
        } catch (e) {
          console.error('Error processing message from', sock.remoteAddress, sock.remotePort)
          console.error(e)
          sock.end()
          sock.destroy()
          return
        }
        buffer = ''
      }

      if (s !== '') buffer += s
    }
  })()

  return {
    write(msg: OutMsg) {
      if (sock.writable) {
        sock.write(JSON.stringify(msg) + '\n')
      }
    },
    close() {
      closed = true
      sock.end()
      sock.destroy()
    }
  }
}