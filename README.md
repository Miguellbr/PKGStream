# PKGStream

Generic HTTP streaming server for files contained in local archives.

## Goal

PKGStream exposes a selected file inside an archive through HTTP without creating a full extracted copy on disk.

Initial architecture:

```
archive
  ↓
archive index
  ↓
selected entry
  ↓
bounded streaming pipeline
  ↓
HTTP response
```

The project is intentionally generic: it does not provide a content catalog, search engine, or links to third-party copyrighted material.

## Design requirements

- Recursive archive entry listing.
- Stream a selected entry instead of extracting the complete archive.
- Keep memory bounded with backpressure.
- HTTP `HEAD` and `GET`.
- Correct HTTP Range handling where the archive backend can support it.
- Support large files without requiring a second full copy on disk.
- Keep archive decoding separate from the HTTP server.

## Archive backend

RAR support is being developed as a separate backend. The Node ecosystem has RAR implementations, but several common libraries either materialize the whole input or the extracted entry, which does not satisfy PKGStream's bounded-memory goal. The backend therefore needs to be chosen/implemented around incremental reads rather than simply calling an API that returns a complete Uint8Array.

## Development

Node.js 20+ is recommended.

```bash
npm install
npm test
npm start
```

Use only archives and files you are authorized to access and transmit.
